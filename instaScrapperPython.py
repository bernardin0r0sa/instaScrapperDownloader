from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import instaloader
import boto3
from botocore.client import Config
import time
import random
import json
import os
import requests
from typing import Dict, Optional, Tuple
import logging
from PIL import Image
from io import BytesIO
import traceback

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstagramDownloader:
    def __init__(self):
        self.loader = instaloader.Instaloader(
            download_pictures=True,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=True,
            quiet=True
        )

        # Initialize S3 client for R2
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('R2_ENDPOINT'),
            aws_access_key_id=os.getenv('R2_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('R2_SECRET_KEY'),
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'virtual'}
            )
        )
        self.bucket_name = os.getenv('R2_BUCKET')
        self.java_webhook_url = os.getenv('JAVA_WEBHOOK_URL')

    def login(self):
        try:
            username = os.getenv('INSTAGRAM_USERNAME')
            password = os.getenv('INSTAGRAM_PASSWORD')
            session_file = os.getenv('INSTAGRAM_SESSION_FILE')

            # Session data (if you have it in memory)
            session_data = {
                "csrftoken": os.getenv('CSRF_TOKEN'),
                "sessionid": os.getenv('SESSION_ID'),
                "ds_user_id": os.getenv('DS_USER_ID'),
                "mid": os.getenv('MID'),
                "ig_did": os.getenv('IG_DID')
            }

            # Check if session_data is complete
            if all(session_data.values()):
                self.loader.context.load_session(username, session_data)
                logger.info("Session loaded from environment variables.")
            elif session_file and os.path.isfile(session_file):
                self.loader.load_session_from_file(username, session_file)
                logger.info("Session loaded from file.")
            else:
                # If no session file or data, perform login with username and password
                self.loader.login(username, password)
                # Save session to file for future use
                self.loader.save_session_to_file(session_file)
                logger.info("Logged in and session saved to file.")

            return True
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def compress_image(self, file_path: str) -> bytes:
        """Compress the image to ensure it is under 5MB"""
        max_size = 5 * 1024 * 1024  # 5MB
        quality = 0.9  # Initial quality

        with Image.open(file_path) as img:
            output = BytesIO()
            img.save(output, format='JPEG', quality=int(quality * 100))
            image_size = output.tell()

            # Reduce quality until the image size is below the max size
            while image_size > max_size and quality > 0.1:
                quality -= 0.1
                output = BytesIO()
                img.save(output, format='JPEG', quality=int(quality * 100))
                image_size = output.tell()

            if image_size > max_size:
                raise ValueError("Compressed image still exceeds 5MB")

            return output.getvalue()

    def upload_to_r2(self, file_path: str, key: str) -> str:
        """Upload file to R2 and return a presigned URL"""
        try:
            # Compress the image if necessary
            image_data = self.compress_image(file_path)

            # Upload the compressed image to R2
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=image_data)

            # Generate a presigned URL with a 24-hour expiration
            r2_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': key},
                ExpiresIn=24 * 60 * 60  # 24 hours
            )

            logger.info(f"Image uploaded to R2: {r2_url}")
            return r2_url
        except Exception as e:
            logger.error(f"R2 upload error: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def send_webhook(self, post_data: Dict):
        """Send webhook to Java service"""
        try:
            response = requests.post(
                self.java_webhook_url,
                json=post_data,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info(f"Webhook sent for post ID: {post_data['id']}")
        except Exception as e:
            logger.error(f"Webhook error: {str(e)}")
            logger.error(traceback.format_exc())
            raise


def calculate_delay(batch_type: str) -> Tuple[int, int]:
    """Returns (min_delay, max_delay) in seconds based on batch type"""
    delays = {
        'SEED': (45, 60),      # Faster for initial dataset
        'INITIAL': (60, 90),   # Standard delay
        'WEEKLY': (60, 90),    # Standard delay
        'MONTHLY': (45, 75),   # Slightly faster for regular updates
        'CUSTOM': (60, 90),    # Standard delay
    }
    return delays.get(batch_type, (60, 90))  # Default to standard delay


downloader = InstagramDownloader()


class DownloadRequest(BaseModel):
    username: str
    batchId: str
    maxPosts: Optional[int] = 50


@app.on_event("startup")
async def startup_event():
    if not downloader.login():
        raise Exception("Failed to login to Instagram")


@app.post("/api/download/{username}")
async def download_posts(username: str, request: DownloadRequest, background_tasks: BackgroundTasks):
    """Endpoint to initiate post download for an influencer"""
    try:
        background_tasks.add_task(process_downloads, username, request.batchId, request.maxPosts)
        return {"status": "success", "message": f"Download initiated for {username}"}
    except Exception as e:
        logger.error(f"Error in download_posts: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def process_downloads(username: str, batchId: str, maxPosts: int):
    """Process downloads with optimized rate limiting and webhooks"""
    try:
        profile = instaloader.Profile.from_username(downloader.loader.context, username)
        
        # Get batch type from batchId (assumes format like "SEED_20250210")
        batch_type = batchId.split('_')[0].upper()
        min_delay, max_delay = calculate_delay(batch_type)
        
        logger.info(f"Starting downloads for {username} with batch type {batch_type}")
        logger.info(f"Using delay range: {min_delay}-{max_delay} seconds")

        for idx, post in enumerate(profile.get_posts()):
            if idx >= maxPosts:
                break

            # Rate limiting delay with batch-specific timing
            delay = random.uniform(min_delay, max_delay)
            logger.info(f"Waiting {delay:.2f} seconds before next download")
            time.sleep(delay)

            try:
                # Download image
                local_path = f"temp_{post.mediaid}"
                downloader.loader.download_pic(
                    filename=local_path,
                    url=post.url,
                    mtime=post.date_local
                )
                logger.info(f"Image downloaded: {local_path}.jpg")

                # Upload to R2
                r2_key = f"fashion/{username}/{post.mediaid}.jpg"
                r2_url = downloader.upload_to_r2(local_path + ".jpg", r2_key)

                # Prepare webhook data
                post_data = {
                    'id': str(post.mediaid),
                    'username': username,
                    'batchId': batchId,
                    'imageUrl': r2_url,
                    'timestamp': post.date_utc.isoformat(),
                    'caption': post.caption,
                    'likes': post.likes
                }

                # Send webhook to Java service
                downloader.send_webhook(post_data)

                # Cleanup local file
                os.remove(local_path + ".jpg")
                logger.info(f"Local file removed: {local_path}.jpg")

            except Exception as e:
                logger.error(f"Error processing post {post.mediaid}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

    except Exception as e:
        logger.error(f"Error processing user {username}: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)