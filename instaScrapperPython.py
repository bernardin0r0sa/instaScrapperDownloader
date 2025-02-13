from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Boolean, Table, ForeignKey, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
import instaloader
import boto3
from botocore.client import Config
import time
import random
import json
import os
import requests
from typing import Dict, Optional, Tuple, List
import logging
from PIL import Image
from io import BytesIO
import traceback
from threading import Lock
from datetime import datetime
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
username = os.getenv('DATABASE_USERNAME')
password = os.getenv('DATABASE_PASSWORD')
db_url = os.getenv('DATABASE_URL')

if not all([username, password, db_url]):
    raise ValueError("Database environment variables not properly configured")

DATABASE_URL = f"mysql+mysqlconnector://{username}:{password}@{db_url}"

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    connect_args={
        'connect_timeout': 60
    }
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class ProcessedPost(Base):
    __tablename__ = "processed_posts"
    id = Column(String(255), primary_key=True)
    image_url = Column(String(1024))
    vector_id = Column(String(255))
    influencer_id = Column(Integer)
    batch_id = Column(String(255))
    processed_at = Column(DateTime)
    analysis_json = Column(Text)
    status = Column(String(20), default='PENDING_ANALYSIS')
    error_message = Column(Text)

class DownloadHistory(Base):
    __tablename__ = "download_history"
    id = Column(Integer, primary_key=True)
    influencer_id = Column(Integer)
    batch_id = Column(String(255))
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    status = Column(String(50))
    error_message = Column(Text)

class Influencer(Base):
    __tablename__ = "influencers"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(255))
    category = Column(String(255))
    gender = Column(String(50))
    is_plus_size = Column(Boolean)
    
    batch_types = relationship(
        "BatchType",
        secondary="influencer_batches",
        back_populates="influencers"
    )

class BatchType(Base):
    __tablename__ = "batch_types"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    frequency = Column(String(50))
    
    influencers = relationship(
        "Influencer",
        secondary="influencer_batches",
        back_populates="batch_types"
    )

InfluencerBatches = Table(
    'influencer_batches',
    Base.metadata,
    Column('influencer_id', Integer, ForeignKey('influencers.id'), primary_key=True),
    Column('batch_type_id', Integer, ForeignKey('batch_types.id'), primary_key=True)
)

class BatchProcessingError(Exception):
    """Custom exception for batch processing errors"""
    pass

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
        
        self.last_request_time = time.time()
        self.lock = Lock()

    def rate_limited_sleep(self, min_delay: int, max_delay: int):
        with self.lock:
            current_time = time.time()
            elapsed_time = current_time - self.last_request_time
            
            if elapsed_time < min_delay:
                sleep_time = min_delay - elapsed_time
                logger.info(f"Rate limit: Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
            else:
                delay = random.uniform(min_delay, max_delay)
                logger.info(f"Rate limit: Sleeping for {delay:.2f} seconds")
                time.sleep(delay)
                
            self.last_request_time = time.time()

    def login(self):
        try:
            username = os.getenv('INSTAGRAM_USERNAME')
            password = os.getenv('INSTAGRAM_PASSWORD')
            session_file = os.getenv('INSTAGRAM_SESSION_FILE')

            session_data = {
                "csrftoken": os.getenv('CSRF_TOKEN'),
                "sessionid": os.getenv('SESSION_ID'),
                "ds_user_id": os.getenv('DS_USER_ID'),
                "mid": os.getenv('MID'),
                "ig_did": os.getenv('IG_DID')
            }

            if all(session_data.values()):
                self.loader.context.load_session(username, session_data)
                logger.info("Session loaded from environment variables.")
            elif session_file and os.path.isfile(session_file):
                self.loader.load_session_from_file(username, session_file)
                logger.info("Session loaded from file.")
            else:
                self.loader.login(username, password)
                self.loader.save_session_to_file(session_file)
                logger.info("Logged in and session saved to file.")

            return True
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def compress_image(self, file_path: str) -> bytes:
        max_size = 5 * 1024 * 1024  # 5MB
        quality = 0.9  # Initial quality

        with Image.open(file_path) as img:
            output = BytesIO()
            img.save(output, format='JPEG', quality=int(quality * 100))
            image_size = output.tell()

            while image_size > max_size and quality > 0.1:
                quality -= 0.1
                output = BytesIO()
                img.save(output, format='JPEG', quality=int(quality * 100))
                image_size = output.tell()

            if image_size > max_size:
                raise ValueError("Compressed image still exceeds 5MB")

            return output.getvalue()

    def upload_to_r2(self, file_path: str, key: str) -> str:
        try:
            image_data = self.compress_image(file_path)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=image_data)

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

app = FastAPI()
downloader = InstagramDownloader()
scheduler = AsyncIOScheduler()

class DownloadRequest(BaseModel):
    username: str
    batchId: str
    maxPosts: Optional[int] = 50

class CustomBatchRequest(BaseModel):
    usernames: Optional[List[str]] = None
    maxPosts: Optional[int] = 25

def calculate_delay(batch_type: str) -> Tuple[int, int]:
    delays = {
        'SEED': (75, 90),
        'INITIAL': (60, 90),
        'WEEKLY': (60, 90),
        'MONTHLY': (45, 75),
        'CUSTOM': (60, 90),
    }
    return delays.get(batch_type, (60, 90))

def get_max_posts(batch_type: str) -> int:
    defaults = {
        'SEED': 25,
        'INITIAL': 35,
        'WEEKLY': 25,
        'MONTHLY': 20,
        'CUSTOM': 25
    }
    return defaults.get(batch_type, 25)

async def process_downloads(username: str, batch_id: str, max_posts: int, db: Session) -> dict:

    stats = {'processed': 0, 'failed': 0, 'errors': []}
    
    try:
        profile = instaloader.Profile.from_username(downloader.loader.context, username)
        batch_type = batch_id.split('_')[0].upper()
        min_delay, max_delay = calculate_delay(batch_type)
        
        logger.info(f"Starting downloads for {username} with batch type {batch_type}")
        
        influencer = db.query(Influencer).filter(Influencer.username == username).first()
        if not influencer:
            raise ValueError(f"Influencer {username} not found in database")

        for idx, post in enumerate(profile.get_posts()):
            if idx >= max_posts:
                break

            try:
                downloader.rate_limited_sleep(min_delay, max_delay)
                logger.info(f"Downloading post {post.mediaid} for {username}")

                local_path = f"temp_{post.mediaid}"
                downloader.loader.download_pic(
                    filename=local_path,
                    url=post.url,
                    mtime=post.date_local
                )

                r2_key = f"fashion/{username}/{post.mediaid}.jpg"
                r2_url = downloader.upload_to_r2(local_path + ".jpg", r2_key)

                processed_post = ProcessedPost(
                    id=str(post.mediaid),
                    image_url=r2_url,
                    influencer_id=influencer.id,
                    batch_id=batch_id,
                    processed_at=datetime.now(),
                    status='PENDING_ANALYSIS'
                )
                
                db.add(processed_post)
                db.commit()
                stats['processed'] += 1

                os.remove(local_path + ".jpg")
                logger.info(f"Local file removed: {local_path}.jpg")

            except Exception as e:
                error_msg = f"Error processing post {post.mediaid}: {str(e)}"
                logger.error(error_msg)
                stats['failed'] += 1
                stats['errors'].append(error_msg)
                continue

        return stats

    except Exception as e:
        error_msg = f"Error processing user {username}: {str(e)}"
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        raise BatchProcessingError(error_msg)

async def process_batch(batch_type: str, batch_size: int = None):
    if batch_size is None:
        batch_size = int(os.getenv('APP_DOWNLOAD_BATCH_SIZE', 5))
            
    batch_id = f"{batch_type}_{datetime.now().strftime('%Y%m%d')}"
    total_stats = {
        'started': datetime.now(),
        'completed': None,
        'batch_type': batch_type,
        'total_influencers': 0,
        'successful_influencers': 0,
        'failed_influencers': 0,
        'total_posts_processed': 0,
        'total_posts_failed': 0,
        'skipped_influencers': 0,
        'errors': []
    }

    try:
        db = SessionLocal()

        if batch_type in ['SEED', 'INITIAL']:
            base_query = (
                db.query(Influencer)
                .join(InfluencerBatches, InfluencerBatches.c.influencer_id == Influencer.id)
                .join(BatchType, BatchType.id == InfluencerBatches.c.batch_type_id)
                .filter(BatchType.name == batch_type)
                .filter(
                    ~Influencer.id.in_(
                        db.query(DownloadHistory.influencer_id)
                        .filter(
                            DownloadHistory.batch_id.like('SEED_%') |
                            DownloadHistory.batch_id.like('INITIAL_%')
                        )
                        .filter(DownloadHistory.status.in_(['COMPLETED', 'COMPLETED_WITH_ERRORS']))
                        .distinct()
                        .subquery()
                    )
                )
            )
        else:
            base_query = (
                db.query(Influencer)
                .join(InfluencerBatches, InfluencerBatches.c.influencer_id == Influencer.id)
                .join(BatchType, BatchType.id == InfluencerBatches.c.batch_type_id)
                .filter(BatchType.name == batch_type)
            )

        influencers = base_query.limit(batch_size).all()
        total_stats['total_influencers'] = len(influencers)

        if not influencers:
            logger.info(f"No influencers to process for {batch_type} batch")
            return

        logger.info(f"Processing {len(influencers)} influencers for {batch_type} batch")

        for influencer in influencers:
            history = DownloadHistory(
                influencer_id=influencer.id,
                batch_id=batch_id,
                started_at=datetime.now(),
                status='STARTED'
            )
            db.add(history)
            db.commit()

            try:
                stats = await process_downloads(
                    username=influencer.username,
                    batch_id=batch_id,
                    max_posts=get_max_posts(batch_type),
                    db=db
                )
                
                total_stats['total_posts_processed'] += stats['processed']
                total_stats['total_posts_failed'] += stats['failed']
                
                if stats['failed'] == 0:
                    total_stats['successful_influencers'] += 1
                    history.status = 'COMPLETED'
                else:
                    total_stats['failed_influencers'] += 1
                    history.status = 'COMPLETED_WITH_ERRORS'
                    history.error_message = '\n'.join(stats['errors'])
                
                history.completed_at = datetime.now()
                db.commit()

            except Exception as e:
                total_stats['failed_influencers'] += 1
                history.status = 'FAILED'
                history.error_message = str(e)
                history.completed_at = datetime.now()
                db.commit()
                logger.error(f"Error processing {influencer.username}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        raise BatchProcessingError(f"Batch processing failed: {str(e)}")
    finally:
        total_stats['completed'] = datetime.now()
        duration = total_stats['completed'] - total_stats['started']
        
        logger.info(f"""
            Batch {batch_id} completed:
            Type: {batch_type}
            Duration: {duration}
            Total Influencers: {total_stats['total_influencers']}
            Successful: {total_stats['successful_influencers']}
            Failed: {total_stats['failed_influencers']}
            Posts Processed: {total_stats['total_posts_processed']}
            Posts Failed: {total_stats['total_posts_failed']}
            Total Errors: {len(total_stats['errors'])}
        """)
        db.close()

@app.on_event("startup")
async def start_scheduler():
    if not downloader.login():
        raise Exception("Failed to login to Instagram")

    # Initial batch check
    if os.getenv('ENABLE_INITIAL_BATCH', 'false').lower() == 'true':
        scheduler.add_job(
            process_batch,
            'date',
            args=['INITIAL']
        )

    # Seed batch check
    if os.getenv('ENABLE_SEED_BATCH', 'false').lower() == 'true':
        scheduler.add_job(
            process_batch,
            'date',
            args=['SEED']
        )

    # Weekly batch - Every Monday at 1 AM
    scheduler.add_job(
        process_batch,
        CronTrigger(day_of_week='mon', hour=1),
        args=['WEEKLY']
    )
    
    # Monthly batch - 1st of every month at 2 AM
    scheduler.add_job(
        process_batch,
        CronTrigger(day=1, hour=2),
        args=['MONTHLY']
    )
    
    scheduler.start()

@app.post("/api/batch/custom")
async def start_custom_batch(request: CustomBatchRequest, background_tasks: BackgroundTasks):
    try:
        db = SessionLocal()
        batch_id = f"CUSTOM_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if request.usernames:
            query = (
                db.query(Influencer)
                .filter(Influencer.username.in_(request.usernames))
            )
        else:
            query = (
                db.query(Influencer)
                .join(InfluencerBatches, InfluencerBatches.c.influencer_id == Influencer.id)
                .join(BatchType, BatchType.id == InfluencerBatches.c.batch_type_id)
                .filter(BatchType.name == 'CUSTOM')
            )
        
        influencers = query.all()
        
        if not influencers:
            raise HTTPException(status_code=404, detail="No influencers found")
            
        background_tasks.add_task(
            process_batch,
            'CUSTOM',
            len(influencers)
        )
        
        return {"status": "success", "batch_id": batch_id}
        
    except Exception as e:
        logger.error(f"Custom batch error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/batch/status/{batch_id}")
async def get_batch_status(batch_id: str):
    try:
        db = SessionLocal()
        
        history_records = db.query(DownloadHistory).filter(
            DownloadHistory.batch_id == batch_id
        ).all()
        
        if not history_records:
            raise HTTPException(status_code=404, detail="Batch not found")
            
        stats = {
            'total_influencers': len(history_records),
            'completed': len([r for r in history_records if r.status == 'COMPLETED']),
            'failed': len([r for r in history_records if r.status == 'FAILED']),
            'in_progress': len([r for r in history_records if r.status == 'STARTED']),
            'started_at': min(r.started_at for r in history_records if r.started_at),
            'latest_update': max((r.completed_at for r in history_records if r.completed_at), default=None)
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/health")
async def health_check():
    try:
        db = SessionLocal()
        # Test database connection
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "details": {"database": "connected"}}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "unhealthy", "details": {"error": str(e)}}
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)