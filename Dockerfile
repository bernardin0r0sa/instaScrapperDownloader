# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install system dependencies, including trickle
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    trickle \
    && rm -rf /var/lib/apt/lists/*

# Copy only the necessary files
COPY requirements.txt .
COPY instaScrapperPython.py .
COPY session_file.json .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8000

# Command to run the application with trickle
CMD ["trickle", "-s", "-d", "350", "-u", "0", "uvicorn", "instaScrapperPython:app", "--host", "0.0.0.0", "--port", "8000"]
