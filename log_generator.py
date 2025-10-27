import boto3
import os
import random
import time
import io

print("--- Log Generator Started ---")

# Get config from environment variables
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
BUCKET_NAME = "log-bucket"
NUM_FILES = 1000
LINES_PER_FILE = 100

LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]

# Wait for MinIO to be ready
time.sleep(10) 

print(f"Connecting to S3 at {S3_ENDPOINT}...")
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# 1. Create the bucket
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created.")
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{BUCKET_NAME}' already exists.")

# 2. Generate and upload fake log files
print(f"Generating {NUM_FILES} log files...")
for i in range(NUM_FILES):
    file_key = f"logs/log_file_{i:04d}.log"
    
    # Create a fake log file in memory
    fake_log_content = ""
    for _ in range(LINES_PER_FILE):
        level = random.choice(LOG_LEVELS)
        fake_log_content += f"[{level}] Some log message here...\n"
    
    # Upload the file
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=fake_log_content.encode('utf-8')
    )
    if (i + 1) % 100 == 0:
        print(f"Uploaded {i+1}/{NUM_FILES} files...")

print("--- Log Generation Complete ---")
