import boto3
import os
import threading
import queue
import time

print("--- Concurrent Log Processor Started ---")

# --- 1. Configuration ---
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
BUCKET_NAME = "log-bucket"
NUM_WORKER_THREADS = 20 # Number of threads to run in parallel

# --- 2. Shared Resources ---
job_queue = queue.Queue()
global_counts = {"ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0}
# A lock to protect global_counts from race conditions
total_counts_lock = threading.Lock()

# --- 3. The Worker Function (The Thread's "Brain") ---
def worker():
    # Create a new s3 client *for this thread*
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    while True:
        try:
            # Get a job (a file_key) from the queue
            file_key = job_queue.get()
            if file_key is None:
                break  # Sentinel value means "no more jobs"

            # 1. Download the file
            s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            
            # 2. Parse the file (memory-efficiently)
            local_counts = {"ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0}
            for line in s3_object['Body'].iter_lines():
                decoded_line = line.decode('utf-8')
                if "[ERROR]" in decoded_line:
                    local_counts["ERROR"] += 1
                elif "[WARNING]" in decoded_line:
                    local_counts["WARNING"] += 1
                elif "[INFO]" in decoded_line:
                    local_counts["INFO"] += 1
                elif "[DEBUG]" in decoded_line:
                    local_counts["DEBUG"] += 1
            
            # 3. Update the global counts (Thread-Safe)
            with total_counts_lock:
                global_counts["ERROR"] += local_counts["ERROR"]
                global_counts["WARNING"] += local_counts["WARNING"]
                global_counts["INFO"] += local_counts["INFO"]
                global_counts["DEBUG"] += local_counts["DEBUG"]

            job_queue.task_done()
        except Exception as e:
            print(f"Worker error: {e}")
            job_queue.task_done()

# --- 4. The Main (Producer) Thread ---
def main():
    start_time = time.time()
    
    # Connect to S3 to get the file list
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # 1. Start the worker threads
    print(f"Starting {NUM_WORKER_THREADS} worker threads...")
    threads = []
    for _ in range(NUM_WORKER_THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # 2. Fill the queue with jobs (Producer)
    print(f"Listing files in bucket '{BUCKET_NAME}'...")
    file_count = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix='logs/'):
        if 'Contents' in page:
            for obj in page['Contents']:
                job_queue.put(obj['Key'])
                file_count += 1
    
    print(f"Added {file_count} files to the queue.")

    # 3. Wait for all jobs to be completed
    job_queue.join()
    print("All jobs completed.")

    # 4. Stop the worker threads
    for _ in range(NUM_WORKER_THREADS):
        job_queue.put(None)
    for t in threads:
        t.join()
        
    end_time = time.time()

    # 5. Print the final report
    print("\n--- Aggregation Complete ---")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")
    print("Final Counts:")
    for level, count in global_counts.items():
        print(f"  {level}: {count}")

if __name__ == "__main__":
    main()
