# Concurrent-File-Processing-System-
A high-level data engineering project. It perfectly combines concurrency, cloud services, and system design. You've described a classic "parallel fan-out" or "producer-consumer" pattern, which is a cornerstone of distributed data processing.

Concurrent S3 Log Processor

This project demonstrates a high-performance, multithreaded Python system for processing large batches of files from an S3-compatible object store. It uses a Producer-Consumer pattern with a thread-safe queue to concurrently download, parse, and aggregate data.

Architecture:

MinIO: A local S3 server.

Python (Producer): A main thread lists all files in the bucket and adds their keys to a queue.Queue.

Python (Consumers): A pool of N worker threads (using threading) pulls file keys from the queue, downloads the files line-by-line, and aggregates keyword counts.

threading.Lock: A synchronization primitive is used to ensure that the global count dictionary is updated safely by all threads.

How to Run:

Ensure you have Docker and Docker Compose installed.

Run docker-compose up --build.

The system will: a. Start MinIO. b. Run log_generator.py to create and upload 1,000 dummy logs. c. Run s3_log_processor.py to process all the logs and print a final report.
