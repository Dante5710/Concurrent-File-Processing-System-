FROM python:3.9-slim
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all python scripts
COPY log_generator.py .
COPY s3_log_processor.py .

# The CMD will be provided by docker-compose
