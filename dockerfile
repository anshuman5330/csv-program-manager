FROM python:3.11-slim

# metadata
LABEL maintainer="you@example.com" \
      description="CSV to Kafka streamer"

# create app user
RUN adduser --disabled-password --gecos "" appuser

# create app dir
WORKDIR /app

# copy project files
COPY requirements.txt /app/requirements.txt
COPY process_files.py /app/process_files.py
COPY config.ini /app/config.ini

# install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# ensure log directory exists and owned by appuser
RUN mkdir -p /var/log/csv_kafka_streamer && chown -R appuser:appuser /var/log/csv_kafka_streamer

# switch to unprivileged user
USER appuser

# default env vars (can be overridden)
ENV APP_CONFIG_PATH=/app/config.ini \
    APP_LOG_FILE=/var/log/csv_kafka_streamer/processor.log \
    APP_LOG_MAX_BYTES=5242880 \
    APP_LOG_BACKUP_COUNT=5 \
    APP_LOG_LEVEL=INFO \
    KAFKA_BROKER=localhost:9092 \
    KAFKA_TOPIC=csv_topic \
    KAFKA_CLIENT_ID=csv-producer

# entrypoint
ENTRYPOINT ["python", "/app/process_files.py"]
