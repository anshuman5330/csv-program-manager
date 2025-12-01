FROM python:3.11-slim

RUN adduser --disabled-password --gecos "" appuser

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY process_files.py /app/process_files.py
COPY config.ini /app/config.ini

RUN pip install --no-cache-dir -r /app/requirements.txt

RUN mkdir -p /var/log/csv_kafka_streamer && chown -R appuser:appuser /var/log/csv_kafka_streamer

USER appuser

ENV APP_CONFIG_PATH=/app/config.ini \
    APP_LOG_FILE=/var/log/csv_kafka_streamer/processor.log \
    APP_LOG_MAX_BYTES=5242880 \
    APP_LOG_BACKUP_COUNT=5 \
    APP_LOG_LEVEL=INFO \
    KAFKA_BROKER=localhost:9092 \
    KAFKA_TOPIC=csv_topic \
    KAFKA_CLIENT_ID=csv-producer

ENTRYPOINT ["python", "/app/process_files.py"]
