"""
Task I did in this file:
- Load folder paths from config.ini
- Read Kafka settings from environment variables
- Validate input folder exists (terminate with error if missing)
- Process only .csv files in input folder (case-insensitive)
- Stream file contents line-by-line to Kafka topic
- On success -> move file to archive folder
- On failure -> move file to error folder and create detailed error log
- Create archive and error folders automatically if absent
- Logging: console + rotating file handler (configurable via env vars)
"""

import os
import sys
import time
import traceback
import shutil
import logging
from logging.handlers import RotatingFileHandler
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer  # kafka-python


# Configuration & Logging


DEFAULT_LOG_FILE = "/var/log/csv_kafka_streamer/processor.log"  # changed in Docker via env/volume
DEFAULT_LOG_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
DEFAULT_LOG_LEVEL = "INFO"

def setup_logging():
    # Read logging config from env vars
    log_file = os.environ.get("APP_LOG_FILE", DEFAULT_LOG_FILE)
    log_level = os.environ.get("APP_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    try:
        max_bytes = int(os.environ.get("APP_LOG_MAX_BYTES", str(DEFAULT_LOG_MAX_BYTES)))
    except ValueError:
        max_bytes = DEFAULT_LOG_MAX_BYTES
    try:
        backup_count = int(os.environ.get("APP_LOG_BACKUP_COUNT", str(DEFAULT_LOG_BACKUP_COUNT)))
    except ValueError:
        backup_count = DEFAULT_LOG_BACKUP_COUNT

    # Ensure log directory exists (if file path provided)
    log_dir = Path(log_file).parent
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Fall back to current dir
        log_file = "./processor.log"
        log_dir = Path(log_file).parent

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Console handler (stream)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(getattr(logging, log_level, logging.INFO))
    ch_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # Rotating file handler
    try:
        fh = RotatingFileHandler(str(log_file), maxBytes=max_bytes, backupCount=backup_count)
        fh.setLevel(getattr(logging, log_level, logging.INFO))
        fh_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] - %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)
    except Exception as e:
        logger.warning("Failed to setup file logging (%s). Continuing with console only.", e)

    logger.debug("Logging initialized; file=%s maxBytes=%d backups=%d level=%s", log_file, max_bytes, backup_count, log_level)
    return logger

logger = setup_logging()


# Config loading functions


def load_config(config_path: str = "config.ini"):
    logger.info("Loading configuration from %s", config_path)
    cfg = ConfigParser()
    read = cfg.read(config_path)
    if not read:
        raise FileNotFoundError(f"Could not find or read config file at '{config_path}'")
    if "paths" not in cfg:
        raise KeyError("Missing 'paths' section in config.ini")
    paths = cfg["paths"]
    return {
        "input_folder": os.path.expanduser(paths.get("input_folder", "").strip()),
        "archive_folder": os.path.expanduser(paths.get("archive_folder", "").strip()),
        "error_folder": os.path.expanduser(paths.get("error_folder", "").strip()),
    }

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def move_file(src: str, dest_dir: str):
    ensure_dir(dest_dir)
    dest = Path(dest_dir) / Path(src).name
    if dest.exists():
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        dest = dest.with_name(f"{dest.stem}_{ts}{dest.suffix}")
    shutil.move(src, str(dest))
    return str(dest)

def write_error_log(error_folder: str, filename: str, exc: Exception, processed_lines: int = 0):
    ensure_dir(error_folder)
    base_name = Path(filename).name
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log_name = f"error_{base_name}_{ts}.log"
    log_path = Path(error_folder) / log_name
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"Timestamp (UTC): {ts}\n")
        f.write(f"File: {filename}\n")
        f.write(f"Processed lines before error: {processed_lines}\n\n")
        f.write("Traceback:\n")
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=f)
    logger.debug("Wrote detailed error log at %s", log_path)
    return str(log_path)


# Kafka wrapper


class ProducerWrapper:
    def __init__(self, brokers: str, topic: str, client_id: str = "csv-producer"):
        if not brokers:
            raise ValueError("KAFKA_BROKER(s) not provided in environment variables")
        if not topic:
            raise ValueError("KAFKA_TOPIC is not provided in environment variables")
        self.brokers = brokers
        self.topic = topic
        logger.info("Initializing Kafka producer for topic '%s' on broker(s) '%s'", topic, brokers)
        self.producer = KafkaProducer(
            bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
            client_id=client_id,
            linger_ms=5,
            max_in_flight_requests_per_connection=5,
        )

    def send_line(self, line: str):
        try:
            future = self.producer.send(self.topic, value=line.encode("utf-8"))
            result = future.get(timeout=10)
            logger.debug("Sent message to Kafka: topic=%s partition=%s offset=%s", result.topic, result.partition, result.offset)
            return result
        except Exception as e:
            logger.exception("Failed to send message to Kafka for line starting: %.80s", (line[:80] if line else "<empty>"))
            raise

    def flush(self):
        try:
            self.producer.flush(timeout=10)
            logger.debug("Kafka producer flushed")
        except Exception as e:
            logger.exception("Kafka flush failed: %s", e)
            raise

    def close(self):
        try:
            self.flush()
        except Exception:
            pass
        try:
            self.producer.close(timeout=10)
            logger.info("Kafka producer closed")
        except Exception:
            logger.exception("Error closing Kafka producer")


# Core processing


def process_csv_file(file_path: str, producer: ProducerWrapper):
    processed = 0
    logger.info("Processing file: %s", file_path)
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line:
                continue
            producer.send_line(line.rstrip("\n"))
            processed += 1
            if processed % 500 == 0:
                logger.info("Processed %d lines from %s", processed, file_path)
    producer.flush()
    logger.info("Finished processing %s; total lines=%d", file_path, processed)
    return processed

def main():
    # 1) loading config.ini
    try:
        cfg = load_config(os.environ.get("APP_CONFIG_PATH", "config.ini"))
    except Exception as e:
        logger.exception("Failed to load config.ini: %s", e)
        sys.exit(2)

    input_folder = cfg["input_folder"]
    archive_folder = cfg["archive_folder"]
    error_folder = cfg["error_folder"]

    logger.info("Paths: input=%s archive=%s error=%s", input_folder, archive_folder, error_folder)

    # 2) Validate input folder exists
    if not input_folder or not os.path.isdir(input_folder):
        logger.critical("Input folder '%s' is missing or not a directory. Exiting.", input_folder)
        sys.exit(3)

    # 3) Create archive/error folders if missing
    ensure_dir(archive_folder)
    ensure_dir(error_folder)

    # 4) Read Kafka settings from env vars
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER")  # comma-separated
    KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
    KAFKA_CLIENT_ID = os.environ.get("KAFKA_CLIENT_ID", "csv-producer")

    if not KAFKA_BROKER or not KAFKA_TOPIC:
        logger.critical("Required Kafka environment variables missing. Ensure KAFKA_BROKER and KAFKA_TOPIC are set.")
        sys.exit(4)

    # 5) Initialize producer
    try:
        producer = ProducerWrapper(KAFKA_BROKER, KAFKA_TOPIC, client_id=KAFKA_CLIENT_ID)
    except Exception as e:
        logger.exception("Could not initialize Kafka producer: %s", e)
        sys.exit(5)

    # 6) Iterate through input folder and process only .csv files
    try:
        files = sorted(Path(input_folder).iterdir())
    except Exception as e:
        logger.exception("Could not list input folder '%s': %s", input_folder, e)
        producer.close()
        sys.exit(6)

    csv_files = [str(p) for p in files if p.is_file() and p.suffix.lower() == ".csv"]

    if not csv_files:
        logger.info("No .csv files to process in input folder. Exiting normally.")
    else:
        for filepath in csv_files:
            fname = Path(filepath).name
            logger.info("Start processing file: %s", fname)
            processed_count = 0
            try:
                processed_count = process_csv_file(filepath, producer)
                moved_to = move_file(filepath, archive_folder)
                logger.info("Successfully processed '%s' (lines=%d). Moved to archive '%s'", fname, processed_count, moved_to)
            except KeyboardInterrupt:
                logger.warning("Interrupted by user. Exiting.")
                producer.close()
                sys.exit(130)
            except Exception as ex:
                logger.exception("Processing failed for '%s': %s", fname, ex)
                try:
                    err_log = write_error_log(error_folder, filepath, ex, processed_lines=processed_count)
                except Exception as e2:
                    logger.exception("Failed to write detailed error log: %s", e2)
                    err_log = "(failed to write detailed log)"
                try:
                    moved = move_file(filepath, error_folder)
                except Exception as e_move:
                    logger.exception("Failed to move file '%s' to error folder: %s", fname, e_move)
                    moved = "(failed to move)"
                logger.error("Moved '%s' to error folder '%s'. Error log: %s", fname, moved, err_log)

    # cleanup
    try:
        producer.close()
    except Exception:
        pass

if __name__ == "__main__":
    main()
