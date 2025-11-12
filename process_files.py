#!/usr/bin/env python3
"""
process_files.py

Responsibilities (first phase):
- Load folder paths from config.ini
- Read Kafka settings from environment variables
- Validate input folder exists (terminate with error if missing)
- Process only .csv files in input folder (case-insensitive)
- Stream file contents line-by-line to Kafka topic
- On success -> move file to archive folder
- On failure -> move file to error folder and create detailed error log
- Create archive and error folders automatically if absent
"""

import os
import sys
import time
import traceback
import shutil
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer  # kafka-python

# --- Helper functions -------------------------------------------------------

def load_config(config_path: str = "config.ini"):
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
    # If a file with same name exists, append timestamp to avoid overwrite
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
        f.write(f"Processed lines before error: {processed_lines}\n")
        f.write("Exception:\n")
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=f)
    return str(log_path)

# --- Kafka wrapper ----------------------------------------------------------

class ProducerWrapper:
    def __init__(self, brokers: str, topic: str, client_id: str = "csv-producer"):
        if not brokers:
            raise ValueError("KAFKA_BROKER(s) not provided in environment variables")
        if not topic:
            raise ValueError("KAFKA_TOPIC is not provided in environment variables")
        self.brokers = brokers
        self.topic = topic
        # create kafka producer
        # value_serializer => send raw bytes (we will encode lines as utf-8)
        self.producer = KafkaProducer(
            bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
            client_id=client_id,
            linger_ms=5,
            max_in_flight_requests_per_connection=5,
        )

    def send_line(self, line: str):
        # KafkaProducer.send is async; we can call get(timeout=...) to ensure delivery
        future = self.producer.send(self.topic, value=line.encode("utf-8"))
        # block until send completes or times out
        result = future.get(timeout=10)
        return result

    def flush(self):
        self.producer.flush(timeout=10)

    def close(self):
        try:
            self.flush()
        except Exception:
            pass
        try:
            self.producer.close(timeout=10)
        except Exception:
            pass

# --- Core processing --------------------------------------------------------

def process_csv_file(file_path: str, producer: ProducerWrapper):
    """
    Reads file line-by-line and streams to kafka topic via producer.
    Returns True on success, raises on failure.
    """
    processed = 0
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # Optional: strip newline but keep content
            if not line:
                continue
            # here we send the raw line. For structured JSON you would transform it.
            producer.send_line(line.rstrip("\n"))
            processed += 1
    # ensure all pending messages are sent
    producer.flush()
    return processed

def main():
    # 1) load config.ini
    try:
        cfg = load_config("config.ini")
    except Exception as e:
        print(f"[FATAL] Failed to load config.ini: {e}", file=sys.stderr)
        sys.exit(2)

    input_folder = cfg["input_folder"]
    archive_folder = cfg["archive_folder"]
    error_folder = cfg["error_folder"]

    # 2) Validate input folder exists (terminate if missing)
    if not input_folder or not os.path.isdir(input_folder):
        print(f"[FATAL] Input folder '{input_folder}' is missing or not a directory.", file=sys.stderr)
        sys.exit(3)

    # 3) Create archive/error folders if missing
    ensure_dir(archive_folder)
    ensure_dir(error_folder)

    # 4) Read Kafka settings from env vars
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER")  # e.g., "localhost:9092"
    KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")    # e.g., "my-topic"
    KAFKA_CLIENT_ID = os.environ.get("KAFKA_CLIENT_ID", "csv-producer")

    if not KAFKA_BROKER or not KAFKA_TOPIC:
        print("[FATAL] Required Kafka environment variables missing. Ensure KAFKA_BROKER and KAFKA_TOPIC are set.", file=sys.stderr)
        sys.exit(4)

    # 5) Initialize producer
    try:
        producer = ProducerWrapper(KAFKA_BROKER, KAFKA_TOPIC, client_id=KAFKA_CLIENT_ID)
    except Exception as e:
        print(f"[FATAL] Could not initialize Kafka producer: {e}", file=sys.stderr)
        sys.exit(5)

    # 6) Iterate through input folder and process only .csv files
    try:
        files = sorted(Path(input_folder).iterdir())
    except Exception as e:
        print(f"[FATAL] Could not list input folder '{input_folder}': {e}", file=sys.stderr)
        producer.close()
        sys.exit(6)

    csv_files = [str(p) for p in files if p.is_file() and p.suffix.lower() == ".csv"]

    if not csv_files:
        print("[INFO] No .csv files to process in input folder.", file=sys.stdout)
    else:
        for filepath in csv_files:
            fname = Path(filepath).name
            print(f"[INFO] Starting processing file: {fname}")
            try:
                processed_count = process_csv_file(filepath, producer)
                # Move to archive after successful processing
                moved_to = move_file(filepath, archive_folder)
                print(f"[INFO] Successfully processed '{fname}', lines sent: {processed_count}. Moved to archive: {moved_to}")
            except KeyboardInterrupt:
                print("[WARN] Interrupted by user. Exiting.")
                producer.close()
                sys.exit(130)
            except Exception as ex:
                # On error: move file to error folder and create a detailed error log
                try:
                    err_log = write_error_log(error_folder, filepath, ex, processed_lines=locals().get("processed_count", 0))
                except Exception as e2:
                    err_log = f"(failed to write detailed log: {e2})"
                try:
                    moved = move_file(filepath, error_folder)
                except Exception as e_move:
                    print(f"[ERROR] Failed to move file '{fname}' to error folder: {e_move}", file=sys.stderr)
                    moved = f"(failed to move: {e_move})"
                print(f"[ERROR] Processing failed for '{fname}': {ex}. Moved to error folder: {moved}. Error log: {err_log}", file=sys.stderr)
    # cleanup
    try:
        producer.close()
    except Exception:
        pass

if __name__ == "__main__":
    main()
