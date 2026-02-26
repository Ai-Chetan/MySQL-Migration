"""
Worker configuration.
"""
import os

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "migration_queue")

# Metadata DB configuration
METADATA_DB_HOST = os.getenv("METADATA_DB_HOST", "localhost")
METADATA_DB_PORT = int(os.getenv("METADATA_DB_PORT", "5432"))
METADATA_DB_NAME = os.getenv("METADATA_DB_NAME", "migration_metadata")
METADATA_DB_USER = os.getenv("METADATA_DB_USER", "postgres")
METADATA_DB_PASSWORD = os.getenv("METADATA_DB_PASSWORD", "postgres")

# Worker configuration
WORKER_ID = os.getenv("WORKER_ID", None)  # Auto-generated if None
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("HEARTBEAT_INTERVAL", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Polling
QUEUE_POLL_TIMEOUT = int(os.getenv("QUEUE_POLL_TIMEOUT", "5"))  # seconds
