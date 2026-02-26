"""
Shared utility functions for the migration platform.
"""
import hashlib
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def compute_checksum(data: List[tuple]) -> str:
    """
    Compute a checksum for a batch of rows.
    
    Args:
        data: List of row tuples
    
    Returns:
        MD5 checksum as hexadecimal string
    """
    hasher = hashlib.md5()
    for row in data:
        row_str = str(row).encode('utf-8')
        hasher.update(row_str)
    return hasher.hexdigest()


def calculate_progress_percentage(completed: int, total: int) -> float:
    """
    Calculate progress percentage.
    
    Args:
        completed: Number of completed items
        total: Total number of items
    
    Returns:
        Progress percentage (0-100)
    """
    if total == 0:
        return 0.0
    return round((completed / total) * 100, 2)


def estimate_completion_time(
    total_items: int,
    completed_items: int,
    elapsed_time: timedelta
) -> Optional[datetime]:
    """
    Estimate completion time based on current progress.
    
    Args:
        total_items: Total number of items to process
        completed_items: Number of items completed so far
        elapsed_time: Time elapsed since start
    
    Returns:
        Estimated completion datetime, or None if cannot estimate
    """
    if completed_items == 0 or total_items == 0:
        return None
    
    remaining_items = total_items - completed_items
    if remaining_items <= 0:
        return datetime.utcnow()
    
    time_per_item = elapsed_time.total_seconds() / completed_items
    remaining_seconds = time_per_item * remaining_items
    
    return datetime.utcnow() + timedelta(seconds=remaining_seconds)


def calculate_throughput(rows_processed: int, duration_seconds: float) -> float:
    """
    Calculate throughput in rows per second.
    
    Args:
        rows_processed: Number of rows processed
        duration_seconds: Duration in seconds
    
    Returns:
        Rows per second
    """
    if duration_seconds <= 0:
        return 0.0
    return round(rows_processed / duration_seconds, 2)


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted string (e.g., "2h 30m 15s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize table name for safe SQL usage.
    
    Args:
        table_name: Raw table name
    
    Returns:
        Sanitized table name
    """
    # Remove backticks and quotes
    cleaned = table_name.strip('`"\' ')
    return cleaned


def build_connection_string(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str
) -> str:
    """
    Build a connection string (for logging purposes, without password).
    
    Args:
        host: Database host
        port: Database port
        user: Database user
        database: Database name
    
    Returns:
        Connection string representation
    """
    return f"{user}@{host}:{port}/{database}"
