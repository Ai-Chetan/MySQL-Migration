"""
logger.py
---------
Structured, application-wide logging configuration.

Design Decisions:
    * A single root logger ("migrator") is configured once at import time.
    * All modules obtain a child logger via ``get_logger(__name__)``.
    * Optional file handler appends structured lines to a persistent log
      file (path set via LOG_FILE env variable).
    * Uses %(levelname)-8s for aligned console output and ISO-8601
      timestamps for easy grep/sort in log files.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from config import CONFIG, get_log_level

_ROOT_LOGGER_NAME = "migrator"
_CONSOLE_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
_FILE_FORMAT = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

_configured = False


def _configure_root_logger() -> None:
    """One-time setup of the root 'migrator' logger and its handlers."""
    global _configured
    if _configured:
        return
    _configured = True

    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(get_log_level())

    # --- Console handler ---
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(get_log_level())
    console_handler.setFormatter(
        logging.Formatter(fmt=_CONSOLE_FORMAT, datefmt=_DATE_FORMAT)
    )
    root.addHandler(console_handler)

    # --- Optional file handler ---
    if CONFIG.migration.log_file:
        log_path = Path(CONFIG.migration.log_file)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)  # Log everything to file
            file_handler.setFormatter(
                logging.Formatter(fmt=_FILE_FORMAT, datefmt=_DATE_FORMAT)
            )
            root.addHandler(file_handler)
        except OSError as exc:
            root.warning("Could not create log file '%s': %s", log_path, exc)


_configure_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Return a child logger scoped to the given name.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` instance under the 'migrator' hierarchy.

    Example::

        log = get_logger(__name__)
        log.info("Operation started")
        log.warning("Potential data loss: %s", details)
        log.error("Fatal error", exc_info=True)
    """
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")
