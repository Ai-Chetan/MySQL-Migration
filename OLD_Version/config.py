"""
config.py
---------
Centralised configuration management for the MySQL Migration Tool.

Loads settings from environment variables (with .env file support via
python-dotenv if available). Provides typed, validated settings as a
frozen dataclass so configuration is immutable at runtime.

Design Decision:
    Using a dataclass with class-level defaults means the app works
    "out of the box" without any .env file, while still allowing
    environment-based overrides for production deployments.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

# Optional: load a .env file if python-dotenv is installed
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent / ".env"
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path)
except ImportError:
    pass  # python-dotenv not installed; rely solely on real env vars


@dataclass(frozen=True)
class DatabaseConfig:
    """Database connection settings."""
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "3306")))
    charset: str = field(default_factory=lambda: os.getenv("DB_CHARSET", "utf8mb4"))
    connect_timeout: int = field(
        default_factory=lambda: int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
    )
    # Username / password are NOT stored here; they are collected at runtime
    # via the login dialog to avoid credentials ever persisting in config files.


@dataclass(frozen=True)
class MigrationConfig:
    """Migration engine settings."""
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("MIGRATION_BATCH_SIZE", "5000"))
    )
    mapping_file: Path = field(
        default_factory=lambda: Path(os.getenv("MAPPING_FILE", "table_mappings.json"))
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )
    log_file: str | None = field(
        default_factory=lambda: os.getenv("LOG_FILE")  # None → log to stderr only
    )
    scripts_dir: Path = field(
        default_factory=lambda: Path(os.getenv("SCRIPTS_DIR", "."))
    )


@dataclass(frozen=True)
class UIConfig:
    """UI layout preferences."""
    main_window_width: int = 1400
    main_window_height: int = 780
    min_window_width: int = 1200
    min_window_height: int = 650
    font_family: str = "Segoe UI"
    mono_font: str = "Courier New"
    font_size: int = 10


@dataclass(frozen=True)
class AppConfig:
    """Root application configuration."""
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    migration: MigrationConfig = field(default_factory=MigrationConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    app_name: str = "MySQL Migration Tool"
    app_version: str = "2.0.0"


def load_config() -> AppConfig:
    """
    Build and return the application configuration.

    Returns:
        AppConfig: Fully populated (and frozen) configuration object.

    Example::

        cfg = load_config()
        print(cfg.db.host)        # "localhost"
        print(cfg.migration.batch_size)  # 5000
    """
    return AppConfig()


# Module-level singleton used throughout the application
CONFIG: AppConfig = load_config()


def get_log_level() -> int:
    """Convert string log level from config to logging module constant."""
    level = getattr(logging, CONFIG.migration.log_level, None)
    if not isinstance(level, int):
        return logging.INFO
    return level
