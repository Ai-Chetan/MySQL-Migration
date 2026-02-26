"""
Database connection utilities for worker.
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector.pooling import MySQLConnectionPool
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor

from shared.models import DatabaseConfig
from shared.utils import setup_logger

logger = setup_logger(__name__)


class MySQLConnection:
    """Manages MySQL database connections."""
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize MySQL connection.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish connection to MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                autocommit=False  # Manual transaction control
            )
            logger.debug(
                f"Connected to MySQL: {self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}"
            )
        except MySQLError as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def get_cursor(self, buffer=False):
        """
        Get a cursor from the connection.
        
        Args:
            buffer: Whether to use buffered cursor
        
        Returns:
            MySQL cursor
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        return self.connection.cursor(buffered=buffer)
    
    def commit(self):
        """Commit current transaction."""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """Rollback current transaction."""
        if self.connection:
            self.connection.rollback()
    
    def close(self):
        """Close the connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.debug("MySQL connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type:
            self.rollback()
        self.close()


class MetadataConnection:
    """Manages PostgreSQL metadata database connections."""
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        """
        Initialize metadata connection.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
    
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            logger.debug(
                f"Connected to metadata DB: {self.user}@{self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to metadata DB: {e}")
            raise
    
    def get_cursor(self):
        """
        Get a cursor from the connection.
        
        Returns:
            PostgreSQL cursor
        """
        if not self.connection or self.connection.closed:
            self.connect()
        
        return self.connection.cursor()
    
    def commit(self):
        """Commit current transaction."""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """Rollback current transaction."""
        if self.connection:
            self.connection.rollback()
    
    def close(self):
        """Close the connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.debug("Metadata DB connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type:
            self.rollback()
        self.close()
