"""
Metadata database connection and initialization.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from typing import Optional
import os
from shared.utils import setup_logger

logger = setup_logger(__name__)


class MetadataDB:
    """Manages connections to the PostgreSQL metadata database."""
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        min_conn: int = 2,
        max_conn: int = 10
    ):
        """
        Initialize metadata database connection pool.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        # Allow environment variable overrides
        self.host = host or os.getenv("METADATA_DB_HOST", "localhost")
        self.port = port or int(os.getenv("METADATA_DB_PORT", "5432"))
        self.database = database or os.getenv("METADATA_DB_NAME", "migration_metadata")
        self.user = user or os.getenv("METADATA_DB_USER", "postgres")
        self.password = password or os.getenv("METADATA_DB_PASSWORD", "postgres")
        
        self.pool: Optional[SimpleConnectionPool] = None
        self.min_conn = min_conn
        self.max_conn = max_conn
    
    def connect(self):
        """Initialize connection pool."""
        try:
            self.pool = SimpleConnectionPool(
                self.min_conn,
                self.max_conn,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            logger.info(
                f"Connected to metadata DB: {self.user}@{self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to metadata DB: {e}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            Database connection
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        return self.pool.getconn()
    
    def return_connection(self, conn):
        """
        Return connection to the pool.
        
        Args:
            conn: Database connection to return
        """
        if self.pool:
            self.pool.putconn(conn)
    
    def close(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Metadata DB connection pool closed")
    
    def initialize_schema(self, schema_file: str = "schema.sql"):
        """
        Initialize database schema from SQL file.
        
        Args:
            schema_file: Path to SQL schema file
        """
        if not os.path.exists(schema_file):
            logger.warning(f"Schema file not found: {schema_file}")
            return
        
        conn = self.get_connection()
        try:
            # Check if schema is already initialized
            cursor = conn.cursor()
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tenants');")
            result = cursor.fetchone()
            schema_exists = result['exists'] if isinstance(result, dict) else result[0]
            
            if schema_exists:
                logger.info("Database schema already initialized, skipping")
                return
            
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            cursor.execute(schema_sql)
            conn.commit()
            logger.info("Database schema initialized successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise
        finally:
            self.return_connection(conn)
    
    def health_check(self) -> bool:
        """
        Check if database is accessible.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.return_connection(conn)
            return result is not None
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Singleton instance
_metadata_db: Optional[MetadataDB] = None


def get_metadata_db() -> MetadataDB:
    """
    Get the singleton metadata database instance.
    
    Returns:
        MetadataDB instance
    """
    global _metadata_db
    if _metadata_db is None:
        _metadata_db = MetadataDB()
        _metadata_db.connect()
    return _metadata_db
