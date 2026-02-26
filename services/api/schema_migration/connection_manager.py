"""
Database Connection Manager
Handles connections to multiple database types (MySQL, PostgreSQL, SQL Server, etc.)
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
from shared.utils import setup_logger

logger = setup_logger(__name__)


class DatabaseType(str, Enum):
    """Supported database types."""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    MARIADB = "mariadb"


@dataclass
class DatabaseConnection:
    """Database connection configuration."""
    id: str
    name: str
    db_type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (without password)."""
        return {
            'id': self.id,
            'name': self.name,
            'db_type': self.db_type.value,
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'username': self.username,
            'ssl': self.ssl
        }


class ConnectionManager:
    """Manages database connections for schema migration."""
    
    def __init__(self):
        self.connections: Dict[str, DatabaseConnection] = {}
        self._active_connections: Dict[str, Any] = {}
    
    def add_connection(
        self,
        name: str,
        db_type: DatabaseType,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        ssl: bool = False
    ) -> str:
        """
        Add a new database connection.
        
        Returns:
            Connection ID
        """
        conn_id = str(uuid.uuid4())
        
        connection = DatabaseConnection(
            id=conn_id,
            name=name,
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            ssl=ssl
        )
        
        self.connections[conn_id] = connection
        logger.info(f"Added connection: {name} ({db_type.value})")
        
        return conn_id
    
    def test_connection(self, conn_id: str) -> Tuple[bool, str]:
        """
        Test a database connection.
        
        Returns:
            (success, message)
        """
        if conn_id not in self.connections:
            return False, "Connection not found"
        
        conn_config = self.connections[conn_id]
        
        try:
            if conn_config.db_type == DatabaseType.MYSQL or conn_config.db_type == DatabaseType.MARIADB:
                conn = pymysql.connect(
                    host=conn_config.host,
                    port=conn_config.port,
                    user=conn_config.username,
                    password=conn_config.password,
                    database=conn_config.database,
                    connect_timeout=5
                )
                cursor = conn.cursor()
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                conn.close()
                return True, f"Connected successfully. Version: {version}"
            
            elif conn_config.db_type == DatabaseType.POSTGRESQL:
                conn = psycopg2.connect(
                    host=conn_config.host,
                    port=conn_config.port,
                    user=conn_config.username,
                    password=conn_config.password,
                    database=conn_config.database,
                    connect_timeout=5
                )
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                conn.close()
                return True, f"Connected successfully. Version: {version[:50]}"
            
            else:
                return False, f"Database type {conn_config.db_type} not yet implemented"
        
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False, str(e)
    
    def get_connection(self, conn_id: str) -> Any:
        """Get an active database connection."""
        if conn_id not in self.connections:
            raise ValueError(f"Connection {conn_id} not found")
        
        # Reuse existing connection if available
        if conn_id in self._active_connections:
            return self._active_connections[conn_id]
        
        conn_config = self.connections[conn_id]
        
        try:
            if conn_config.db_type == DatabaseType.MYSQL or conn_config.db_type == DatabaseType.MARIADB:
                conn = pymysql.connect(
                    host=conn_config.host,
                    port=conn_config.port,
                    user=conn_config.username,
                    password=conn_config.password,
                    database=conn_config.database,
                    cursorclass=pymysql.cursors.DictCursor
                )
            elif conn_config.db_type == DatabaseType.POSTGRESQL:
                conn = psycopg2.connect(
                    host=conn_config.host,
                    port=conn_config.port,
                    user=conn_config.username,
                    password=conn_config.password,
                    database=conn_config.database,
                    cursor_factory=RealDictCursor
                )
            else:
                raise ValueError(f"Unsupported database type: {conn_config.db_type}")
            
            self._active_connections[conn_id] = conn
            logger.info(f"Established connection: {conn_config.name}")
            return conn
        
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    def close_connection(self, conn_id: str):
        """Close a database connection."""
        if conn_id in self._active_connections:
            try:
                self._active_connections[conn_id].close()
                del self._active_connections[conn_id]
                logger.info(f"Closed connection: {conn_id}")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
    
    def get_tables(self, conn_id: str) -> List[str]:
        """Get list of tables in database."""
        conn_config = self.connections[conn_id]
        conn = self.get_connection(conn_id)
        cursor = conn.cursor()
        
        try:
            if conn_config.db_type == DatabaseType.MYSQL or conn_config.db_type == DatabaseType.MARIADB:
                cursor.execute("SHOW TABLES")
                tables = [list(row.values())[0] for row in cursor.fetchall()]
            elif conn_config.db_type == DatabaseType.POSTGRESQL:
                cursor.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
                tables = [row['tablename'] for row in cursor.fetchall()]
            else:
                tables = []
            
            return sorted(tables)
        finally:
            cursor.close()
    
    def get_table_schema(self, conn_id: str, table_name: str) -> List[Dict[str, Any]]:
        """Get schema definition for a table."""
        conn_config = self.connections[conn_id]
        conn = self.get_connection(conn_id)
        cursor = conn.cursor()
        
        try:
            if conn_config.db_type == DatabaseType.MYSQL or conn_config.db_type == DatabaseType.MARIADB:
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = cursor.fetchall()
                return [
                    {
                        'name': col['Field'],
                        'type': col['Type'],
                        'nullable': col['Null'] == 'YES',
                        'key': col['Key'],
                        'default': col['Default'],
                        'extra': col['Extra']
                    }
                    for col in columns
                ]
            elif conn_config.db_type == DatabaseType.POSTGRESQL:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                return [
                    {
                        'name': col['column_name'],
                        'type': col['data_type'],
                        'nullable': col['is_nullable'] == 'YES',
                        'key': '',
                        'default': col['column_default'],
                        'extra': ''
                    }
                    for col in columns
                ]
            else:
                return []
        finally:
            cursor.close()
    
    def get_table_data(
        self,
        conn_id: str,
        table_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get data from a table with pagination.
        
        Returns:
            (rows, total_count)
        """
        conn = self.get_connection(conn_id)
        conn_config = self.connections[conn_id]
        cursor = conn.cursor()
        
        try:
            # Get total count
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
            total_count = list(cursor.fetchone().values())[0]
            
            # Get data with limit/offset
            if conn_config.db_type == DatabaseType.POSTGRESQL:
                cursor.execute(
                    f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
                )
            else:
                cursor.execute(
                    f"SELECT * FROM `{table_name}` LIMIT {offset}, {limit}"
                )
            
            rows = cursor.fetchall()
            return rows, total_count
        finally:
            cursor.close()
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """List all connections."""
        return [conn.to_dict() for conn in self.connections.values()]
    
    def remove_connection(self, conn_id: str):
        """Remove a connection."""
        self.close_connection(conn_id)
        if conn_id in self.connections:
            del self.connections[conn_id]
            logger.info(f"Removed connection: {conn_id}")
    
    def close_all(self):
        """Close all connections."""
        for conn_id in list(self._active_connections.keys()):
            self.close_connection(conn_id)


# Global instance
_connection_manager: Optional[ConnectionManager] = None


def get_connection_manager() -> ConnectionManager:
    """Get the singleton connection manager instance."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager
