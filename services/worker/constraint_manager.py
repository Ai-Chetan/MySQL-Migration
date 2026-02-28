"""
Constraint Management Utilities
For optimizing bulk insert performance by temporarily disabling constraints
"""
import json
from typing import List, Dict, Any
from uuid import UUID

from services.worker.db import MySQLConnection, MetadataConnection
from shared.utils import setup_logger

logger = setup_logger(__name__)


class ConstraintManager:
    """Manage database constraints for bulk insert optimization."""
    
    def __init__(self, metadata_conn: MetadataConnection):
        self.metadata_conn = metadata_conn
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "level": level.upper(),
            "service": "constraint_manager",
            "message": message,
            **kwargs
        }
        
        log_line = json.dumps(log_data)
        
        if level == "error":
            logger.error(log_line)
        elif level == "warning":
            logger.warning(log_line)
        else:
            logger.info(log_line)
    
    def get_table_indexes(
        self,
        conn: MySQLConnection,
        table_name: str,
        exclude_primary: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get all indexes for a table.
        
        Args:
            conn: Database connection
            table_name: Table name
            exclude_primary: Exclude primary key index
            
        Returns:
            List of index definitions
        """
        cursor = conn.get_cursor()
        
        if conn.connection.database_info[0] == 'mysql':
            cursor.execute(f"SHOW INDEX FROM `{table_name}`")
            indexes = cursor.fetchall()
            
            result = []
            seen_indexes = set()
            
            for idx in indexes:
                index_name = idx['Key_name']
                
                if exclude_primary and index_name == 'PRIMARY':
                    continue
                
                if index_name not in seen_indexes:
                    seen_indexes.add(index_name)
                    result.append({
                        'name': index_name,
                        'unique': idx['Non_unique'] == 0,
                        'columns': []
                    })
            
            return result
        
        else:  # PostgreSQL
            query = """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%_pkey'
            """
            cursor.execute(query, (table_name,))
            indexes = cursor.fetchall()
            
            return [
                {
                    'name': idx['indexname'],
                    'definition': idx['indexdef']
                }
                for idx in indexes
            ]
    
    def get_foreign_keys(
        self,
        conn: MySQLConnection,
        table_name: str
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        cursor = conn.get_cursor()
        
        if conn.connection.database_info[0] == 'mysql':
            query = """
                SELECT
                    CONSTRAINT_NAME as name,
                    COLUMN_NAME as column_name,
                    REFERENCED_TABLE_NAME as ref_table,
                    REFERENCED_COLUMN_NAME as ref_column
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """
            cursor.execute(query, (table_name,))
            fks = cursor.fetchall()
            
            return [
                {
                    'name': fk['name'],
                    'column': fk['column_name'],
                    'ref_table': fk['ref_table'],
                    'ref_column': fk['ref_column']
                }
                for fk in fks
            ]
        
        else:  # PostgreSQL
            query = """
                SELECT
                    tc.constraint_name as name,
                    kcu.column_name,
                    ccu.table_name AS ref_table,
                    ccu.column_name AS ref_column
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = %s
            """
            cursor.execute(query, (table_name,))
            fks = cursor.fetchall()
            
            return [
                {
                    'name': fk['name'],
                    'column': fk['column_name'],
                    'ref_table': fk['ref_table'],
                    'ref_column': fk['ref_column']
                }
                for fk in fks
            ]
    
    def drop_indexes(
        self,
        conn: MySQLConnection,
        job_id: UUID,
        table_name: str
    ) -> int:
        """
        Drop secondary indexes from table for bulk insert optimization.
        
        Returns:
            Number of indexes dropped
        """
        try:
            indexes = self.get_table_indexes(conn, table_name)
            cursor = conn.get_cursor()
            dropped_count = 0
            
            for idx in indexes:
                try:
                    # Backup index definition
                    meta_cursor = self.metadata_conn.get_cursor()
                    meta_cursor.execute(
                        """
                        INSERT INTO table_constraints_backup
                        (job_id, table_name, constraint_type, constraint_name, constraint_definition, dropped_at)
                        VALUES (%s, %s, 'index', %s, %s, NOW())
                        """,
                        (str(job_id), table_name, idx['name'], json.dumps(idx))
                    )
                    self.metadata_conn.commit()
                    
                    # Drop index
                    if conn.connection.database_info[0] == 'mysql':
                        cursor.execute(f"DROP INDEX `{idx['name']}` ON `{table_name}`")
                    else:
                        cursor.execute(f'DROP INDEX "{idx["name"]}"')
                    
                    dropped_count += 1
                    self._log_structured("info", "Index dropped", table=table_name, index=idx['name'])
                    
                except Exception as e:
                    self._log_structured("warning", "Failed to drop index", table=table_name, index=idx['name'], error=str(e))
            
            conn.commit()
            return dropped_count
            
        except Exception as e:
            self._log_structured("error", "Failed to drop indexes", table=table_name, error=str(e))
            return 0
    
    def disable_foreign_keys(
        self,
        conn: MySQLConnection,
        job_id: UUID,
        table_name: str
    ) -> int:
        """
        Disable foreign key constraints.
        
        Returns:
            Number of FKs disabled
        """
        try:
            fks = self.get_foreign_keys(conn, table_name)
            cursor = conn.get_cursor()
            disabled_count = 0
            
            for fk in fks:
                try:
                    # Backup FK definition
                    meta_cursor = self.metadata_conn.get_cursor()
                    meta_cursor.execute(
                        """
                        INSERT INTO table_constraints_backup
                        (job_id, table_name, constraint_type, constraint_name, constraint_definition, dropped_at)
                        VALUES (%s, %s, 'foreign_key', %s, %s, NOW())
                        """,
                        (str(job_id), table_name, fk['name'], json.dumps(fk))
                    )
                    self.metadata_conn.commit()
                    
                    # Drop FK
                    if conn.connection.database_info[0] == 'mysql':
                        cursor.execute(f"ALTER TABLE `{table_name}` DROP FOREIGN KEY `{fk['name']}`")
                    else:
                        cursor.execute(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{fk["name"]}"')
                    
                    disabled_count += 1
                    self._log_structured("info", "Foreign key dropped", table=table_name, fk=fk['name'])
                    
                except Exception as e:
                    self._log_structured("warning", "Failed to drop FK", table=table_name, fk=fk['name'], error=str(e))
            
            conn.commit()
            return disabled_count
            
        except Exception as e:
            self._log_structured("error", "Failed to drop foreign keys", table=table_name, error=str(e))
            return 0
    
    def restore_indexes(
        self,
        conn: MySQLConnection,
        job_id: UUID,
        table_name: str
    ) -> int:
        """
        Restore indexes after bulk insert.
        
        Returns:
            Number of indexes restored
        """
        try:
            # Get backed up indexes
            meta_cursor = self.metadata_conn.get_cursor()
            meta_cursor.execute(
                """
                SELECT id, constraint_name, constraint_definition
                FROM table_constraints_backup
                WHERE job_id = %s
                AND table_name = %s
                AND constraint_type = 'index'
                AND restored_at IS NULL
                """,
                (str(job_id), table_name)
            )
            indexes = meta_cursor.fetchall()
            
            cursor = conn.get_cursor()
            restored_count = 0
            
            for idx in indexes:
                try:
                    idx_def = json.loads(idx['constraint_definition'])
                    
                    # Recreate index (simplified)
                    if conn.connection.database_info[0] == 'mysql':
                        unique_str = "UNIQUE" if idx_def.get('unique') else ""
                        cursor.execute(
                            f"CREATE {unique_str} INDEX `{idx_def['name']}` ON `{table_name}` (`id`)"
                        )
                    
                    # Mark as restored
                    meta_cursor.execute(
                        "UPDATE table_constraints_backup SET restored_at = NOW() WHERE id = %s",
                        (str(idx['id']),)
                    )
                    self.metadata_conn.commit()
                    
                    restored_count += 1
                    self._log_structured("info", "Index restored", table=table_name, index=idx_def['name'])
                    
                except Exception as e:
                    self._log_structured("warning", "Failed to restore index", table=table_name, error=str(e))
            
            conn.commit()
            return restored_count
            
        except Exception as e:
            self._log_structured("error", "Failed to restore indexes", table=table_name, error=str(e))
            return 0
    
    def restore_foreign_keys(
        self,
        conn: MySQLConnection,
        job_id: UUID,
        table_name: str
    ) -> int:
        """
        Restore foreign key constraints.
        
        Returns:
            Number of FKs restored
        """
        try:
            # Get backed up FKs
            meta_cursor = self.metadata_conn.get_cursor()
            meta_cursor.execute(
                """
                SELECT id, constraint_name, constraint_definition
                FROM table_constraints_backup
                WHERE job_id = %s
                AND table_name = %s
                AND constraint_type = 'foreign_key'
                AND restored_at IS NULL
                """,
                (str(job_id), table_name)
            )
            fks = meta_cursor.fetchall()
            
            cursor = conn.get_cursor()
            restored_count = 0
            
            for fk in fks:
                try:
                    fk_def = json.loads(fk['constraint_definition'])
                    
                    # Recreate FK
                    if conn.connection.database_info[0] == 'mysql':
                        cursor.execute(
                            f"""
                            ALTER TABLE `{table_name}`
                            ADD CONSTRAINT `{fk_def['name']}`
                            FOREIGN KEY (`{fk_def['column']}`)
                            REFERENCES `{fk_def['ref_table']}` (`{fk_def['ref_column']}`)
                            """
                        )
                    
                    # Mark as restored
                    meta_cursor.execute(
                        "UPDATE table_constraints_backup SET restored_at = NOW() WHERE id = %s",
                        (str(fk['id']),)
                    )
                    self.metadata_conn.commit()
                    
                    restored_count += 1
                    self._log_structured("info", "Foreign key restored", table=table_name, fk=fk_def['name'])
                    
                except Exception as e:
                    self._log_structured("warning", "Failed to restore FK", table=table_name, error=str(e))
            
            conn.commit()
            return restored_count
            
        except Exception as e:
            self._log_structured("error", "Failed to restore foreign keys", table=table_name, error=str(e))
            return 0
