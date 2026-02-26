"""
Chunk executor - core migration logic.
"""
from uuid import UUID
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import time

from shared.models import DatabaseConfig, ChunkStatus
from shared.utils import setup_logger, calculate_throughput
from services.worker.db import MySQLConnection, MetadataConnection

logger = setup_logger(__name__)


class ChunkExecutor:
    """Executes migration for a single chunk."""
    
    def __init__(
        self,
        chunk_id: UUID,
        chunk_data: Dict[str, Any],
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        metadata_conn: MetadataConnection,
        batch_size: int = 1000
    ):
        """
        Initialize chunk executor.
        
        Args:
            chunk_id: Chunk UUID
            chunk_data: Chunk metadata from database
            source_config: Source database configuration
            target_config: Target database configuration
            metadata_conn: Metadata database connection
            batch_size: Number of rows per insert batch
        """
        self.chunk_id = chunk_id
        self.chunk_data = chunk_data
        self.source_config = source_config
        self.target_config = target_config
        self.metadata_conn = metadata_conn
        self.batch_size = batch_size
        
        self.table_name = chunk_data['table_name']
        self.pk_start = chunk_data['pk_start']
        self.pk_end = chunk_data['pk_end']
        
        # Get primary key column from table metadata
        self.pk_column = self._get_pk_column()
    
    def _get_pk_column(self) -> str:
        """Get primary key column name from metadata."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                SELECT primary_key_column 
                FROM migration_tables 
                WHERE id = %s
                """,
                (str(self.chunk_data['table_id']),)
            )
            result = cursor.fetchone()
            return result['primary_key_column'] if result else 'id'
        except Exception as e:
            logger.warning(f"Could not get PK column, defaulting to 'id': {e}")
            return 'id'
    
    def execute(self) -> bool:
        """
        Execute chunk migration.
        
        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.utcnow()
        rows_processed = 0
        
        try:
            logger.info(
                f"Processing chunk {self.chunk_id}: {self.table_name} "
                f"[{self.pk_start}, {self.pk_end}]"
            )
            
            # Mark chunk as running
            self._update_status(ChunkStatus.RUNNING)
            
            # Open database connections
            with MySQLConnection(self.source_config) as source_conn, \
                 MySQLConnection(self.target_config) as target_conn:
                
                # Get source data
                source_cursor = source_conn.get_cursor()
                
                # Stream rows using server-side cursor
                source_cursor.execute(
                    f"""
                    SELECT * FROM `{self.table_name}`
                    WHERE `{self.pk_column}` BETWEEN %s AND %s
                    ORDER BY `{self.pk_column}`
                    """,
                    (self.pk_start, self.pk_end)
                )
                
                # Get column names
                column_names = [desc[0] for desc in source_cursor.description]
                
                # Process in batches
                batch = []
                heartbeat_counter = 0
                
                while True:
                    # Fetch batch
                    rows = source_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        # Insert final batch if exists
                        if batch:
                            self._insert_batch(
                                target_conn,
                                column_names,
                                batch
                            )
                            rows_processed += len(batch)
                        break
                    
                    # Accumulate batch
                    batch.extend(rows)
                    
                    # Insert when batch is full
                    if len(batch) >= self.batch_size:
                        self._insert_batch(
                            target_conn,
                            column_names,
                            batch
                        )
                        rows_processed += len(batch)
                        batch = []
                        
                        # Commit after each batch
                        target_conn.commit()
                        
                        # Update heartbeat periodically
                        heartbeat_counter += 1
                        if heartbeat_counter >= 10:  # Every 10 batches
                            self._update_heartbeat()
                            heartbeat_counter = 0
                        
                        logger.debug(
                            f"Chunk {self.chunk_id}: {rows_processed} rows processed"
                        )
                
                # Final commit
                target_conn.commit()
            
            # Calculate metrics
            end_time = datetime.utcnow()
            duration = end_time - start_time
            duration_ms = int(duration.total_seconds() * 1000)
            throughput = calculate_throughput(
                rows_processed,
                duration.total_seconds()
            )
            
            # Mark as completed
            self._update_status(
                ChunkStatus.COMPLETED,
                rows_processed=rows_processed,
                duration_ms=duration_ms
            )
            
            logger.info(
                f"Chunk {self.chunk_id} completed: {rows_processed} rows, "
                f"{throughput} rows/sec"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Chunk {self.chunk_id} failed: {e}")
            
            # Mark as failed
            self._update_status(
                ChunkStatus.FAILED,
                error=str(e),
                rows_processed=rows_processed
            )
            
            return False
    
    def _insert_batch(
        self,
        target_conn: MySQLConnection,
        column_names: list,
        batch: list
    ):
        """
        Insert a batch of rows into target database.
        
        Args:
            target_conn: Target database connection
            column_names: List of column names
            batch: List of row tuples
        """
        if not batch:
            return
        
        cursor = target_conn.get_cursor()
        
        # Build INSERT statement
        columns_str = ", ".join([f"`{col}`" for col in column_names])
        placeholders = ", ".join(["%s"] * len(column_names))
        values_clause = ", ".join([f"({placeholders})"] * len(batch))
        
        # Flatten batch data
        flat_data = []
        for row in batch:
            flat_data.extend(row)
        
        # For idempotency: delete existing rows in range first (Phase 1 simple approach)
        pk_column_idx = column_names.index(self.pk_column)
        pk_values = [row[pk_column_idx] for row in batch]
        
        if pk_values:
            min_pk = min(pk_values)
            max_pk = max(pk_values)
            
            cursor.execute(
                f"""
                DELETE FROM `{self.table_name}`
                WHERE `{self.pk_column}` BETWEEN %s AND %s
                """,
                (min_pk, max_pk)
            )
        
        # Insert new data
        query = f"""
            INSERT INTO `{self.table_name}` ({columns_str})
            VALUES {values_clause}
        """
        
        cursor.execute(query, flat_data)
    
    def _update_status(
        self,
        status: ChunkStatus,
        rows_processed: Optional[int] = None,
        duration_ms: Optional[int] = None,
        error: Optional[str] = None
    ):
        """
        Update chunk status in metadata database.
        
        Args:
            status: New status
            rows_processed: Number of rows processed
            duration_ms: Processing duration in milliseconds
            error: Error message if failed
        """
        try:
            cursor = self.metadata_conn.get_cursor()
            
            updates = ["status = %s"]
            params = [status.value]
            
            if status == ChunkStatus.RUNNING:
                updates.append("started_at = NOW(), last_heartbeat = NOW()")
            elif status in (ChunkStatus.COMPLETED, ChunkStatus.FAILED):
                updates.append("completed_at = NOW()")
                if status == ChunkStatus.FAILED:
                    updates.append("retry_count = retry_count + 1")
            
            if rows_processed is not None:
                updates.append("rows_processed = %s")
                params.append(rows_processed)
            
            if duration_ms is not None:
                updates.append("duration_ms = %s")
                params.append(duration_ms)
            
            if error:
                updates.append("last_error = %s")
                params.append(error)
            
            params.append(str(self.chunk_id))
            
            query = f"""
                UPDATE migration_chunks
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            cursor.execute(query, params)
            self.metadata_conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to update chunk status: {e}")
            self.metadata_conn.rollback()
    
    def _update_heartbeat(self):
        """Update chunk heartbeat timestamp."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                "UPDATE migration_chunks SET last_heartbeat = NOW() WHERE id = %s",
                (str(self.chunk_id),)
            )
            self.metadata_conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {e}")
