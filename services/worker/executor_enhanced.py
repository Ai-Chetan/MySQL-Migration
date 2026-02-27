"""
Production-Safe Chunk Executor
- Idempotency via range deletion
- Row count validation
- Exponential backoff retries
- Heartbeat mechanism
- Transaction safety
- Structured JSON logging
"""
import json
import threading
from uuid import UUID
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import time
import os

from shared.models import DatabaseConfig, ChunkStatus
from shared.utils import setup_logger, calculate_throughput
from services.worker.db import MySQLConnection, MetadataConnection

logger = setup_logger(__name__)


class EnhancedChunkExecutor:
    """Production-safe chunk executor with full validation and retry logic."""
    
    def __init__(
        self,
        chunk_id: UUID,
        chunk_data: Dict[str, Any],
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        metadata_conn: MetadataConnection,
        worker_id: str,
        batch_size: int = 5000
    ):
        """
        Initialize production-safe chunk executor.
        
        Args:
            chunk_id: Chunk UUID
            chunk_data: Chunk metadata from database
            source_config: Source database configuration
            target_config: Target database configuration
            metadata_conn: Metadata database connection
            worker_id: Unique worker identifier
            batch_size: Number of rows per insert batch
        """
        self.chunk_id = chunk_id
        self.chunk_data = chunk_data
        self.source_config = source_config
        self.target_config = target_config
        self.metadata_conn = metadata_conn
        self.worker_id = worker_id
        self.batch_size = batch_size
        
        self.table_name = chunk_data['table_name']
        self.pk_start = chunk_data['pk_start']
        self.pk_end = chunk_data['pk_end']
        self.retry_count = chunk_data.get('retry_count', 0)
        self.max_retries = chunk_data.get('max_retries', 3)
        
        # Heartbeat thread
        self.heartbeat_thread = None
        self.heartbeat_stop = threading.Event()
        
        # Get primary key column from table metadata
        self.pk_column = self._get_pk_column()
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """
        Emit structured JSON log.
        
        Args:
            level: Log level (info, error, warning)
            message: Log message
            **kwargs: Additional structured fields
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level.upper(),
            "worker_id": self.worker_id,
            "chunk_id": str(self.chunk_id),
            "table": self.table_name,
            "pk_range": f"[{self.pk_start}, {self.pk_end}]",
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
            self._log_structured("warning", "Could not get PK column, defaulting to 'id'", error=str(e))
            return 'id'
    
    def _acquire_chunk_lock(self) -> bool:
        """
        Acquire exclusive lock on chunk using FOR UPDATE SKIP LOCKED.
        
        Returns:
            True if lock acquired, False if chunk already locked
        """
        try:
            cursor = self.metadata_conn.get_cursor()
            
            # Try to lock the chunk
            cursor.execute(
                """
                SELECT id, status, retry_count, max_retries
                FROM migration_chunks
                WHERE id = %s
                FOR UPDATE SKIP LOCKED
                """,
                (str(self.chunk_id),)
            )
            
            result = cursor.fetchone()
            
            if not result:
                self._log_structured("warning", "Chunk already locked by another worker")
                return False
            
            # Verify chunk is eligible for processing
            status = result['status']
            retry_count = result['retry_count']
            max_retries = result['max_retries']
            
            if status == 'pending':
                return True
            elif status == 'failed' and retry_count < max_retries:
                return True
            else:
                self._log_structured(
                    "warning", 
                    "Chunk not eligible for processing",
                    status=status,
                    retry_count=retry_count,
                    max_retries=max_retries
                )
                return False
                
        except Exception as e:
            self._log_structured("error", "Failed to acquire chunk lock", error=str(e))
            return False
    
    def _start_heartbeat(self):
        """Start heartbeat thread to update last_heartbeat every 5 seconds."""
        def heartbeat_worker():
            while not self.heartbeat_stop.is_set():
                try:
                    cursor = self.metadata_conn.get_cursor()
                    cursor.execute(
                        """
                        UPDATE migration_chunks
                        SET last_heartbeat = NOW()
                        WHERE id = %s
                        """,
                        (str(self.chunk_id),)
                    )
                    self.metadata_conn.commit()
                except Exception as e:
                    self._log_structured("warning", "Heartbeat update failed", error=str(e))
                
                # Wait 5 seconds or until stop signal
                self.heartbeat_stop.wait(5)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
        self.heartbeat_thread.start()
        self._log_structured("info", "Heartbeat thread started")
    
    def _stop_heartbeat(self):
        """Stop heartbeat thread."""
        if self.heartbeat_thread:
            self.heartbeat_stop.set()
            self.heartbeat_thread.join(timeout=2)
            self._log_structured("info", "Heartbeat thread stopped")
    
    def _delete_target_range(self, target_conn: MySQLConnection):
        """
        Delete existing rows in target range for idempotency.
        
        Args:
            target_conn: Target database connection
        """
        cursor = target_conn.get_cursor()
        
        delete_start = time.time()
        
        cursor.execute(
            f"""
            DELETE FROM `{self.table_name}`
            WHERE `{self.pk_column}` BETWEEN %s AND %s
            """,
            (self.pk_start, self.pk_end)
        )
        
        deleted_rows = cursor.rowcount
        delete_duration = time.time() - delete_start
        
        self._log_structured(
            "info",
            "Target range deleted for idempotency",
            deleted_rows=deleted_rows,
            delete_duration_ms=int(delete_duration * 1000)
        )
    
    def _validate_row_counts(
        self,
        source_conn: MySQLConnection,
        target_conn: MySQLConnection
    ) -> tuple[int, int, bool]:
        """
        Validate row counts between source and target.
        
        Args:
            source_conn: Source database connection
            target_conn: Target database connection
            
        Returns:
            (source_count, target_count, is_valid)
        """
        source_cursor = source_conn.get_cursor()
        target_cursor = target_conn.get_cursor()
        
        # Count source rows
        source_cursor.execute(
            f"""
            SELECT COUNT(*) as count
            FROM `{self.table_name}`
            WHERE `{self.pk_column}` BETWEEN %s AND %s
            """,
            (self.pk_start, self.pk_end)
        )
        source_count = source_cursor.fetchone()['count']
        
        # Count target rows
        target_cursor.execute(
            f"""
            SELECT COUNT(*) as count
            FROM `{self.table_name}`
            WHERE `{self.pk_column}` BETWEEN %s AND %s
            """,
            (self.pk_start, self.pk_end)
        )
        target_count = target_cursor.fetchone()['count']
        
        is_valid = source_count == target_count
        
        self._log_structured(
            "info" if is_valid else "error",
            "Row count validation",
            source_count=source_count,
            target_count=target_count,
            is_valid=is_valid
        )
        
        return source_count, target_count, is_valid
    
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
        
        # Insert new data
        query = f"""
            INSERT INTO `{self.table_name}` ({columns_str})
            VALUES {values_clause}
        """
        
        cursor.execute(query, flat_data)
    
    def _log_execution_attempt(
        self,
        status: str,
        rows_processed: int = 0,
        source_row_count: int = None,
        target_row_count: int = None,
        duration_ms: int = None,
        error_message: str = None
    ):
        """Log execution attempt to audit table."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                INSERT INTO chunk_execution_log
                (chunk_id, worker_id, attempt_number, status, rows_processed,
                 source_row_count, target_row_count, duration_ms, error_message, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    str(self.chunk_id),
                    self.worker_id,
                    self.retry_count + 1,
                    status,
                    rows_processed,
                    source_row_count,
                    target_row_count,
                    duration_ms,
                    error_message
                )
            )
            self.metadata_conn.commit()
        except Exception as e:
            self._log_structured("warning", "Failed to log execution attempt", error=str(e))
    
    def execute(self) -> bool:
        """
        Execute chunk migration with production safety guarantees.
        
        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.utcnow()
        rows_processed = 0
        source_row_count = 0
        target_row_count = 0
        
        try:
            self._log_structured("info", "Starting chunk execution", retry_count=self.retry_count)
            
            # 1. Acquire exclusive lock on chunk
            if not self._acquire_chunk_lock():
                return False
            
            # 2. Mark chunk as running and assign worker
            self._update_status(
                ChunkStatus.RUNNING,
                worker_id=self.worker_id
            )
            
            # 3. Start heartbeat thread
            self._start_heartbeat()
            
            # 4. Open database connections with transaction
            with MySQLConnection(self.source_config) as source_conn, \
                 MySQLConnection(self.target_config) as target_conn:
                
                # BEGIN TRANSACTION
                target_conn.begin()
                
                try:
                    # 5. DELETE existing target range (idempotency)
                    self._delete_target_range(target_conn)
                    
                    # 6. Stream and insert source data
                    source_cursor = source_conn.get_cursor()
                    
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
                    
                    while True:
                        rows = source_cursor.fetchmany(self.batch_size)
                        
                        if not rows:
                            # Insert final batch
                            if batch:
                                self._insert_batch(target_conn, column_names, batch)
                                rows_processed += len(batch)
                            break
                        
                        batch.extend(rows)
                        
                        if len(batch) >= self.batch_size:
                            self._insert_batch(target_conn, column_names, batch)
                            rows_processed += len(batch)
                            batch = []
                            
                            self._log_structured(
                                "info",
                                "Batch inserted",
                                rows_processed=rows_processed
                            )
                    
                    # 7. VALIDATE row counts before commit
                    source_row_count, target_row_count, is_valid = self._validate_row_counts(
                        source_conn,
                        target_conn
                    )
                    
                    if not is_valid:
                        # ROLLBACK on validation failure
                        target_conn.rollback()
                        raise ValueError(
                            f"Row count mismatch: source={source_row_count}, target={target_row_count}"
                        )
                    
                    # 8. COMMIT transaction
                    target_conn.commit()
                    
                    self._log_structured(
                        "info",
                        "Transaction committed successfully",
                        rows_processed=rows_processed
                    )
                    
                except Exception as e:
                    # ROLLBACK on any error
                    target_conn.rollback()
                    raise
            
            # 9. Calculate metrics
            end_time = datetime.utcnow()
            duration = end_time - start_time
            duration_ms = int(duration.total_seconds() * 1000)
            throughput = calculate_throughput(rows_processed, duration.total_seconds())
            
            # 10. Mark as completed with validation status
            self._update_status(
                ChunkStatus.COMPLETED,
                rows_processed=rows_processed,
                source_row_count=source_row_count,
                target_row_count=target_row_count,
                duration_ms=duration_ms,
                validation_status='validated'
            )
            
            # 11. Log execution attempt
            self._log_execution_attempt(
                status='completed',
                rows_processed=rows_processed,
                source_row_count=source_row_count,
                target_row_count=target_row_count,
                duration_ms=duration_ms
            )
            
            self._log_structured(
                "info",
                "Chunk completed successfully",
                rows_processed=rows_processed,
                duration_ms=duration_ms,
                throughput_rows_per_sec=throughput
            )
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            self._log_structured(
                "error",
                "Chunk execution failed",
                error=error_msg,
                rows_processed=rows_processed,
                retry_count=self.retry_count
            )
            
            # Calculate next retry time with exponential backoff
            backoff_seconds = 2 ** self.retry_count  # 1, 2, 4, 8, 16...
            next_retry_at = datetime.utcnow() + timedelta(seconds=backoff_seconds)
            
            # Mark as failed with retry scheduling
            self._update_status(
                ChunkStatus.FAILED,
                error=error_msg,
                rows_processed=rows_processed,
                next_retry_at=next_retry_at if self.retry_count < self.max_retries else None
            )
            
            # Log execution attempt
            end_time = datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            self._log_execution_attempt(
                status='failed',
                rows_processed=rows_processed,
                source_row_count=source_row_count if source_row_count > 0 else None,
                target_row_count=target_row_count if target_row_count > 0 else None,
                duration_ms=duration_ms,
                error_message=error_msg
            )
            
            return False
            
        finally:
            # Always stop heartbeat
            self._stop_heartbeat()
    
    def _update_status(
        self,
        status: ChunkStatus,
        worker_id: str = None,
        rows_processed: Optional[int] = None,
        source_row_count: Optional[int] = None,
        target_row_count: Optional[int] = None,
        duration_ms: Optional[int] = None,
        validation_status: Optional[str] = None,
        error: Optional[str] = None,
        next_retry_at: Optional[datetime] = None
    ):
        """
        Update chunk status in metadata database.
        
        Args:
            status: New status
            worker_id: Worker identifier
            rows_processed: Number of rows processed
            source_row_count: Source row count for validation
            target_row_count: Target row count for validation
            duration_ms: Processing duration in milliseconds
            validation_status: Validation result
            error: Error message if failed
            next_retry_at: Next retry timestamp
        """
        try:
            cursor = self.metadata_conn.get_cursor()
            
            updates = ["status = %s"]
            params = [status.value]
            
            if status == ChunkStatus.RUNNING:
                updates.append("started_at = NOW(), last_heartbeat = NOW()")
                if worker_id:
                    updates.append("worker_id = %s")
                    params.append(worker_id)
            elif status in (ChunkStatus.COMPLETED, ChunkStatus.FAILED):
                updates.append("completed_at = NOW()")
                if status == ChunkStatus.FAILED:
                    updates.append("retry_count = retry_count + 1")
            
            if rows_processed is not None:
                updates.append("rows_processed = %s")
                params.append(rows_processed)
            
            if source_row_count is not None:
                updates.append("source_row_count = %s")
                params.append(source_row_count)
            
            if target_row_count is not None:
                updates.append("target_row_count = %s")
                params.append(target_row_count)
            
            if duration_ms is not None:
                updates.append("duration_ms = %s")
                params.append(duration_ms)
            
            if validation_status:
                updates.append("validation_status = %s")
                params.append(validation_status)
            
            if error:
                updates.append("last_error = %s")
                params.append(error)
            
            if next_retry_at:
                updates.append("next_retry_at = %s")
                params.append(next_retry_at)
            
            params.append(str(self.chunk_id))
            
            query = f"""
                UPDATE migration_chunks
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            cursor.execute(query, params)
            self.metadata_conn.commit()
            
        except Exception as e:
            self._log_structured("error", "Failed to update chunk status", error=str(e))
            self.metadata_conn.rollback()
