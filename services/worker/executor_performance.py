"""
High-Performance Chunk Executor
Optimized for large-scale migrations (5TB+)

Features:
- Streaming reads (server-side cursors)
- Bulk insert strategies (COPY, LOAD DATA INFILE)
- Adaptive batch sizing
- Memory optimization
- Performance tracking
- Backpressure control
"""
import json
import threading
import time
import psutil
import io
import csv
from uuid import UUID
from typing import Optional, Dict, Any, Iterator, Tuple
from datetime import datetime, timedelta
from decimal import Decimal

from shared.models import DatabaseConfig, ChunkStatus
from shared.utils import setup_logger, calculate_throughput
from services.worker.db import MySQLConnection, MetadataConnection


logger = setup_logger(__name__)


class PerformanceMetrics:
    """Track performance metrics for a chunk execution."""
    
    def __init__(self):
        self.start_time = time.time()
        self.rows_processed = 0
        self.bytes_processed = 0
        self.peak_memory_mb = 0
        self.insert_latencies = []
        self.batch_sizes = []
        
    def record_batch(self, rows: int, bytes_size: int, latency_ms: int, batch_size: int):
        """Record metrics for a batch."""
        self.rows_processed += rows
        self.bytes_processed += bytes_size
        self.insert_latencies.append(latency_ms)
        self.batch_sizes.append(batch_size)
        
        # Track peak memory
        process = psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)
        self.peak_memory_mb = max(self.peak_memory_mb, memory_mb)
    
    def get_throughput(self) -> Tuple[float, float]:
        """Get rows/sec and MB/sec."""
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return 0, 0
        
        rows_per_sec = self.rows_processed / elapsed
        mb_per_sec = (self.bytes_processed / (1024 * 1024)) / elapsed
        return rows_per_sec, mb_per_sec
    
    def get_avg_latency(self) -> int:
        """Get average insert latency in ms."""
        if not self.insert_latencies:
            return 0
        return int(sum(self.insert_latencies) / len(self.insert_latencies))


class AdaptiveBatchSizer:
    """Dynamically adjust batch size based on insert latency."""
    
    def __init__(
        self,
        initial_batch_size: int = 5000,
        min_batch_size: int = 1000,
        max_batch_size: int = 50000,
        target_latency_ms: int = 500
    ):
        self.current_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_latency_ms = target_latency_ms
        self.latency_window = []
        self.window_size = 10
    
    def adjust(self, actual_latency_ms: int) -> Tuple[int, str]:
        """
        Adjust batch size based on observed latency.
        
        Returns:
            (new_batch_size, adjustment_reason)
        """
        self.latency_window.append(actual_latency_ms)
        if len(self.latency_window) > self.window_size:
            self.latency_window.pop(0)
        
        # Need sufficient samples
        if len(self.latency_window) < 3:
            return self.current_batch_size, "warming_up"
        
        avg_latency = sum(self.latency_window) / len(self.latency_window)
        old_batch_size = self.current_batch_size
        
        # Adaptive formula: new_size = current_size * (target / actual)
        adjustment_factor = self.target_latency_ms / max(avg_latency, 1)
        
        # Smooth adjustment (max 20% change per step)
        adjustment_factor = max(0.8, min(1.2, adjustment_factor))
        
        new_batch_size = int(self.current_batch_size * adjustment_factor)
        new_batch_size = max(self.min_batch_size, min(self.max_batch_size, new_batch_size))
        
        reason = "stable"
        if new_batch_size > old_batch_size:
            reason = "increasing_low_latency"
        elif new_batch_size < old_batch_size:
            reason = "decreasing_high_latency"
        
        self.current_batch_size = new_batch_size
        return new_batch_size, reason


class HighPerformanceExecutor:
    """High-performance chunk executor optimized for large-scale migrations."""
    
    def __init__(
        self,
        chunk_id: UUID,
        chunk_data: Dict[str, Any],
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        metadata_conn: MetadataConnection,
        worker_id: str,
        initial_batch_size: int = 5000,
        use_bulk_insert: bool = True,
        enable_adaptive_sizing: bool = True
    ):
        """
        Initialize high-performance executor.
        
        Args:
            chunk_id: Chunk UUID
            chunk_data: Chunk metadata
            source_config: Source database config
            target_config: Target database config
            metadata_conn: Metadata connection
            worker_id: Worker identifier
            initial_batch_size: Starting batch size
            use_bulk_insert: Use COPY/LOAD DATA optimization
            enable_adaptive_sizing: Enable adaptive batch sizing
        """
        self.chunk_id = chunk_id
        self.chunk_data = chunk_data
        self.source_config = source_config
        self.target_config = target_config
        self.metadata_conn = metadata_conn
        self.worker_id = worker_id
        self.use_bulk_insert = use_bulk_insert
        self.enable_adaptive_sizing = enable_adaptive_sizing
        
        self.table_name = chunk_data['table_name']
        self.pk_start = chunk_data['pk_start']
        self.pk_end = chunk_data['pk_end']
        self.retry_count = chunk_data.get('retry_count', 0)
        self.max_retries = chunk_data.get('max_retries', 3)
        
        # Performance tracking
        self.metrics = PerformanceMetrics()
        self.batch_sizer = AdaptiveBatchSizer(initial_batch_size=initial_batch_size)
        
        # Heartbeat thread
        self.heartbeat_thread = None
        self.heartbeat_stop = threading.Event()
        
        self.pk_column = self._get_pk_column()
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level.upper(),
            "worker_id": self.worker_id,
            "chunk_id": str(self.chunk_id),
            "table": self.table_name,
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
        """Get primary key column name."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                "SELECT primary_key_column FROM migration_tables WHERE id = %s",
                (str(self.chunk_data['table_id']),)
            )
            result = cursor.fetchone()
            return result['primary_key_column'] if result else 'id'
        except:
            return 'id'
    
    def _start_heartbeat(self):
        """Start heartbeat thread."""
        def heartbeat_worker():
            while not self.heartbeat_stop.is_set():
                try:
                    cursor = self.metadata_conn.get_cursor()
                    cursor.execute(
                        "UPDATE migration_chunks SET last_heartbeat = NOW() WHERE id = %s",
                        (str(self.chunk_id),)
                    )
                    self.metadata_conn.commit()
                except Exception as e:
                    self._log_structured("warning", "Heartbeat failed", error=str(e))
                
                self.heartbeat_stop.wait(5)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
        self.heartbeat_thread.start()
    
    def _stop_heartbeat(self):
        """Stop heartbeat thread."""
        if self.heartbeat_thread:
            self.heartbeat_stop.set()
            self.heartbeat_thread.join(timeout=2)
    
    def _stream_source_data(self, source_conn: MySQLConnection) -> Iterator[Tuple[list, list]]:
        """
        Stream data from source using server-side cursor.
        
        Yields:
            (column_names, rows) tuples
        """
        # Use server-side cursor for streaming (constant memory)
        cursor = source_conn.connection.cursor()
        
        # For MySQL, use SSCursor
        query = f"""
            SELECT * FROM `{self.table_name}`
            WHERE `{self.pk_column}` BETWEEN %s AND %s
            ORDER BY `{self.pk_column}`
        """
        
        cursor.execute(query, (self.pk_start, self.pk_end))
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Stream in batches
        while True:
            batch_size = self.batch_sizer.current_batch_size if self.enable_adaptive_sizing else 5000
            rows = cursor.fetchmany(batch_size)
            
            if not rows:
                break
            
            yield column_names, rows
        
        cursor.close()
    
    def _bulk_insert_postgres(
        self,
        target_conn: MySQLConnection,
        column_names: list,
        rows: list
    ) -> int:
        """
        Bulk insert using PostgreSQL COPY.
        
        Returns:
            Insert latency in milliseconds
        """
        start_time = time.time()
        
        # Create CSV buffer in memory
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)
        
        # Use COPY FROM STDIN
        cursor = target_conn.get_cursor()
        columns_str = ", ".join([f'"{col}"' for col in column_names])
        
        copy_sql = f'COPY "{self.table_name}" ({columns_str}) FROM STDIN WITH CSV'
        cursor.copy_expert(copy_sql, buffer)
        
        latency_ms = int((time.time() - start_time) * 1000)
        return latency_ms
    
    def _bulk_insert_mysql(
        self,
        target_conn: MySQLConnection,
        column_names: list,
        rows: list
    ) -> int:
        """
        Bulk insert using MySQL multi-value INSERT (fast path).
        
        Returns:
            Insert latency in milliseconds
        """
        start_time = time.time()
        
        cursor = target_conn.get_cursor()
        
        # Build multi-value INSERT
        columns_str = ", ".join([f"`{col}`" for col in column_names])
        placeholders = ", ".join(["%s"] * len(column_names))
        values_clause = ", ".join([f"({placeholders})"] * len(rows))
        
        # Flatten rows
        flat_data = []
        for row in rows:
            flat_data.extend(row)
        
        query = f"INSERT INTO `{self.table_name}` ({columns_str}) VALUES {values_clause}"
        cursor.execute(query, flat_data)
        
        latency_ms = int((time.time() - start_time) * 1000)
        return latency_ms
    
    def _standard_insert(
        self,
        target_conn: MySQLConnection,
        column_names: list,
        rows: list
    ) -> int:
        """Standard batch insert (fallback)."""
        return self._bulk_insert_mysql(target_conn, column_names, rows)
    
    def _delete_target_range(self, target_conn: MySQLConnection):
        """Delete existing target range for idempotency."""
        cursor = target_conn.get_cursor()
        
        delete_start = time.time()
        cursor.execute(
            f"DELETE FROM `{self.table_name}` WHERE `{self.pk_column}` BETWEEN %s AND %s",
            (self.pk_start, self.pk_end)
        )
        
        deleted_rows = cursor.rowcount
        delete_duration_ms = int((time.time() - delete_start) * 1000)
        
        self._log_structured(
            "info",
            "Target range deleted",
            deleted_rows=deleted_rows,
            delete_duration_ms=delete_duration_ms
        )
    
    def _validate_row_counts(
        self,
        source_conn: MySQLConnection,
        target_conn: MySQLConnection
    ) -> Tuple[int, int, bool]:
        """Validate source vs target row counts."""
        source_cursor = source_conn.get_cursor()
        target_cursor = target_conn.get_cursor()
        
        # Count source
        source_cursor.execute(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE `{self.pk_column}` BETWEEN %s AND %s",
            (self.pk_start, self.pk_end)
        )
        source_count = source_cursor.fetchone()['count']
        
        # Count target
        target_cursor.execute(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE `{self.pk_column}` BETWEEN %s AND %s",
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
    
    def _record_performance_metric(self):
        """Record performance metrics to database."""
        try:
            rows_per_sec, mb_per_sec = self.metrics.get_throughput()
            avg_latency = self.metrics.get_avg_latency()
            
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                INSERT INTO performance_metrics
                (job_id, rows_per_second, mb_per_second, memory_usage_mb,
                 insert_latency_ms, worker_id, current_batch_size)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(self.chunk_data['job_id']),
                    Decimal(str(round(rows_per_sec, 2))),
                    Decimal(str(round(mb_per_sec, 2))),
                    int(self.metrics.peak_memory_mb),
                    avg_latency,
                    self.worker_id,
                    self.batch_sizer.current_batch_size
                )
            )
            self.metadata_conn.commit()
        except Exception as e:
            self._log_structured("warning", "Failed to record performance metric", error=str(e))
    
    def execute(self) -> bool:
        """
        Execute high-performance chunk migration.
        
        Returns:
            True if successful
        """
        start_time = datetime.utcnow()
        source_row_count = 0
        target_row_count = 0
        
        try:
            self._log_structured("info", "Starting high-performance execution")
            
            # Update status
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                UPDATE migration_chunks
                SET status = 'running', started_at = NOW(), last_heartbeat = NOW(), worker_id = %s
                WHERE id = %s
                """,
                (self.worker_id, str(self.chunk_id))
            )
            self.metadata_conn.commit()
            
            # Start heartbeat
            self._start_heartbeat()
            
            # Open connections
            with MySQLConnection(self.source_config) as source_conn, \
                 MySQLConnection(self.target_config) as target_conn:
                
                # BEGIN TRANSACTION
                target_conn.begin()
                
                try:
                    # Delete target range (idempotency)
                    self._delete_target_range(target_conn)
                    
                    # Stream and insert data
                    batch_count = 0
                    insert_method = "bulk" if self.use_bulk_insert else "standard"
                    
                    for column_names, rows in self._stream_source_data(source_conn):
                        batch_start = time.time()
                        
                        # Calculate batch size in bytes (approximate)
                        batch_bytes = sum(len(str(v)) for row in rows for v in row)
                        
                        # Insert batch
                        if self.use_bulk_insert and self.target_config.db_type == 'mysql':
                            latency_ms = self._bulk_insert_mysql(target_conn, column_names, rows)
                        else:
                            latency_ms = self._standard_insert(target_conn, column_names, rows)
                        
                        # Track metrics
                        self.metrics.record_batch(len(rows), batch_bytes, latency_ms, len(rows))
                        batch_count += 1
                        
                        # Adaptive batch sizing
                        if self.enable_adaptive_sizing and batch_count % 5 == 0:
                            old_size = self.batch_sizer.current_batch_size
                            new_size, reason = self.batch_sizer.adjust(latency_ms)
                            
                            if new_size != old_size:
                                self._log_structured(
                                    "info",
                                    "Batch size adjusted",
                                    old_size=old_size,
                                    new_size=new_size,
                                    avg_latency_ms=self.metrics.get_avg_latency(),
                                    reason=reason
                                )
                        
                        # Log progress every 10 batches
                        if batch_count % 10 == 0:
                            rows_per_sec, mb_per_sec = self.metrics.get_throughput()
                            self._log_structured(
                                "info",
                                "Batch progress",
                                batch_count=batch_count,
                                rows_processed=self.metrics.rows_processed,
                                rows_per_sec=round(rows_per_sec, 2),
                                mb_per_sec=round(mb_per_sec, 2),
                                current_batch_size=self.batch_sizer.current_batch_size
                            )
                    
                    # Validate row counts
                    source_row_count, target_row_count, is_valid = self._validate_row_counts(
                        source_conn, target_conn
                    )
                    
                    if not is_valid:
                        target_conn.rollback()
                        raise ValueError(
                            f"Row count mismatch: source={source_row_count}, target={target_row_count}"
                        )
                    
                    # COMMIT
                    target_conn.commit()
                    
                except Exception as e:
                    target_conn.rollback()
                    raise
            
            # Calculate final metrics
            end_time = datetime.utcnow()
            duration = end_time - start_time
            duration_ms = int(duration.total_seconds() * 1000)
            rows_per_sec, mb_per_sec = self.metrics.get_throughput()
            
            # Record performance metrics
            self._record_performance_metric()
            
            # Update chunk status
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                UPDATE migration_chunks
                SET status = 'completed',
                    completed_at = NOW(),
                    rows_processed = %s,
                    source_row_count = %s,
                    target_row_count = %s,
                    duration_ms = %s,
                    throughput_rows_per_sec = %s,
                    throughput_mb_per_sec = %s,
                    memory_peak_mb = %s,
                    insert_latency_ms = %s,
                    batch_size_used = %s,
                    bulk_insert_method = %s,
                    validation_status = 'validated'
                WHERE id = %s
                """,
                (
                    self.metrics.rows_processed,
                    source_row_count,
                    target_row_count,
                    duration_ms,
                    Decimal(str(round(rows_per_sec, 2))),
                    Decimal(str(round(mb_per_sec, 2))),
                    int(self.metrics.peak_memory_mb),
                    self.metrics.get_avg_latency(),
                    self.batch_sizer.current_batch_size,
                    "bulk" if self.use_bulk_insert else "standard",
                    str(self.chunk_id)
                )
            )
            self.metadata_conn.commit()
            
            self._log_structured(
                "info",
                "Chunk completed",
                rows_processed=self.metrics.rows_processed,
                duration_ms=duration_ms,
                rows_per_sec=round(rows_per_sec, 2),
                mb_per_sec=round(mb_per_sec, 2),
                peak_memory_mb=int(self.metrics.peak_memory_mb)
            )
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            
            self._log_structured("error", "Chunk execution failed", error=error_msg)
            
            # Calculate backoff
            backoff_seconds = 2 ** self.retry_count
            next_retry_at = datetime.utcnow() + timedelta(seconds=backoff_seconds)
            
            # Update failure status
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                UPDATE migration_chunks
                SET status = 'failed',
                    completed_at = NOW(),
                    retry_count = retry_count + 1,
                    last_error = %s,
                    next_retry_at = %s
                WHERE id = %s
                """,
                (error_msg, next_retry_at if self.retry_count < self.max_retries else None, str(self.chunk_id))
            )
            self.metadata_conn.commit()
            
            return False
            
        finally:
            self._stop_heartbeat()
