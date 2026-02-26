"""
Metadata repository for managing migration jobs, tables, and chunks.
"""
from uuid import UUID, uuid4
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from shared.models import (
    JobStatus, ChunkStatus, TableStatus,
    MigrationJobSummary, TableProgress, ChunkInfo,
    DatabaseConfig, ChunkRange
)
from shared.utils import setup_logger, calculate_progress_percentage
from services.api.metadata.db import MetadataDB

logger = setup_logger(__name__)


class MetadataRepository:
    """Repository for metadata database operations."""
    
    def __init__(self, db: MetadataDB):
        """
        Initialize repository.
        
        Args:
            db: MetadataDB instance
        """
        self.db = db
    
    # ===== JOB OPERATIONS =====
    
    def create_job(
        self,
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        tenant_id: str = "local"
    ) -> UUID:
        """
        Create a new migration job.
        
        Args:
            source_config: Source database configuration
            target_config: Target database configuration
            tenant_id: Tenant identifier
        
        Returns:
            Job UUID
        """
        job_id = uuid4()
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO migration_jobs (
                    id, tenant_id, status, source_config, target_config
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    str(job_id),
                    tenant_id,
                    JobStatus.PENDING.value,
                    json.dumps(source_config.model_dump()),
                    json.dumps(target_config.model_dump())
                )
            )
            conn.commit()
            logger.info(f"Created migration job: {job_id}")
            return job_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create job: {e}")
            raise
        finally:
            self.db.return_connection(conn)
    
    def update_job_status(
        self,
        job_id: UUID,
        status: JobStatus,
        error: Optional[str] = None
    ):
        """
        Update job status.
        
        Args:
            job_id: Job UUID
            status: New status
            error: Error message if failed
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            
            # Build update query dynamically
            updates = ["status = %s"]
            params = [status.value]
            
            if status == JobStatus.RUNNING:
                updates.append("started_at = NOW()")
            elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
                updates.append("completed_at = NOW()")
            
            if error:
                updates.append("last_error = %s")
                params.append(error)
            
            params.append(str(job_id))
            
            query = f"""
                UPDATE migration_jobs
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            cursor.execute(query, params)
            conn.commit()
            logger.info(f"Updated job {job_id} status to {status.value}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update job status: {e}")
            raise
        finally:
            self.db.return_connection(conn)
    
    def get_job(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get job by ID.
        
        Args:
            job_id: Job UUID
        
        Returns:
            Job record or None
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM migration_jobs WHERE id = %s",
                (str(job_id),)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
        finally:
            self.db.return_connection(conn)
    
    def get_job_summary(self, job_id: UUID) -> Optional[MigrationJobSummary]:
        """
        Get job summary with progress.
        
        Args:
            job_id: Job UUID
        
        Returns:
            MigrationJobSummary or None
        """
        job = self.get_job(job_id)
        if not job:
            return None
        
        progress = calculate_progress_percentage(
            job['completed_chunks'],
            job['total_chunks']
        )
        
        return MigrationJobSummary(
            id=UUID(job['id']) if isinstance(job['id'], str) else job['id'],
            status=JobStatus(job['status']),
            tenant_id=job['tenant_id'],
            total_tables=job['total_tables'],
            total_chunks=job['total_chunks'],
            completed_chunks=job['completed_chunks'],
            failed_chunks=job['failed_chunks'],
            progress_percentage=progress,
            created_at=job['created_at'],
            started_at=job.get('started_at'),
            completed_at=job.get('completed_at'),
            last_error=job.get('last_error')
        )
    
    # ===== TABLE OPERATIONS =====
    
    def create_table(
        self,
        job_id: UUID,
        table_name: str,
        primary_key_column: str,
        total_rows: int
    ) -> UUID:
        """
        Create a migration table record.
        
        Args:
            job_id: Parent job UUID
            table_name: Table name
            primary_key_column: Primary key column name
            total_rows: Total row count
        
        Returns:
            Table UUID
        """
        table_id = uuid4()
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO migration_tables (
                    id, job_id, table_name, primary_key_column, total_rows
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (str(table_id), str(job_id), table_name, primary_key_column, total_rows)
            )
            conn.commit()
            logger.info(f"Created table record: {table_name} ({table_id})")
            return table_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create table record: {e}")
            raise
        finally:
            self.db.return_connection(conn)
    
    def get_tables_by_job(self, job_id: UUID) -> List[Dict[str, Any]]:
        """
        Get all tables for a job.
        
        Args:
            job_id: Job UUID
        
        Returns:
            List of table records
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM migration_tables WHERE job_id = %s ORDER BY table_name",
                (str(job_id),)
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.db.return_connection(conn)
    
    # ===== CHUNK OPERATIONS =====
    
    def create_chunk(
        self,
        job_id: UUID,
        table_id: UUID,
        table_name: str,
        pk_start: int,
        pk_end: int,
        max_retries: int = 3
    ) -> UUID:
        """
        Create a migration chunk record.
        
        Args:
            job_id: Parent job UUID
            table_id: Parent table UUID
            table_name: Table name
            pk_start: Start of PK range
            pk_end: End of PK range
            max_retries: Maximum retry attempts
        
        Returns:
            Chunk UUID
        """
        chunk_id = uuid4()
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO migration_chunks (
                    id, job_id, table_id, table_name, 
                    pk_start, pk_end, max_retries
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(chunk_id), str(job_id), str(table_id), table_name,
                    pk_start, pk_end, max_retries
                )
            )
            conn.commit()
            return chunk_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create chunk: {e}")
            raise
        finally:
            self.db.return_connection(conn)
    
    def update_chunk_status(
        self,
        chunk_id: UUID,
        status: ChunkStatus,
        rows_processed: Optional[int] = None,
        error: Optional[str] = None,
        duration_ms: Optional[int] = None
    ):
        """
        Update chunk status.
        
        Args:
            chunk_id: Chunk UUID
            status: New status
            rows_processed: Number of rows processed
            error: Error message if failed
            duration_ms: Processing duration in milliseconds
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            
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
            
            params.append(str(chunk_id))
            
            query = f"""
                UPDATE migration_chunks
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update chunk status: {e}")
            raise
        finally:
            self.db.return_connection(conn)
    
    def update_chunk_heartbeat(self, chunk_id: UUID):
        """
        Update chunk heartbeat timestamp.
        
        Args:
            chunk_id: Chunk UUID
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE migration_chunks SET last_heartbeat = NOW() WHERE id = %s",
                (str(chunk_id),)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update heartbeat: {e}")
        finally:
            self.db.return_connection(conn)
    
    def get_failed_chunks(self, job_id: UUID) -> List[Dict[str, Any]]:
        """
        Get all failed chunks for a job.
        
        Args:
            job_id: Job UUID
        
        Returns:
            List of failed chunk records
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM migration_chunks 
                WHERE job_id = %s AND status = 'failed'
                ORDER BY table_name, pk_start
                """,
                (str(job_id),)
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.db.return_connection(conn)
    
    def get_stale_chunks(self, heartbeat_threshold_seconds: int = 120) -> List[UUID]:
        """
        Find chunks stuck in running state.
        
        Args:
            heartbeat_threshold_seconds: Seconds before considering stale
        
        Returns:
            List of stale chunk UUIDs
        """
        conn = self.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM detect_stale_chunks(%s)",
                (heartbeat_threshold_seconds,)
            )
            return [UUID(row['chunk_id']) for row in cursor.fetchall()]
        finally:
            self.db.return_connection(conn)
    
    def requeue_chunk(self, chunk_id: UUID):
        """
        Reset chunk to pending for requeue.
        
        Args:
            chunk_id: Chunk UUID
        """
        self.update_chunk_status(chunk_id, ChunkStatus.PENDING)
