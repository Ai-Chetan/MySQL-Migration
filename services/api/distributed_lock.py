"""
Distributed Locking Mechanism
Prevents double-processing in multi-node environments
"""
import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from uuid import UUID, uuid4
from contextlib import contextmanager

from services.worker.db import MetadataConnection

logger = logging.getLogger(__name__)


class DistributedLock:
    """
    Database-based distributed lock for job and chunk processing.
    
    Uses PostgreSQL advisory locks and row-level locking for coordination
    across multiple worker pods.
    """
    
    def __init__(self, db: MetadataConnection):
        self.db = db
    
    @contextmanager
    def acquire_job_lock(self, job_id: str, worker_id: str, timeout: int = 30):
        """
        Acquire exclusive lock on a migration job.
        
        Args:
            job_id: Job identifier
            worker_id: Worker identifier
            timeout: Lock timeout in seconds
        
        Yields:
            True if lock acquired, False otherwise
        """
        conn = None
        acquired = False
        lock_key = None
        
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Use PostgreSQL advisory lock
            # Hash job_id to integer for lock key
            lock_key = abs(hash(job_id)) % (2**31)
            
            # Try to acquire lock with timeout
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (lock_key,)
            )
            
            result = cursor.fetchone()
            acquired = result[0] if result else False
            
            if acquired:
                # Update job with lock information
                cursor.execute(
                    """
                    UPDATE migration_jobs
                    SET 
                        locked_by = %s,
                        locked_at = %s
                    WHERE id = %s
                    """,
                    (worker_id, datetime.utcnow(), job_id)
                )
                conn.commit()
                
                logger.info(f"Acquired job lock for {job_id} by worker {worker_id}")
            else:
                logger.warning(f"Failed to acquire job lock for {job_id}")
            
            yield acquired
            
        except Exception as e:
            logger.error(f"Error acquiring job lock: {e}")
            if conn:
                conn.rollback()
            yield False
            
        finally:
            # Release lock
            if acquired and lock_key is not None and conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
                    
                    # Clear lock information
                    cursor.execute(
                        """
                        UPDATE migration_jobs
                        SET locked_by = NULL, locked_at = NULL
                        WHERE id = %s
                        """,
                        (job_id,)
                    )
                    conn.commit()
                    logger.info(f"Released job lock for {job_id}")
                except Exception as e:
                    logger.error(f"Error releasing job lock: {e}")
            
            if conn:
                self.db.return_connection(conn)
    
    @contextmanager
    def acquire_chunk_lock(self, chunk_id: str, worker_id: str):
        """
        Acquire exclusive lock on a chunk using SELECT FOR UPDATE SKIP LOCKED.
        
        This ensures only one worker processes a chunk at a time.
        
        Args:
            chunk_id: Chunk identifier
            worker_id: Worker identifier
        
        Yields:
            Chunk data if lock acquired, None otherwise
        """
        conn = None
        chunk_data = None
        
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Try to lock chunk row
            cursor.execute(
                """
                SELECT 
                    id, chunk_number, start_offset, end_offset,
                    status, table_id, job_id
                FROM migration_chunks
                WHERE id = %s
                    AND status = 'pending'
                FOR UPDATE SKIP LOCKED
                """,
                (chunk_id,)
            )
            
            chunk = cursor.fetchone()
            
            if chunk:
                # Update chunk status to 'running' with worker info
                cursor.execute(
                    """
                    UPDATE migration_chunks
                    SET 
                        status = 'running',
                        worker_id = %s,
                        started_at = %s,
                        heartbeat = %s
                    WHERE id = %s
                    """,
                    (worker_id, datetime.utcnow(), datetime.utcnow(), chunk_id)
                )
                conn.commit()
                
                chunk_data = dict(chunk)
                logger.info(f"Acquired chunk lock for {chunk_id} by worker {worker_id}")
            else:
                logger.debug(f"Chunk {chunk_id} is locked or already processed")
            
            yield chunk_data
            
        except Exception as e:
            logger.error(f"Error acquiring chunk lock: {e}")
            if conn:
                conn.rollback()
            yield None
            
        finally:
            if conn:
                self.db.return_connection(conn)
    
    def is_job_locked(self, job_id: str) -> bool:
        """Check if a job is currently locked."""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT locked_by, locked_at
                FROM migration_jobs
                WHERE id = %s
                """,
                (job_id,)
            )
            
            result = cursor.fetchone()
            self.db.return_connection(conn)
            
            if result and result['locked_by']:
                # Check if lock is stale (older than 5 minutes)
                locked_at = result['locked_at']
                if locked_at and (datetime.utcnow() - locked_at) > timedelta(minutes=5):
                    logger.warning(f"Stale lock detected for job {job_id}")
                    # Clear stale lock
                    self.clear_stale_lock(job_id)
                    return False
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking job lock: {e}")
            return False
    
    def clear_stale_lock(self, job_id: str):
        """Clear stale lock on a job."""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """
                UPDATE migration_jobs
                SET locked_by = NULL, locked_at = NULL
                WHERE id = %s
                    AND locked_at < %s
                """,
                (job_id, datetime.utcnow() - timedelta(minutes=5))
            )
            
            conn.commit()
            self.db.return_connection(conn)
            
            logger.info(f"Cleared stale lock for job {job_id}")
            
        except Exception as e:
            logger.error(f"Error clearing stale lock: {e}")
    
    def heartbeat_chunk(self, chunk_id: str):
        """
        Update chunk heartbeat to indicate worker is still processing.
        
        Called periodically during chunk processing.
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """
                UPDATE migration_chunks
                SET heartbeat = %s
                WHERE id = %s
                """,
                (datetime.utcnow(), chunk_id)
            )
            
            conn.commit()
            self.db.return_connection(conn)
            
        except Exception as e:
            logger.error(f"Error updating chunk heartbeat: {e}")
    
    def recover_stale_chunks(self, stale_threshold_minutes: int = 10):
        """
        Recover chunks that have stale heartbeats.
        
        Resets chunks to 'pending' if worker appears to have crashed.
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            stale_time = datetime.utcnow() - timedelta(minutes=stale_threshold_minutes)
            
            cursor.execute(
                """
                UPDATE migration_chunks
                SET 
                    status = 'pending',
                    worker_id = NULL,
                    error_message = 'Recovered from stale heartbeat'
                WHERE status = 'running'
                    AND heartbeat < %s
                RETURNING id, job_id
                """,
                (stale_time,)
            )
            
            recovered = cursor.fetchall()
            conn.commit()
            self.db.return_connection(conn)
            
            if recovered:
                logger.warning(f"Recovered {len(recovered)} stale chunks")
                for chunk in recovered:
                    logger.info(f"Recovered chunk {chunk['id']} from job {chunk['job_id']}")
            
            return len(recovered)
            
        except Exception as e:
            logger.error(f"Error recovering stale chunks: {e}")
            return 0


# Global instance
_lock_manager = None


def get_distributed_lock(db: MetadataConnection = None) -> DistributedLock:
    """Get global distributed lock manager instance."""
    global _lock_manager
    if _lock_manager is None:
        if db is None:
            from services.api.metadata import get_metadata_db
            db = get_metadata_db()
        _lock_manager = DistributedLock(db)
    return _lock_manager
