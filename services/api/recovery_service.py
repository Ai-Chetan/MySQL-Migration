"""
Stale Chunk Recovery Service
Detects and recovers chunks that lost heartbeat (worker crashed)
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

from services.api.metadata import get_metadata_db
from shared.utils import setup_logger

logger = setup_logger(__name__)


class StaleChunkRecovery:
    """Detects and recovers stale chunks for crash recovery."""
    
    def __init__(self, heartbeat_timeout_seconds: int = 120):
        """
        Initialize stale chunk recovery service.
        
        Args:
            heartbeat_timeout_seconds: Seconds before chunk considered stale
        """
        self.heartbeat_timeout = heartbeat_timeout_seconds
        self.running = False
        self.task = None
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level.upper(),
            "service": "stale_chunk_recovery",
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
    
    async def detect_and_recover_stale_chunks(self):
        """
        Find and recover chunks that have lost heartbeat.
        
        A chunk is stale if:
        - status = 'running'
        - last_heartbeat < NOW() - timeout
        """
        try:
            metadata_db = get_metadata_db()
            conn = metadata_db.get_connection()
            cursor = conn.cursor()
            
            # Find stale chunks
            cursor.execute(
                """
                SELECT 
                    id,
                    job_id,
                    table_name,
                    pk_start,
                    pk_end,
                    worker_id,
                    retry_count,
                    max_retries,
                    last_heartbeat,
                    EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_stale
                FROM migration_chunks
                WHERE status = 'running'
                AND last_heartbeat < NOW() - INTERVAL '%s seconds'
                """,
                (self.heartbeat_timeout,)
            )
            
            stale_chunks = cursor.fetchall()
            
            if not stale_chunks:
                metadata_db.return_connection(conn)
                return
            
            self._log_structured(
                "warning",
                "Detected stale chunks",
                count=len(stale_chunks)
            )
            
            # Process each stale chunk
            for chunk in stale_chunks:
                chunk_id = chunk['id']
                retry_count = chunk['retry_count']
                max_retries = chunk['max_retries']
                
                # Check if chunk can be retried
                if retry_count < max_retries:
                    # Calculate exponential backoff
                    backoff_seconds = 2 ** retry_count
                    next_retry_at = datetime.utcnow() + timedelta(seconds=backoff_seconds)
                    
                    # Mark as failed and schedule retry
                    cursor.execute(
                        """
                        UPDATE migration_chunks
                        SET 
                            status = 'failed',
                            retry_count = retry_count + 1,
                            next_retry_at = %s,
                            last_error = 'Worker heartbeat lost - crash recovery',
                            completed_at = NOW()
                        WHERE id = %s
                        """,
                        (next_retry_at, str(chunk_id))
                    )
                    
                    self._log_structured(
                        "info",
                        "Stale chunk marked for retry",
                        chunk_id=str(chunk_id),
                        table_name=chunk['table_name'],
                        worker_id=chunk['worker_id'],
                        retry_count=retry_count + 1,
                        next_retry_at=next_retry_at.isoformat()
                    )
                else:
                    # Exceeded max retries - mark as permanently failed
                    cursor.execute(
                        """
                        UPDATE migration_chunks
                        SET 
                            status = 'failed',
                            last_error = 'Worker heartbeat lost - max retries exceeded',
                            completed_at = NOW()
                        WHERE id = %s
                        """,
                        (str(chunk_id),)
                    )
                    
                    self._log_structured(
                        "error",
                        "Stale chunk permanently failed",
                        chunk_id=str(chunk_id),
                        table_name=chunk['table_name'],
                        worker_id=chunk['worker_id'],
                        retry_count=retry_count,
                        max_retries=max_retries
                    )
            
            # Commit all updates
            metadata_db.commit_connection(conn)
            metadata_db.return_connection(conn)
            
            self._log_structured(
                "info",
                "Stale chunk recovery completed",
                recovered=len(stale_chunks)
            )
            
        except Exception as e:
            self._log_structured(
                "error",
                "Stale chunk recovery failed",
                error=str(e)
            )
    
    async def check_job_failure_escalation(self):
        """
        Check if any jobs have exceeded failure threshold and should be marked as failed.
        """
        try:
            metadata_db = get_metadata_db()
            conn = metadata_db.get_connection()
            cursor = conn.cursor()
            
            # Find jobs with high failure rate
            cursor.execute(
                """
                SELECT 
                    j.id,
                    j.tenant_id,
                    j.total_chunks,
                    j.failed_chunks,
                    j.failure_threshold_percent,
                    ROUND((j.failed_chunks::DECIMAL / NULLIF(j.total_chunks, 0)) * 100, 2) as failure_rate
                FROM migration_jobs j
                WHERE j.status IN ('running', 'planning')
                AND j.total_chunks > 0
                AND (j.failed_chunks::DECIMAL / j.total_chunks) * 100 > j.failure_threshold_percent
                AND j.auto_failed_at IS NULL
                """
            )
            
            failing_jobs = cursor.fetchall()
            
            if not failing_jobs:
                metadata_db.return_connection(conn)
                return
            
            # Mark jobs as failed
            for job in failing_jobs:
                job_id = job['id']
                failure_rate = job['failure_rate']
                
                cursor.execute(
                    """
                    UPDATE migration_jobs
                    SET 
                        status = 'failed',
                        auto_failed_at = NOW(),
                        completed_at = NOW()
                    WHERE id = %s
                    """,
                    (str(job_id),)
                )
                
                self._log_structured(
                    "error",
                    "Job auto-failed due to high failure rate",
                    job_id=str(job_id),
                    tenant_id=job['tenant_id'],
                    failure_rate=float(failure_rate),
                    failed_chunks=job['failed_chunks'],
                    total_chunks=job['total_chunks'],
                    threshold=job['failure_threshold_percent']
                )
            
            metadata_db.commit_connection(conn)
            metadata_db.return_connection(conn)
            
            self._log_structured(
                "warning",
                "Jobs auto-failed",
                count=len(failing_jobs)
            )
            
        except Exception as e:
            self._log_structured(
                "error",
                "Failure escalation check failed",
                error=str(e)
            )
    
    async def recovery_loop(self):
        """Main recovery loop - runs every 30 seconds."""
        self._log_structured("info", "Stale chunk recovery service started")
        
        while self.running:
            try:
                # Run stale chunk detection
                await self.detect_and_recover_stale_chunks()
                
                # Check for job failure escalation
                await self.check_job_failure_escalation()
                
            except Exception as e:
                self._log_structured(
                    "error",
                    "Recovery loop error",
                    error=str(e)
                )
            
            # Wait 30 seconds before next check
            await asyncio.sleep(30)
        
        self._log_structured("info", "Stale chunk recovery service stopped")
    
    def start(self):
        """Start the recovery service."""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self.recovery_loop())
        self._log_structured("info", "Recovery service start requested")
    
    async def stop(self):
        """Stop the recovery service."""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            await self.task
        
        self._log_structured("info", "Recovery service stopped")


# Global recovery service instance
_recovery_service = None


def get_recovery_service() -> StaleChunkRecovery:
    """Get global recovery service instance."""
    global _recovery_service
    if _recovery_service is None:
        _recovery_service = StaleChunkRecovery(heartbeat_timeout_seconds=120)
    return _recovery_service
