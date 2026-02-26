"""
Worker service - processes migration chunks from Redis queue.
"""
import redis
import json
import time
import signal
import sys
from uuid import UUID, uuid4
from typing import Optional
from datetime import datetime

from shared.models import DatabaseConfig, ChunkStatus
from shared.utils import setup_logger
from services.worker.config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_QUEUE_NAME,
    METADATA_DB_HOST, METADATA_DB_PORT, METADATA_DB_NAME,
    METADATA_DB_USER, METADATA_DB_PASSWORD,
    WORKER_ID, BATCH_SIZE, QUEUE_POLL_TIMEOUT,
    HEARTBEAT_INTERVAL_SECONDS
)
from services.worker.db import MetadataConnection
from services.worker.executor import ChunkExecutor

logger = setup_logger(__name__, level="INFO")


class Worker:
    """Migration worker that processes chunks from queue."""
    
    def __init__(self, worker_id: Optional[str] = None):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier
        """
        self.worker_id = worker_id or WORKER_ID or str(uuid4())
        self.running = True
        self.current_chunk_id: Optional[UUID] = None
        
        # Initialize Redis client
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        
        # Initialize metadata connection
        self.metadata_conn = MetadataConnection(
            host=METADATA_DB_HOST,
            port=METADATA_DB_PORT,
            database=METADATA_DB_NAME,
            user=METADATA_DB_USER,
            password=METADATA_DB_PASSWORD
        )
        
        logger.info(f"Worker {self.worker_id} initialized")
    
    def start(self):
        """Start worker main loop."""
        logger.info(f"Worker {self.worker_id} starting...")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Connect to metadata DB
        self.metadata_conn.connect()
        
        # Register worker heartbeat
        self._register_worker()
        
        # Main processing loop
        while self.running:
            try:
                # Poll queue for chunk
                chunk_id_str = self._poll_queue()
                
                if chunk_id_str:
                    self._process_chunk(chunk_id_str)
                else:
                    # No work available, short sleep
                    time.sleep(1)
                    
                # Update worker heartbeat
                self._update_worker_heartbeat()
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(5)  # Back off on error
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    def _poll_queue(self) -> Optional[str]:
        """
        Poll Redis queue for next chunk.
        
        Returns:
            Chunk ID string or None
        """
        try:
            # BRPOP blocks until item available or timeout
            result = self.redis_client.brpop(
                REDIS_QUEUE_NAME,
                timeout=QUEUE_POLL_TIMEOUT
            )
            
            if result:
                queue_name, chunk_id_str = result
                logger.debug(f"Dequeued chunk: {chunk_id_str}")
                return chunk_id_str
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to poll queue: {e}")
            return None
    
    def _process_chunk(self, chunk_id_str: str):
        """
        Process a single chunk.
        
        Args:
            chunk_id_str: Chunk UUID as string
        """
        try:
            chunk_id = UUID(chunk_id_str)
            self.current_chunk_id = chunk_id
            
            logger.info(f"Processing chunk {chunk_id}")
            
            # Fetch chunk metadata
            chunk_data = self._get_chunk_metadata(chunk_id)
            
            if not chunk_data:
                logger.error(f"Chunk {chunk_id} not found in metadata")
                return
            
            # Check if chunk is already completed or max retries reached
            if chunk_data['status'] == ChunkStatus.COMPLETED.value:
                logger.info(f"Chunk {chunk_id} already completed, skipping")
                return
            
            if chunk_data['retry_count'] >= chunk_data['max_retries']:
                logger.warning(
                    f"Chunk {chunk_id} has reached max retries, skipping"
                )
                return
            
            # Get job configuration
            job_id = UUID(chunk_data['job_id']) if isinstance(chunk_data['job_id'], str) else chunk_data['job_id']
            job_data = self._get_job_metadata(job_id)
            
            if not job_data:
                logger.error(f"Job {job_id} not found in metadata")
                return
            
            # Parse database configurations
            source_config = DatabaseConfig(**json.loads(job_data['source_config']))
            target_config = DatabaseConfig(**json.loads(job_data['target_config']))
            
            # Execute chunk
            executor = ChunkExecutor(
                chunk_id=chunk_id,
                chunk_data=chunk_data,
                source_config=source_config,
                target_config=target_config,
                metadata_conn=self.metadata_conn,
                batch_size=BATCH_SIZE
            )
            
            success = executor.execute()
            
            if success:
                logger.info(f"Chunk {chunk_id} completed successfully")
            else:
                logger.error(f"Chunk {chunk_id} failed, may be retried")
                
                # Requeue if retries remaining
                if chunk_data['retry_count'] + 1 < chunk_data['max_retries']:
                    self.redis_client.lpush(REDIS_QUEUE_NAME, str(chunk_id))
                    logger.info(f"Chunk {chunk_id} requeued for retry")
            
        except Exception as e:
            logger.error(f"Error processing chunk: {e}")
        finally:
            self.current_chunk_id = None
    
    def _get_chunk_metadata(self, chunk_id: UUID) -> Optional[dict]:
        """
        Fetch chunk metadata from database.
        
        Args:
            chunk_id: Chunk UUID
        
        Returns:
            Chunk metadata dict or None
        """
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                "SELECT * FROM migration_chunks WHERE id = %s",
                (str(chunk_id),)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get chunk metadata: {e}")
            return None
    
    def _get_job_metadata(self, job_id: UUID) -> Optional[dict]:
        """
        Fetch job metadata from database.
        
        Args:
            job_id: Job UUID
        
        Returns:
            Job metadata dict or None
        """
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                "SELECT * FROM migration_jobs WHERE id = %s",
                (str(job_id),)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get job metadata: {e}")
            return None
    
    def _register_worker(self):
        """Register worker in heartbeat table."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                INSERT INTO worker_heartbeats (worker_id, last_seen, status)
                VALUES (%s, NOW(), 'active')
                ON CONFLICT (worker_id) 
                DO UPDATE SET last_seen = NOW(), status = 'active'
                """,
                (self.worker_id,)
            )
            self.metadata_conn.commit()
            logger.info(f"Worker {self.worker_id} registered")
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
    
    def _update_worker_heartbeat(self):
        """Update worker heartbeat timestamp."""
        try:
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                UPDATE worker_heartbeats
                SET last_seen = NOW(), 
                    current_chunk = %s
                WHERE worker_id = %s
                """,
                (str(self.current_chunk_id) if self.current_chunk_id else None, self.worker_id)
            )
            self.metadata_conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update worker heartbeat: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def shutdown(self):
        """Cleanup and shutdown worker."""
        logger.info(f"Worker {self.worker_id} shutting down...")
        
        try:
            # Update worker status to inactive
            cursor = self.metadata_conn.get_cursor()
            cursor.execute(
                """
                UPDATE worker_heartbeats
                SET status = 'inactive', last_seen = NOW()
                WHERE worker_id = %s
                """,
                (self.worker_id,)
            )
            self.metadata_conn.commit()
        except Exception as e:
            logger.error(f"Error updating worker status: {e}")
        
        # Close connections
        if self.metadata_conn:
            self.metadata_conn.close()
        
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Worker shutdown complete")


def main():
    """Main entry point."""
    worker = Worker()
    
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
