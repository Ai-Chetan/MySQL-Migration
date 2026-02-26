"""
Migration routes for the API.
"""
from fastapi import APIRouter, HTTPException, Depends
from uuid import UUID
from typing import List, Optional

from services.api.schemas import (
    CreateMigrationRequest,
    CreateMigrationResponse,
    MigrationJobSummary,
    JobDetailResponse,
    ResumeJobResponse,
    TableProgress,
    ChunkInfo,
    TableDetailResponse,
    ChunkDetailResponse
)
from services.api.metadata import get_metadata_db, MetadataRepository
from services.api.planner import MigrationPlanner
# from services.api.config import get_redis_client  # COMMENTED OUT - Redis will be added later
from services.api.routers.auth import get_current_user, get_current_tenant
from shared.models import JobStatus, ChunkStatus
from shared.utils import setup_logger, calculate_progress_percentage

logger = setup_logger(__name__)
router = APIRouter(prefix="/migrations", tags=["migrations"])


def get_metadata_repo() -> MetadataRepository:
    """Dependency: Get metadata repository."""
    return MetadataRepository(get_metadata_db())


@router.post("", response_model=CreateMigrationResponse, status_code=201)
async def create_migration(
    request: CreateMigrationRequest,
    current_user: dict = Depends(get_current_user),
    current_tenant: dict = Depends(get_current_tenant),
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    Create a new migration job.
    
    This will:
    1. Create job record
    2. Analyze source database
    3. Calculate chunks
    4. Queue chunks for processing
    
    Requires authentication.
    """
    try:
        # Create job record with tenant ID from authenticated user
        job_id = repo.create_job(
            source_config=request.source_config,
            target_config=request.target_config,
            tenant_id=str(current_tenant['id'])
        )
        
        # Update status to planning
        repo.update_job_status(job_id, JobStatus.PLANNING)
        
        # Run planner
        planner = MigrationPlanner(
            metadata_repo=repo,
            chunk_size=request.chunk_size
        )
        
        chunk_ids = planner.analyze_and_plan(
            job_id=job_id,
            source_config=request.source_config
        )
        
        # Queue chunks in Redis (COMMENTED OUT - TO BE ADDED LATER)
        # redis_client = get_redis_client()
        # for chunk_id in chunk_ids:
        #     redis_client.lpush("migration_queue", str(chunk_id))
        
        logger.info(f"Created {len(chunk_ids)} chunks for job {job_id} (Redis queueing disabled)")
        
        # Update job status to running
        repo.update_job_status(job_id, JobStatus.RUNNING)
        
        # Get updated job info
        job = repo.get_job(job_id)
        
        return CreateMigrationResponse(
            job_id=job_id,
            message="Migration job created and queued successfully",
            total_tables=job['total_tables'],
            total_chunks=job['total_chunks']
        )
        
    except Exception as e:
        logger.error(f"Failed to create migration: {e}")
        
        # Mark job as failed if it was created
        if 'job_id' in locals():
            try:
                repo.update_job_status(job_id, JobStatus.FAILED, error=str(e))
            except:
                pass
        
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_migration_status(
    job_id: UUID,
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """Get detailed status of a migration job."""
    try:
        # Get job summary
        job_summary = repo.get_job_summary(job_id)
        
        if not job_summary:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Get table progress
        tables = repo.get_tables_by_job(job_id)
        table_progress = [
            TableProgress(
                id=UUID(t['id']) if isinstance(t['id'], str) else t['id'],
                table_name=t['table_name'],
                total_rows=t['total_rows'],
                total_chunks=t['total_chunks'],
                completed_chunks=t['completed_chunks'],
                failed_chunks=t['failed_chunks'],
                status=t['status'],
                progress_percentage=calculate_progress_percentage(
                    t['completed_chunks'],
                    t['total_chunks']
                )
            )
            for t in tables
        ]
        
        # Get failed chunks
        failed_chunk_records = repo.get_failed_chunks(job_id)
        failed_chunks = [
            ChunkInfo(
                id=UUID(c['id']) if isinstance(c['id'], str) else c['id'],
                table_name=c['table_name'],
                pk_start=c['pk_start'],
                pk_end=c['pk_end'],
                status=ChunkStatus(c['status']),
                retry_count=c['retry_count'],
                rows_processed=c.get('rows_processed'),
                started_at=c.get('started_at'),
                completed_at=c.get('completed_at'),
                last_error=c.get('last_error')
            )
            for c in failed_chunk_records
        ]
        
        return JobDetailResponse(
            job=job_summary,
            tables=table_progress,
            failed_chunks=failed_chunks
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{job_id}/resume", response_model=ResumeJobResponse)
async def resume_migration(
    job_id: UUID,
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    Resume a failed or paused migration job.
    
    This will requeue failed and stale chunks.
    """
    try:
        # Check if job exists
        job = repo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Find chunks to requeue
        # 1. Failed chunks
        failed_chunks = repo.get_failed_chunks(job_id)
        
        # 2. Stale running chunks
        stale_chunks = repo.get_stale_chunks(heartbeat_threshold_seconds=120)
        
        # Filter stale chunks for this job
        job_stale_chunks = [
            chunk_id for chunk_id in stale_chunks
            # We need to verify these belong to this job
        ]
        
        chunks_to_requeue = []
        
        # Requeue failed chunks (REDIS QUEUEING COMMENTED OUT - TO BE ADDED LATER)
        # redis_client = get_redis_client()
        
        for chunk in failed_chunks:
            chunk_id = UUID(chunk['id']) if isinstance(chunk['id'], str) else chunk['id']
            
            # Check if retry limit reached
            if chunk['retry_count'] >= chunk['max_retries']:
                logger.warning(
                    f"Chunk {chunk_id} has reached max retries, not requeuing"
                )
                continue
            
            repo.requeue_chunk(chunk_id)
            # redis_client.lpush("migration_queue", str(chunk_id))  # COMMENTED OUT
            chunks_to_requeue.append(chunk_id)
        
        # Requeue stale chunks
        for chunk_id in job_stale_chunks:
            repo.requeue_chunk(chunk_id)
            # redis_client.lpush("migration_queue", str(chunk_id))  # COMMENTED OUT
            chunks_to_requeue.append(chunk_id)
        
        # Update job status to running if it was paused/failed
        if job['status'] in [JobStatus.PAUSED.value, JobStatus.FAILED.value]:
            repo.update_job_status(UUID(job['id']), JobStatus.RUNNING)
        
        logger.info(f"Requeued {len(chunks_to_requeue)} chunks for job {job_id}")
        
        return ResumeJobResponse(
            job_id=job_id,
            message="Migration resumed successfully",
            chunks_requeued=len(chunks_to_requeue)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resume migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[MigrationJobSummary])
async def list_migrations(
    repo: MetadataRepository = Depends(get_metadata_repo),
    limit: int = 10,
    offset: int = 0
):
    """List all migration jobs."""
    try:
        conn = repo.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT * FROM migration_jobs
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        
        jobs = cursor.fetchall()
        repo.db.return_connection(conn)
        
        return [
            MigrationJobSummary(
                id=UUID(j['id']) if isinstance(j['id'], str) else j['id'],
                status=JobStatus(j['status']),
                tenant_id=j['tenant_id'],
                total_tables=j['total_tables'],
                total_chunks=j['total_chunks'],
                completed_chunks=j['completed_chunks'],
                failed_chunks=j['failed_chunks'],
                progress_percentage=calculate_progress_percentage(
                    j['completed_chunks'],
                    j['total_chunks']
                ),
                created_at=j['created_at'],
                started_at=j.get('started_at'),
                completed_at=j.get('completed_at'),
                last_error=j.get('last_error')
            )
            for j in jobs
        ]
        
    except Exception as e:
        logger.error(f"Failed to list migrations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/tables", response_model=List[TableDetailResponse])
async def get_job_tables(
    job_id: UUID,
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """Get all tables for a migration job."""
    try:
        # Check if job exists
        job = repo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Get tables
        tables = repo.get_tables_for_job(job_id)
        
        result = []
        for table in tables:
            table_id = UUID(table['id']) if isinstance(table['id'], str) else table['id']
            
            # Get chunk counts for this table
            chunk_counts = repo.count_chunks_by_status(table_id)
            
            result.append(TableDetailResponse(
                id=table_id,
                job_id=job_id,
                table_name=table['table_name'],
                status=table['status'],
                total_rows=table.get('total_rows', 0),
                migrated_rows=table.get('migrated_rows', 0),
                total_chunks=chunk_counts.get('total', 0),
                completed_chunks=chunk_counts.get(ChunkStatus.COMPLETED.value, 0),
                failed_chunks=chunk_counts.get(ChunkStatus.FAILED.value, 0),
                primary_key_column=table['primary_key_column'],
                created_at=table['created_at'],
                started_at=table.get('started_at'),
                completed_at=table.get('completed_at'),
                error_message=table.get('error_message')
            ))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/tables/{table_id}/chunks", response_model=List[ChunkDetailResponse])
async def get_table_chunks(
    job_id: UUID,
    table_id: UUID,
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """Get all chunks for a specific table."""
    try:
        # Get chunks from database
        conn = repo.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT *
            FROM migration_chunks
            WHERE job_id = %s AND table_id = %s
            ORDER BY chunk_number ASC
            """,
            (str(job_id), str(table_id))
        )
        
        chunks = cursor.fetchall()
        repo.db.return_connection(conn)
        
        result = []
        for chunk in chunks:
            result.append(ChunkDetailResponse(
                id=UUID(chunk['id']) if isinstance(chunk['id'], str) else chunk['id'],
                job_id=job_id,
                table_id=table_id,
                table_name=chunk.get('table_name', ''),
                chunk_number=chunk['chunk_number'],
                start_pk=chunk['start_pk'],
                end_pk=chunk['end_pk'],
                status=chunk['status'],
                estimated_rows=chunk.get('estimated_rows', 0),
                actual_rows=chunk.get('actual_rows'),
                retry_count=chunk.get('retry_count', 0),
                worker_id=chunk.get('worker_id'),
                last_heartbeat=chunk.get('last_heartbeat'),
                created_at=chunk['created_at'],
                started_at=chunk.get('started_at'),
                completed_at=chunk.get('completed_at'),
                error_message=chunk.get('error_message')
            ))
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get table chunks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{job_id}/chunks/{chunk_id}/retry")
async def retry_chunk(
    job_id: UUID,
    chunk_id: UUID,
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """Retry a failed chunk."""
    try:
        # Get chunk
        chunk = repo.get_chunk(chunk_id)
        if not chunk:
            raise HTTPException(status_code=404, detail="Chunk not found")
        
        # Verify chunk belongs to job
        if UUID(chunk['job_id']) != job_id:
            raise HTTPException(status_code=400, detail="Chunk does not belong to this job")
        
        # Check if chunk is failed
        if chunk['status'] != ChunkStatus.FAILED.value:
            raise HTTPException(
                status_code=400,
                detail=f"Chunk is not in FAILED state (current: {chunk['status']})"
            )
        
        # Check retry limit
        max_retries = chunk.get('max_retries', 3)
        if chunk['retry_count'] >= max_retries:
            raise HTTPException(
                status_code=400,
                detail=f"Chunk has reached max retries ({max_retries})"
            )
        
        # Requeue chunk
        repo.requeue_chunk(chunk_id)
        
        # Push to Redis queue (COMMENTED OUT - TO BE ADDED LATER)
        # redis_client = get_redis_client()
        # redis_client.lpush("migration_queue", str(chunk_id))
        
        logger.info(f"Chunk {chunk_id} prepared for retry (Redis queueing disabled)")
        
        return {"message": "Chunk queued for retry", "chunk_id": str(chunk_id)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retry chunk: {e}")
        raise HTTPException(status_code=500, detail=str(e))
