"""
Production Monitoring Endpoints
- Job summary with detailed progress
- Table-level metrics
- Chunk details with filtering
- System health checks
- Execution audit logs
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from uuid import UUID
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

from services.api.metadata import get_metadata_db
from services.api.routers.auth import get_current_user, get_current_tenant
from shared.utils import setup_logger

logger = setup_logger(__name__)
router = APIRouter(prefix="/monitoring", tags=["Production Monitoring"])


# Response Models
class JobSummary(BaseModel):
    job_id: UUID
    status: str
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    running_chunks: int
    pending_chunks: int
    rows_processed: int
    completion_percent: float
    failure_percent: float
    eta_seconds: Optional[int]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


class TableMetrics(BaseModel):
    table_name: str
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    running_chunks: int
    pending_chunks: int
    progress_percent: float
    rows_processed: int
    estimated_total_rows: int


class ChunkDetail(BaseModel):
    chunk_id: UUID
    table_name: str
    pk_start: int
    pk_end: int
    status: str
    retry_count: int
    max_retries: int
    worker_id: Optional[str]
    rows_processed: int
    source_row_count: Optional[int]
    target_row_count: Optional[int]
    validation_status: str
    duration_ms: Optional[int]
    last_error: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    last_heartbeat: Optional[datetime]
    next_retry_at: Optional[datetime]


class ExecutionAttempt(BaseModel):
    attempt_id: UUID
    attempt_number: int
    worker_id: str
    status: str
    rows_processed: int
    source_row_count: Optional[int]
    target_row_count: Optional[int]
    duration_ms: Optional[int]
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]


class SystemHealth(BaseModel):
    status: str
    metadata_db_connected: bool
    redis_connected: bool
    active_workers: int
    stale_chunks_detected: int
    recovery_service_running: bool
    timestamp: datetime


@router.get("/jobs/{job_id}/summary", response_model=JobSummary)
async def get_job_summary(
    job_id: UUID,
    current_user: dict = Depends(get_current_user)
):
"""
    Get comprehensive summary for a specific migration job.
    
    Includes:
    - Chunk status distribution
    - Progress percentage
    - Failure rate
    - ETA calculation
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get job details
        cursor.execute(
            """
            SELECT 
                j.id,
                j.status,
                j.total_chunks,
                j.completed_chunks,
                j.failed_chunks,
                j.created_at,
                j.started_at,
                j.completed_at,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM migration_chunks 
                    WHERE job_id = j.id AND status = 'running'
                ), 0) as running_chunks,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM migration_chunks 
                    WHERE job_id = j.id AND status = 'pending'
                ), 0) as pending_chunks,
                COALESCE((
                    SELECT SUM(rows_processed) 
                    FROM migration_chunks 
                    WHERE job_id = j.id AND status = 'completed'
                ), 0) as rows_processed
            FROM migration_jobs j
            WHERE j.id = %s
            """,
            (str(job_id),)
        )
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Calculate percentages
        total_chunks = result['total_chunks'] or 1
        completed_chunks = result['completed_chunks'] or 0
        failed_chunks = result['failed_chunks'] or 0
        
        completion_percent = round((completed_chunks / total_chunks) * 100, 2)
        failure_percent = round((failed_chunks / total_chunks) * 100, 2)
        
        # Calculate ETA
        eta_seconds = None
        if result['started_at'] and completed_chunks > 0:
            elapsed = datetime.utcnow() - result['started_at']
            elapsed_seconds = elapsed.total_seconds()
            chunks_remaining = total_chunks - completed_chunks
            
            if chunks_remaining > 0:
                seconds_per_chunk = elapsed_seconds / completed_chunks
                eta_seconds = int(seconds_per_chunk * chunks_remaining)
        
        db.return_connection(conn)
        
        return JobSummary(
            job_id=result['id'],
            status=result['status'],
            total_chunks=result['total_chunks'],
            completed_chunks=result['completed_chunks'],
            failed_chunks=result['failed_chunks'],
            running_chunks=result['running_chunks'],
            pending_chunks=result['pending_chunks'],
            rows_processed=result['rows_processed'],
            completion_percent=completion_percent,
            failure_percent=failure_percent,
            eta_seconds=eta_seconds,
            created_at=result['created_at'],
            started_at=result['started_at'],
            completed_at=result['completed_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/tables", response_model=List[TableMetrics])
async def get_table_metrics(
    job_id: UUID,
    current_user: dict = Depends(get_current_user)
):
    """
    Get per-table progress metrics for a job.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                t.table_name,
                COUNT(c.id) as total_chunks,
                COUNT(CASE WHEN c.status = 'completed' THEN 1 END) as completed_chunks,
                COUNT(CASE WHEN c.status = 'failed' THEN 1 END) as failed_chunks,
                COUNT(CASE WHEN c.status = 'running' THEN 1 END) as running_chunks,
                COUNT(CASE WHEN c.status = 'pending' THEN 1 END) as pending_chunks,
                COALESCE(SUM(CASE WHEN c.status = 'completed' THEN c.rows_processed ELSE 0 END), 0) as rows_processed,
                t.total_rows as estimated_total_rows
            FROM migration_tables t
            LEFT JOIN migration_chunks c ON c.table_id = t.id
            WHERE t.job_id = %s
            GROUP BY t.table_name, t.total_rows
            ORDER BY t.table_name
            """,
            (str(job_id),)
        )
        
        tables = cursor.fetchall()
        db.return_connection(conn)
        
        return [
            TableMetrics(
                table_name=t['table_name'],
                total_chunks=t['total_chunks'],
                completed_chunks=t['completed_chunks'],
                failed_chunks=t['failed_chunks'],
                running_chunks=t['running_chunks'],
                pending_chunks=t['pending_chunks'],
                progress_percent=round(
                    (t['completed_chunks'] / t['total_chunks'] * 100) if t['total_chunks'] > 0 else 0,
                    2
                ),
                rows_processed=t['rows_processed'],
                estimated_total_rows=t['estimated_total_rows'] or 0
            )
            for t in tables
        ]
        
    except Exception as e:
        logger.error(f"Failed to get table metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/chunks", response_model=List[ChunkDetail])
async def get_chunk_details(
    job_id: UUID,
    status: Optional[str] = Query(None, description="Filter by status: pending, running, completed, failed"),
    table_name: Optional[str] = Query(None, description="Filter by table name"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    """
    Get detailed chunk information with optional filtering.
    
    Useful for debugging failed chunks or monitoring progress.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Build query with filters
        conditions = ["job_id = %s"]
        params = [str(job_id)]
        
        if status:
            conditions.append("status = %s")
            params.append(status)
        
        if table_name:
            conditions.append("table_name = %s")
            params.append(table_name)
        
        where_clause = " AND ".join(conditions)
        
        cursor.execute(
            f"""
            SELECT 
                id as chunk_id,
                table_name,
                pk_start,
                pk_end,
                status,
                retry_count,
                max_retries,
                worker_id,
                rows_processed,
                source_row_count,
                target_row_count,
                validation_status,
                duration_ms,
                last_error,
                started_at,
                completed_at,
                last_heartbeat,
                next_retry_at
            FROM migration_chunks
            WHERE {where_clause}
            ORDER BY 
                CASE status 
                    WHEN 'failed' THEN 1
                    WHEN 'running' THEN 2
                    WHEN 'pending' THEN 3
                    ELSE 4
                END,
                created_at DESC
            LIMIT %s
            """,
            params + [limit]
        )
        
        chunks = cursor.fetchall()
        db.return_connection(conn)
        
        return [
            ChunkDetail(
                chunk_id=c['chunk_id'],
                table_name=c['table_name'],
                pk_start=c['pk_start'],
                pk_end=c['pk_end'],
                status=c['status'],
                retry_count=c['retry_count'],
                max_retries=c['max_retries'],
                worker_id=c['worker_id'],
                rows_processed=c['rows_processed'],
                source_row_count=c['source_row_count'],
                target_row_count=c['target_row_count'],
                validation_status=c['validation_status'],
                duration_ms=c['duration_ms'],
                last_error=c['last_error'],
                started_at=c['started_at'],
                completed_at=c['completed_at'],
                last_heartbeat=c['last_heartbeat'],
                next_retry_at=c['next_retry_at']
            )
            for c in chunks
        ]
        
    except Exception as e:
        logger.error(f"Failed to get chunk details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chunks/{chunk_id}/execution-log", response_model=List[ExecutionAttempt])
async def get_chunk_execution_log(
    chunk_id: UUID,
    current_user: dict = Depends(get_current_user)
):
    """
    Get execution audit log for a specific chunk.
    
    Shows all retry attempts with detailed metrics.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                id as attempt_id,
                attempt_number,
                worker_id,
                status,
                rows_processed,
                source_row_count,
                target_row_count,
                duration_ms,
                error_message,
                started_at,
                completed_at
            FROM chunk_execution_log
            WHERE chunk_id = %s
            ORDER BY attempt_number DESC
            """,
            (str(chunk_id),)
        )
        
        attempts = cursor.fetchall()
        db.return_connection(conn)
        
        if not attempts:
            raise HTTPException(status_code=404, detail="No execution log found for this chunk")
        
        return [
            ExecutionAttempt(
                attempt_id=a['attempt_id'],
                attempt_number=a['attempt_number'],
                worker_id=a['worker_id'],
                status=a['status'],
                rows_processed=a['rows_processed'],
                source_row_count=a['source_row_count'],
                target_row_count=a['target_row_count'],
                duration_ms=a['duration_ms'],
                error_message=a['error_message'],
                started_at=a['started_at'],
                completed_at=a['completed_at']
            )
            for a in attempts
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get execution log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/health", response_model=SystemHealth)
async def get_system_health():
    """
    Get overall system health status.
    
    Checks:
    - Metadata DB connection
    - Redis connection (when enabled)
    - Active worker count
    - Stale chunks
    - Recovery service status
    """
    try:
        metadata_db_connected = False
        active_workers = 0
        stale_chunks = 0
        recovery_running = False
        
        # Check metadata DB
        try:
            db = get_metadata_db()
            conn = db.get_connection()
            cursor = conn.cursor()
            
            # Simple health check query
            cursor.execute("SELECT 1 as health_check")
            result = cursor.fetchone()
            metadata_db_connected = result['health_check'] == 1
            
            # Count active workers (from heartbeats within last 2 minutes)
            cursor.execute(
                """
                SELECT COUNT(DISTINCT worker_id) as count
                FROM worker_heartbeats
                WHERE last_seen > NOW() - INTERVAL '2 minutes'
                """
            )
            worker_result = cursor.fetchone()
            active_workers = worker_result['count'] if worker_result else 0
            
            # Count stale chunks
            cursor.execute(
                """
                SELECT COUNT(*) as count
                FROM migration_chunks
                WHERE status = 'running'
                AND last_heartbeat < NOW() - INTERVAL '2 minutes'
                """
            )
            stale_result = cursor.fetchone()
            stale_chunks = stale_result['count'] if stale_result else 0
            
            db.return_connection(conn)
            
        except Exception as e:
            logger.error(f"Metadata DB health check failed: {e}")
        
        # Check recovery service
        try:
            from services.api.recovery_service import get_recovery_service
            recovery_service = get_recovery_service()
            recovery_running = recovery_service.running
        except:
            pass
        
        # Overall status
        status = "healthy"
        if not metadata_db_connected:
            status = "critical"
        elif stale_chunks > 0:
            status = "degraded"
        elif not recovery_running:
            status = "warning"
        
        return SystemHealth(
            status=status,
            metadata_db_connected=metadata_db_connected,
            redis_connected=False,  # Redis disabled for now
            active_workers=active_workers,
            stale_chunks_detected=stale_chunks,
            recovery_service_running=recovery_running,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"System health check failed: {e}")
        return SystemHealth(
            status="error",
            metadata_db_connected=False,
            redis_connected=False,
            active_workers=0,
            stale_chunks_detected=0,
            recovery_service_running=False,
            timestamp=datetime.utcnow()
        )
