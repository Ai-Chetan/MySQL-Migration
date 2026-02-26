"""
Analytics and monitoring routes.
"""
from fastapi import APIRouter, HTTPException, Depends
from uuid import UUID
from typing import List
from datetime import datetime, timedelta
from pydantic import BaseModel

from services.api.metadata import get_metadata_db
from services.api.routers.auth import get_current_user, get_current_tenant
from shared.utils import setup_logger

logger = setup_logger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])


# Response Models
class PerformanceMetrics(BaseModel):
    timestamp: datetime
    rows_per_second: float
    chunks_per_minute: float
    active_workers: int
    queue_length: int


class ThroughputStats(BaseModel):
    table_name: str
    total_rows: int
    migrated_rows: int
    rows_per_second: float
    estimated_completion: str


class WorkerStatus(BaseModel):
    worker_id: str
    status: str
    current_chunk: str
    last_heartbeat: datetime
    chunks_completed: int


class UsageStats(BaseModel):
    period: str
    total_migrations: int
    total_rows_migrated: int
    total_chunks_processed: int
    average_throughput: float


@router.get("/performance/realtime", response_model=PerformanceMetrics)
async def get_realtime_performance(
    current_user: dict = Depends(get_current_user),
    current_tenant: dict = Depends(get_current_tenant)
):
    """
    Get real-time performance metrics for current tenant.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get active workers count
        cursor.execute(
            """
            SELECT COUNT(DISTINCT worker_id)
            FROM migration_chunks
            WHERE status = 'running'
            AND last_heartbeat > NOW() - INTERVAL '30 seconds'
            """
        )
        active_workers = cursor.fetchone()[0] or 0
        
        # Calculate rows per second (last 5 minutes)
        cursor.execute(
            """
            SELECT 
                COALESCE(SUM(actual_rows), 0) as total_rows,
                EXTRACT(EPOCH FROM (MAX(completed_at) - MIN(started_at))) as duration
            FROM migration_chunks
            WHERE status = 'completed'
            AND completed_at > NOW() - INTERVAL '5 minutes'
            AND job_id IN (
                SELECT id FROM migration_jobs WHERE tenant_id = %s
            )
            """,
            (str(current_tenant['id']),)
        )
        result = cursor.fetchone()
        total_rows = result[0] or 0
        duration = result[1] or 1
        rows_per_second = total_rows / max(duration, 1)
        
        # Calculate chunks per minute (last 5 minutes)
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM migration_chunks
            WHERE status = 'completed'
            AND completed_at > NOW() - INTERVAL '5 minutes'
            AND job_id IN (
                SELECT id FROM migration_jobs WHERE tenant_id = %s
            )
            """,
            (str(current_tenant['id']),)
        )
        completed_chunks = cursor.fetchone()[0] or 0
        chunks_per_minute = (completed_chunks / 5.0) if completed_chunks > 0 else 0
        
        # Get queue length (Redis)
        from services.api.config import get_redis_client
        redis_client = get_redis_client()
        queue_length = redis_client.llen("migration_queue")
        
        db.return_connection(conn)
        
        return PerformanceMetrics(
            timestamp=datetime.utcnow(),
            rows_per_second=round(rows_per_second, 2),
            chunks_per_minute=round(chunks_per_minute, 2),
            active_workers=active_workers,
            queue_length=queue_length
        )
        
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/throughput/{job_id}", response_model=List[ThroughputStats])
async def get_job_throughput(
    job_id: UUID,
    current_user: dict = Depends(get_current_user)
):
    """
    Get throughput statistics per table for a specific job.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                t.table_name,
                t.total_rows,
                t.migrated_rows,
                COALESCE(
                    t.migrated_rows / NULLIF(
                        EXTRACT(EPOCH FROM (NOW() - t.started_at)), 0
                    ), 
                    0
                ) as rows_per_second,
                CASE 
                    WHEN t.migrated_rows > 0 AND t.migrated_rows < t.total_rows THEN
                        TO_CHAR(
                            NOW() + (
                                (t.total_rows - t.migrated_rows) / NULLIF(
                                    t.migrated_rows / NULLIF(
                                        EXTRACT(EPOCH FROM (NOW() - t.started_at)), 0
                                    ), 0
                                ) * INTERVAL '1 second'
                            ),
                            'YYYY-MM-DD HH24:MI:SS'
                        )
                    ELSE 'N/A'
                END as estimated_completion
            FROM migration_tables t
            WHERE t.job_id = %s
            AND t.status IN ('running', 'completed')
            ORDER BY t.table_name
            """,
            (str(job_id),)
        )
        
        tables = cursor.fetchall()
        db.return_connection(conn)
        
        return [
            ThroughputStats(
                table_name=t[0],
                total_rows=t[1],
                migrated_rows=t[2],
                rows_per_second=round(t[3], 2),
                estimated_completion=t[4]
            )
            for t in tables
        ]
        
    except Exception as e:
        logger.error(f"Failed to get throughput stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workers/active", response_model=List[WorkerStatus])
async def get_active_workers(
    current_user: dict = Depends(get_current_user),
    current_tenant: dict = Depends(get_current_tenant)
):
    """
    Get status of all active workers for current tenant.
    """
    try:
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                c.worker_id,
                c.status,
                COALESCE(t.table_name || ' chunk #' || c.chunk_number::text, 'N/A') as current_chunk,
                c.last_heartbeat,
                (
                    SELECT COUNT(*)
                    FROM migration_chunks c2
                    WHERE c2.worker_id = c.worker_id
                    AND c2.status = 'completed'
                ) as chunks_completed
            FROM migration_chunks c
            LEFT JOIN migration_tables t ON c.table_id = t.id
            WHERE c.status = 'running'
            AND c.last_heartbeat > NOW() - INTERVAL '30 seconds'
            AND c.job_id IN (
                SELECT id FROM migration_jobs WHERE tenant_id = %s
            )
            GROUP BY c.worker_id, c.status, t.table_name, c.chunk_number, c.last_heartbeat
            ORDER BY c.last_heartbeat DESC
            """,
            (str(current_tenant['id']),)
        )
        
        workers = cursor.fetchall()
        db.return_connection(conn)
        
        return [
            WorkerStatus(
                worker_id=w[0],
                status=w[1],
                current_chunk=w[2],
                last_heartbeat=w[3],
                chunks_completed=w[4]
            )
            for w in workers
        ]
        
    except Exception as e:
        logger.error(f"Failed to get worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage", response_model=UsageStats)
async def get_usage_stats(
    period: str = "month",
    current_user: dict = Depends(get_current_user),
    current_tenant: dict = Depends(get_current_tenant)
):
    """
    Get usage statistics for billing.
    Period: 'day', 'week', 'month'
    """
    try:
        # Calculate time range
        if period == "day":
            start_time = datetime.utcnow() - timedelta(days=1)
        elif period == "week":
            start_time = datetime.utcnow() - timedelta(weeks=1)
        else:  # month
            start_time = datetime.utcnow() - timedelta(days=30)
        
        db = get_metadata_db()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                COUNT(DISTINCT j.id) as total_migrations,
                COALESCE(SUM(j.completed_chunks), 0) as total_chunks,
                COALESCE(SUM(
                    (SELECT SUM(actual_rows) 
                     FROM migration_chunks 
                     WHERE job_id = j.id AND status = 'completed')
                ), 0) as total_rows
            FROM migration_jobs j
            WHERE j.tenant_id = %s
            AND j.created_at >= %s
            """,
            (str(current_tenant['id']), start_time)
        )
        
        result = cursor.fetchone()
        db.return_connection(conn)
        
        total_migrations = result[0] or 0
        total_chunks = result[1] or 0
        total_rows = result[2] or 0
        
        # Calculate average throughput
        duration_hours = (datetime.utcnow() - start_time).total_seconds() / 3600
        avg_throughput = total_rows / max(duration_hours, 1)
        
        return UsageStats(
            period=period,
            total_migrations=total_migrations,
            total_rows_migrated=total_rows,
            total_chunks_processed=total_chunks,
            average_throughput=round(avg_throughput, 2)
        )
        
    except Exception as e:
        logger.error(f"Failed to get usage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
