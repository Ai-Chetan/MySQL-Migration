"""
Performance Monitoring API Endpoints
Real-time performance metrics for migrations
"""
from typing import Dict, List, Any
from uuid import UUID
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from services.api.auth import get_current_user
from services.worker.db import MetadataConnection

router = APIRouter(
    prefix="/performance",
    tags=["Performance Metrics"]
)


class RealtimeMetrics(BaseModel):
    """Real-time performance metrics."""
    job_id: UUID
    rows_per_second: float
    mb_per_second: float
    memory_usage_mb: int
    avg_insert_latency_ms: int
    active_workers: int
    current_batch_size: int
    throughput_trend: str
    estimated_completion: str


class WorkerStats(BaseModel):
    """Per-worker statistics."""
    worker_id: str
    rows_processed: int
    throughput_rows_per_sec: float
    throughput_mb_per_sec: float
    memory_peak_mb: int
    avg_latency_ms: int
    last_update: datetime


class PerformanceHistory(BaseModel):
    """Historical performance data."""
    timestamp: datetime
    rows_per_second: float
    mb_per_second: float
    memory_usage_mb: int
    insert_latency_ms: int


@router.get("/realtime/{job_id}", response_model=RealtimeMetrics)
async def get_realtime_metrics(
    job_id: UUID,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get real-time performance metrics for a job.
    
    Returns current throughput, memory usage, and ETA.
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        # Use the realtime_performance view
        cursor.execute(
            """
            SELECT 
                job_id,
                rows_per_second,
                mb_per_second,
                memory_usage_mb,
                avg_insert_latency_ms,
                active_workers,
                current_batch_size,
                throughput_trend,
                estimated_completion
            FROM realtime_performance
            WHERE job_id = %s
            """,
            (str(job_id),)
        )
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="No performance data found")
        
        return RealtimeMetrics(
            job_id=result['job_id'],
            rows_per_second=float(result['rows_per_second'] or 0),
            mb_per_second=float(result['mb_per_second'] or 0),
            memory_usage_mb=result['memory_usage_mb'] or 0,
            avg_insert_latency_ms=result['avg_insert_latency_ms'] or 0,
            active_workers=result['active_workers'] or 0,
            current_batch_size=result['current_batch_size'] or 5000,
            throughput_trend=result['throughput_trend'] or 'stable',
            estimated_completion=result['estimated_completion'] or 'calculating...'
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metrics: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/history/{job_id}", response_model=List[PerformanceHistory])
async def get_performance_history(
    job_id: UUID,
    hours: int = 1,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get historical performance data for time-series visualization.
    
    Args:
        job_id: Migration job ID
        hours: How many hours of history to retrieve (default 1)
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        since = datetime.now() - timedelta(hours=hours)
        
        cursor.execute(
            """
            SELECT
                timestamp,
                rows_per_second,
                mb_per_second,
                memory_usage_mb,
                insert_latency_ms
            FROM performance_metrics
            WHERE job_id = %s
            AND timestamp >= %s
            ORDER BY timestamp ASC
            """,
            (str(job_id), since)
        )
        
        results = cursor.fetchall()
        
        return [
            PerformanceHistory(
                timestamp=row['timestamp'],
                rows_per_second=float(row['rows_per_second'] or 0),
                mb_per_second=float(row['mb_per_second'] or 0),
                memory_usage_mb=row['memory_usage_mb'] or 0,
                insert_latency_ms=row['insert_latency_ms'] or 0
            )
            for row in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching history: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/workers/{job_id}", response_model=List[WorkerStats])
async def get_worker_stats(
    job_id: UUID,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get per-worker performance statistics.
    
    Shows throughput and resource usage for each worker.
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        cursor.execute(
            """
            SELECT
                mc.worker_id,
                SUM(mc.rows_processed) as rows_processed,
                AVG(mc.throughput_rows_per_sec) as avg_throughput_rows,
                AVG(mc.throughput_mb_per_sec) as avg_throughput_mb,
                MAX(mc.memory_peak_mb) as memory_peak,
                AVG(mc.insert_latency_ms) as avg_latency,
                MAX(mc.last_heartbeat) as last_update
            FROM migration_chunks mc
            WHERE mc.job_id = %s
            AND mc.worker_id IS NOT NULL
            GROUP BY mc.worker_id
            ORDER BY rows_processed DESC
            """,
            (str(job_id),)
        )
        
        results = cursor.fetchall()
        
        return [
            WorkerStats(
                worker_id=row['worker_id'],
                rows_processed=row['rows_processed'] or 0,
                throughput_rows_per_sec=float(row['avg_throughput_rows'] or 0),
                throughput_mb_per_sec=float(row['avg_throughput_mb'] or 0),
                memory_peak_mb=row['memory_peak'] or 0,
                avg_latency_ms=int(row['avg_latency'] or 0),
                last_update=row['last_update']
            )
            for row in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching worker stats: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/batch-size-history/{job_id}")
async def get_batch_size_history(
    job_id: UUID,
    table_name: str = None,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get adaptive batch size adjustment history.
    
    Shows how batch sizes changed over time and reasons for adjustments.
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        query = """
            SELECT
                timestamp,
                table_name,
                old_batch_size,
                new_batch_size,
                reason,
                avg_latency_ms
            FROM batch_size_history
            WHERE job_id = %s
        """
        params = [str(job_id)]
        
        if table_name:
            query += " AND table_name = %s"
            params.append(table_name)
        
        query += " ORDER BY timestamp DESC LIMIT 100"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return [
            {
                "timestamp": row['timestamp'].isoformat(),
                "table_name": row['table_name'],
                "old_batch_size": row['old_batch_size'],
                "new_batch_size": row['new_batch_size'],
                "reason": row['reason'],
                "avg_latency_ms": row['avg_latency_ms']
            }
            for row in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching batch history: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/constraints/{job_id}")
async def get_constraint_status(
    job_id: UUID,
    current_user: Dict = Depends(get_current_user)
):
    """
    Get constraint management status.
    
    Shows which constraints were dropped and restored.
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        cursor.execute(
            """
            SELECT
                table_name,
                constraint_type,
                constraint_name,
                dropped_at,
                restored_at
            FROM table_constraints_backup
            WHERE job_id = %s
            ORDER BY table_name, constraint_type, dropped_at
            """,
            (str(job_id),)
        )
        
        results = cursor.fetchall()
        
        return [
            {
                "table_name": row['table_name'],
                "constraint_type": row['constraint_type'],
                "constraint_name": row['constraint_name'],
                "dropped_at": row['dropped_at'].isoformat() if row['dropped_at'] else None,
                "restored_at": row['restored_at'].isoformat() if row['restored_at'] else None,
                "status": "restored" if row['restored_at'] else "dropped"
            }
            for row in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching constraint status: {str(e)}")
    finally:
        metadata_conn.close()
