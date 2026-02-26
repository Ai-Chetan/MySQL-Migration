"""
FastAPI application for Migration Platform Control Plane.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime

from services.api.routers import migrations, auth, analytics, schema_migration
from services.api.metadata import get_metadata_db
# from services.api.config import get_redis_client, close_redis_client  # COMMENTED OUT - Redis will be added later
from services.api.schemas import HealthResponse, MetricsResponse
from shared.utils import setup_logger

logger = setup_logger(__name__, level="INFO")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    
    Manages startup and shutdown of database connections and resources.
    """
    # Startup
    logger.info("Starting Migration Platform API...")
    
    try:
        # Initialize metadata database
        metadata_db = get_metadata_db()
        metadata_db.initialize_schema("schema.sql")
        logger.info("Metadata database initialized")
        
        # Test Redis connection (COMMENTED OUT - TO BE ADDED LATER)
        # redis_client = get_redis_client()
        # redis_client.ping()
        # logger.info("Redis connection established")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Migration Platform API...")
    
    try:
        metadata_db = get_metadata_db()
        metadata_db.close()
        
        # Close Redis connection (COMMENTED OUT - TO BE ADDED LATER)
        # close_redis_client()
        close_redis_client()
        logger.info("Connections closed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


# Create FastAPI application
app = FastAPI(
    title="Migration Platform API",
    description="Control Plane for Database Migration Platform",
    version="1.0.0 (Phase 1)",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(migrations.router)
app.include_router(analytics.router)
app.include_router(schema_migration.router)


@app.get("/", tags=["root"])
async def root():
    """Root endpoint."""
    return {
        "service": "Migration Platform API",
        "version": "1.0.0",
        "phase": "Phase 1",
        "status": "operational"
    }


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """
    Health check endpoint.
    
    Checks connectivity to metadata database and Redis.
    """
    metadata_db_healthy = False
    redis_healthy = False
    
    try:
        metadata_db = get_metadata_db()
        metadata_db_healthy = metadata_db.health_check()
    except Exception as e:
        logger.error(f"Metadata DB health check failed: {e}")
    
    try:
        redis_client = get_redis_client()
        redis_client.ping()
        redis_healthy = True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
    
    overall_status = "healthy" if (metadata_db_healthy and redis_healthy) else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        timestamp=datetime.utcnow(),
        metadata_db=metadata_db_healthy,
        redis=redis_healthy
    )


@app.get("/metrics", response_model=MetricsResponse, tags=["metrics"])
async def get_metrics():
    """
    Get system-wide metrics.
    
    Returns statistics about jobs, chunks, and throughput.
    """
    try:
        metadata_db = get_metadata_db()
        conn = metadata_db.get_connection()
        cursor = conn.cursor()
        
        # Total jobs by status
        cursor.execute(
            """
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(*) FILTER (WHERE status IN ('running', 'planning')) as active_jobs,
                COUNT(*) FILTER (WHERE status = 'completed') as completed_jobs,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_jobs
            FROM migration_jobs
            """
        )
        job_stats = cursor.fetchone()
        
        # Total chunks processed
        cursor.execute(
            """
            SELECT 
                COUNT(*) FILTER (WHERE status = 'completed') as completed_chunks,
                SUM(rows_processed) as total_rows
            FROM migration_chunks
            """
        )
        chunk_stats = cursor.fetchone()
        
        # Average throughput (rows per second)
        cursor.execute(
            """
            SELECT 
                AVG(rows_processed::DECIMAL / (duration_ms / 1000.0)) as avg_throughput
            FROM migration_chunks
            WHERE status = 'completed' AND duration_ms > 0
            """
        )
        throughput_result = cursor.fetchone()
        avg_throughput = throughput_result[0] if throughput_result[0] else None
        
        # Active workers (from heartbeats)
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM worker_heartbeats
            WHERE last_seen > NOW() - INTERVAL '2 minutes'
            """
        )
        active_workers = cursor.fetchone()[0]
        
        metadata_db.return_connection(conn)
        
        return MetricsResponse(
            total_jobs=job_stats[0] or 0,
            active_jobs=job_stats[1] or 0,
            completed_jobs=job_stats[2] or 0,
            failed_jobs=job_stats[3] or 0,
            total_chunks_processed=chunk_stats[0] or 0,
            total_rows_migrated=int(chunk_stats[1] or 0),
            average_throughput=round(float(avg_throughput), 2) if avg_throughput else None,
            active_workers=active_workers
        )
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise


if __name__ == "__main__":
    import uvicorn
    from services.api.config import API_HOST, API_PORT
    
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
        log_level="info"
    )
