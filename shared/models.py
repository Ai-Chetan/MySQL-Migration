"""
Shared data models for the migration platform.
These models are used across API and Worker services.
"""
from enum import Enum
from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Migration job status enumeration."""
    PENDING = "pending"
    PLANNING = "planning"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class ChunkStatus(str, Enum):
    """Migration chunk status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TableStatus(str, Enum):
    """Migration table status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# ===== Database Connection Models =====

class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    host: str
    port: int = 3306
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


# ===== Request/Response Models =====

class CreateMigrationRequest(BaseModel):
    """Request model for creating a new migration job."""
    source_config: DatabaseConfig
    target_config: DatabaseConfig
    tenant_id: str = "local"
    chunk_size: int = Field(default=100000, ge=1000, le=1000000)
    batch_size: int = Field(default=1000, ge=100, le=10000)


class MigrationJobSummary(BaseModel):
    """Summary information about a migration job."""
    id: UUID
    status: JobStatus
    tenant_id: str
    total_tables: int
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    progress_percentage: float
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_error: Optional[str] = None


class TableProgress(BaseModel):
    """Progress information for a single table."""
    id: UUID
    table_name: str
    total_rows: Optional[int]
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    status: TableStatus
    progress_percentage: float


class ChunkInfo(BaseModel):
    """Information about a single chunk."""
    id: UUID
    table_name: str
    pk_start: int
    pk_end: int
    status: ChunkStatus
    retry_count: int
    rows_processed: Optional[int]
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_error: Optional[str] = None


class JobDetailResponse(BaseModel):
    """Detailed response for a migration job."""
    job: MigrationJobSummary
    tables: List[TableProgress]
    failed_chunks: List[ChunkInfo]
    estimated_completion_time: Optional[datetime] = None
    throughput_rows_per_second: Optional[float] = None


class MetricsResponse(BaseModel):
    """System-wide metrics."""
    total_jobs: int
    active_jobs: int
    completed_jobs: int
    failed_jobs: int
    total_chunks_processed: int
    total_rows_migrated: int
    average_throughput: Optional[float] = None
    active_workers: int


# ===== Internal Models (used within services) =====

class ChunkTask(BaseModel):
    """Task model pushed to Redis queue."""
    chunk_id: UUID
    job_id: UUID
    table_name: str
    pk_start: int
    pk_end: int
    source_config: DatabaseConfig
    target_config: DatabaseConfig
    batch_size: int = 1000


class TableMetadata(BaseModel):
    """Metadata about a source table."""
    table_name: str
    primary_key_column: str
    total_rows: int
    min_pk: int
    max_pk: int
    estimated_chunks: int


class ChunkRange(BaseModel):
    """Represents a chunk range for processing."""
    pk_start: int
    pk_end: int
    estimated_rows: int
