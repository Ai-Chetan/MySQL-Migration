"""Pydantic schemas for API request/response models."""
from typing import Optional, List
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field

# Re-export shared models for API use
from shared.models import (
    DatabaseConfig,
    CreateMigrationRequest,
    MigrationJobSummary,
    TableProgress,
    ChunkInfo,
    JobDetailResponse,
    MetricsResponse,
    JobStatus,
    TableStatus,
    ChunkStatus
)

__all__ = [
    'DatabaseConfig',
    'CreateMigrationRequest',
    'MigrationJobSummary',
    'TableProgress',
    'ChunkInfo',
    'JobDetailResponse',
    'MetricsResponse',
    'JobStatus',
    'TableStatus',
    'ChunkStatus',
    'CreateMigrationResponse',
    'ResumeJobResponse',
    'HealthResponse',
    'TableDetailResponse',
    'ChunkDetailResponse'
]


class CreateMigrationResponse(BaseModel):
    """Response after creating a migration job."""
    job_id: UUID
    message: str
    total_tables: int
    total_chunks: int


class ResumeJobResponse(BaseModel):
    """Response after resuming a migration job."""
    job_id: UUID
    message: str
    chunks_requeued: int


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    metadata_db: bool
    redis: bool


class TableDetailResponse(BaseModel):
    """Detailed table information."""
    id: UUID
    job_id: UUID
    table_name: str
    status: str
    total_rows: int
    migrated_rows: int
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    primary_key_column: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class ChunkDetailResponse(BaseModel):
    """Detailed chunk information."""
    id: UUID
    job_id: UUID
    table_id: UUID
    table_name: str
    chunk_number: int
    start_pk: int
    end_pk: int
    status: str
    estimated_rows: int
    actual_rows: Optional[int] = None
    retry_count: int
    worker_id: Optional[str] = None
    last_heartbeat: Optional[datetime] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
