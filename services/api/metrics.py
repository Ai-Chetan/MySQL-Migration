"""
Prometheus Metrics for Observability
Tracks migration performance, resource usage, and system health
"""
import os
import time
import logging
from typing import Dict, Any, Optional
from functools import wraps
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    CollectorRegistry, generate_latest,
    CONTENT_TYPE_LATEST
)

logger = logging.getLogger(__name__)

# Create custom registry
REGISTRY = CollectorRegistry()

# ==========================================
# Migration Metrics
# ==========================================

# Job Metrics
job_created_total = Counter(
    'migration_job_created_total',
    'Total migration jobs created',
    ['tenant_id', 'source_db', 'target_db'],
    registry=REGISTRY
)

job_completed_total = Counter(
    'migration_job_completed_total',
    'Total migration jobs completed',
    ['tenant_id', 'status'],
    registry=REGISTRY
)

job_duration_seconds = Histogram(
    'migration_job_duration_seconds',
    'Migration job duration in seconds',
    ['tenant_id', 'job_id'],
    buckets=[60, 300, 600, 1800, 3600, 7200, 14400, 28800],  # 1m to 8h
    registry=REGISTRY
)

# Chunk Metrics
chunk_processed_total = Counter(
    'migration_chunk_processed_total',
    'Total chunks processed',
    ['tenant_id', 'job_id', 'status'],
    registry=REGISTRY
)

chunk_duration_seconds = Histogram(
    'migration_chunk_duration_seconds',
    'Chunk processing duration',
    ['tenant_id'],
    buckets=[1, 5, 10, 30, 60, 120, 300],  # 1s to 5m
    registry=REGISTRY
)

chunk_rows_processed = Counter(
    'migration_chunk_rows_processed_total',
    'Total rows processed across all chunks',
    ['tenant_id', 'job_id'],
    registry=REGISTRY
)

chunk_bytes_processed = Counter(
    'migration_chunk_bytes_processed_total',
    'Total bytes processed',
    ['tenant_id', 'job_id'],
    registry=REGISTRY
)

# Performance Metrics
rows_per_second = Gauge(
    'migration_rows_per_second',
    'Current rows per second throughput',
    ['tenant_id', 'job_id'],
    registry=REGISTRY
)

mbps_throughput = Gauge(
    'migration_mbps_throughput',
    'Current MB/s throughput',
    ['tenant_id', 'job_id'],
    registry=REGISTRY
)

# Error Metrics
chunk_errors_total = Counter(
    'migration_chunk_errors_total',
    'Total chunk errors',
    ['tenant_id', 'job_id', 'error_type'],
    registry=REGISTRY
)

chunk_retries_total = Counter(
    'migration_chunk_retries_total',
    'Total chunk retries',
    ['tenant_id', 'job_id'],
    registry=REGISTRY
)

# ==========================================
# Worker Metrics
# ==========================================

active_workers = Gauge(
    'migration_active_workers',
    'Number of active worker pods',
    registry=REGISTRY
)

worker_tasks_processing = Gauge(
    'migration_worker_tasks_processing',
    'Number of tasks currently being processed',
    ['worker_id'],
    registry=REGISTRY
)

worker_task_duration_seconds = Summary(
    'migration_worker_task_duration_seconds',
    'Worker task processing time',
    ['worker_id', 'task_type'],
    registry=REGISTRY
)

# ==========================================
# Kafka Metrics
# ==========================================

kafka_messages_produced_total = Counter(
    'migration_kafka_messages_produced_total',
    'Total Kafka messages produced',
    ['topic'],
    registry=REGISTRY
)

kafka_messages_consumed_total = Counter(
    'migration_kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['topic', 'consumer_group'],
    registry=REGISTRY
)

kafka_consumer_lag = Gauge(
    'migration_kafka_consumer_lag',
    'Kafka consumer lag',
    ['topic', 'partition', 'consumer_group'],
    registry=REGISTRY
)

kafka_message_processing_duration = Histogram(
    'migration_kafka_message_processing_duration_seconds',
    'Kafka message processing duration',
    ['topic'],
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60],
    registry=REGISTRY
)

# ==========================================
# API Metrics
# ==========================================

api_requests_total = Counter(
    'migration_api_requests_total',
    'Total API requests',
    ['method', 'endpoint', 'status'],
    registry=REGISTRY
)

api_request_duration_seconds = Histogram(
    'migration_api_request_duration_seconds',
    'API request duration',
    ['method', 'endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10],
    registry=REGISTRY
)

api_active_requests = Gauge(
    'migration_api_active_requests',
    'Number of active API requests',
    registry=REGISTRY
)

# ==========================================
# Database Metrics
# ==========================================

db_connections_active = Gauge(
    'migration_db_connections_active',
    'Active database connections',
    ['db_type', 'db_host'],
    registry=REGISTRY
)

db_query_duration_seconds = Histogram(
    'migration_db_query_duration_seconds',
    'Database query duration',
    ['db_type', 'operation'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10],
    registry=REGISTRY
)

db_errors_total = Counter(
    'migration_db_errors_total',
    'Total database errors',
    ['db_type', 'error_type'],
    registry=REGISTRY
)

# ==========================================
# Usage & Billing Metrics
# ==========================================

tenant_data_migrated_gb = Counter(
    'migration_tenant_data_migrated_gb_total',
    'Total GB migrated per tenant',
    ['tenant_id'],
    registry=REGISTRY
)

tenant_rows_migrated = Counter(
    'migration_tenant_rows_migrated_total',
    'Total rows migrated per tenant',
    ['tenant_id'],
    registry=REGISTRY
)

tenant_api_calls_total = Counter(
    'migration_tenant_api_calls_total',
    'Total API calls per tenant',
    ['tenant_id', 'endpoint'],
    registry=REGISTRY
)

tenant_compute_hours = Counter(
    'migration_tenant_compute_hours_total',
    'Total compute hours per tenant',
    ['tenant_id'],
    registry=REGISTRY
)

# ==========================================
# Rate Limiting Metrics
# ==========================================

rate_limit_exceeded_total = Counter(
    'migration_rate_limit_exceeded_total',
    'Total rate limit violations',
    ['tenant_id', 'endpoint'],
    registry=REGISTRY
)

rate_limit_current_usage = Gauge(
    'migration_rate_limit_current_usage',
    'Current rate limit usage',
    ['tenant_id'],
    registry=REGISTRY
)

# ==========================================
# Object Storage Metrics
# ==========================================

s3_operations_total = Counter(
    'migration_s3_operations_total',
    'Total S3 operations',
    ['operation', 'status'],
    registry=REGISTRY
)

s3_bytes_uploaded = Counter(
    'migration_s3_bytes_uploaded_total',
    'Total bytes uploaded to S3',
    registry=REGISTRY
)

s3_bytes_downloaded = Counter(
    'migration_s3_bytes_downloaded_total',
    'Total bytes downloaded from S3',
    registry=REGISTRY
)


# ==========================================
# Metric Helpers
# ==========================================

def track_job_duration(tenant_id: str, job_id: str):
    """Decorator to track job duration."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                job_duration_seconds.labels(tenant_id=tenant_id, job_id=job_id).observe(duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                job_duration_seconds.labels(tenant_id=tenant_id, job_id=job_id).observe(duration)
                raise
        return wrapper
    return decorator


def track_api_request(method: str, endpoint: str):
    """Decorator to track API request metrics."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            api_active_requests.inc()
            start_time = time.time()
            status = "success"
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                status = "error"
                raise
            finally:
                duration = time.time() - start_time
                api_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)
                api_requests_total.labels(method=method, endpoint=endpoint, status=status).inc()
                api_active_requests.dec()
        
        return wrapper
    return decorator


def get_metrics() -> bytes:
    """Get Prometheus metrics in text format."""
    return generate_latest(REGISTRY)


def get_metrics_content_type() -> str:
    """Get Prometheus metrics content type."""
    return CONTENT_TYPE_LATEST


class MetricsCollector:
    """Helper class for collecting custom metrics."""
    
    @staticmethod
    def record_job_created(tenant_id: str, source_db: str, target_db: str):
        """Record job creation."""
        job_created_total.labels(
            tenant_id=tenant_id,
            source_db=source_db,
            target_db=target_db
        ).inc()
    
    @staticmethod
    def record_job_completed(tenant_id: str, status: str):
        """Record job completion."""
        job_completed_total.labels(
            tenant_id=tenant_id,
            status=status
        ).inc()
    
    @staticmethod
    def record_chunk_processed(tenant_id: str, job_id: str, status: str, rows: int, bytes_size: int):
        """Record chunk processing."""
        chunk_processed_total.labels(
            tenant_id=tenant_id,
            job_id=job_id,
            status=status
        ).inc()
        
        if status == "completed":
            chunk_rows_processed.labels(tenant_id=tenant_id, job_id=job_id).inc(rows)
            chunk_bytes_processed.labels(tenant_id=tenant_id, job_id=job_id).inc(bytes_size)
    
    @staticmethod
    def record_error(tenant_id: str, job_id: str, error_type: str):
        """Record error."""
        chunk_errors_total.labels(
            tenant_id=tenant_id,
            job_id=job_id,
            error_type=error_type
        ).inc()
    
    @staticmethod
    def update_throughput(tenant_id: str, job_id: str, rows_per_sec: float, mbps: float):
        """Update throughput metrics."""
        rows_per_second.labels(tenant_id=tenant_id, job_id=job_id).set(rows_per_sec)
        mbps_throughput.labels(tenant_id=tenant_id, job_id=job_id).set(mbps)
    
    @staticmethod
    def record_kafka_message(topic: str, operation: str):
        """Record Kafka message."""
        if operation == "produce":
            kafka_messages_produced_total.labels(topic=topic).inc()
        elif operation == "consume":
            kafka_messages_consumed_total.labels(topic=topic, consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "migration-workers")).inc()
    
    @staticmethod
    def record_s3_operation(operation: str, status: str, bytes_size: int = 0):
        """Record S3 operation."""
        s3_operations_total.labels(operation=operation, status=status).inc()
        
        if operation == "upload" and status == "success":
            s3_bytes_uploaded.inc(bytes_size)
        elif operation == "download" and status == "success":
            s3_bytes_downloaded.inc(bytes_size)


logger.info("Prometheus metrics initialized")
