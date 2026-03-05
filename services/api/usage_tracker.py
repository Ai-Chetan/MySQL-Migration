"""
Usage Tracking Service
Records billable events for tenant usage metering
"""
import json
from typing import Dict, Any, Optional
from uuid import UUID
from datetime import datetime
from decimal import Decimal

from services.worker.db import MetadataConnection
from services.api.metadata import get_metadata_db
from shared.utils import setup_logger

logger = setup_logger(__name__)


class UsageTracker:
    """Track and record usage events for billing."""
    
    def __init__(self, metadata_conn: MetadataConnection):
        self.metadata_conn = metadata_conn
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "level": level.upper(),
            "service": "usage_tracker",
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
    
    def record_event(
        self,
        tenant_id: UUID,
        event_type: str,
        metric_name: str,
        metric_value: float,
        unit: str = None,
        job_id: UUID = None,
        metadata: Dict[str, Any] = None
    ) -> UUID:
        """
        Record a usage event.
        
        Args:
            tenant_id: Tenant ID
            event_type: Type of event ('data_migrated', 'job_created', 'compute_time', 'api_call')
            metric_name: Name of metric ('gb_migrated', 'rows_processed', 'compute_hours')
            metric_value: Numeric value of the metric
            unit: Unit of measurement ('gb', 'rows', 'hours', 'count')
            job_id: Associated job ID (optional)
            metadata: Additional context (optional)
            
        Returns:
            Event ID
        """
        cursor = self.metadata_conn.get_cursor()
        
        try:
            cursor.execute(
                """
                INSERT INTO usage_events 
                (tenant_id, job_id, event_type, metric_name, metric_value, unit, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    str(tenant_id),
                    str(job_id) if job_id else None,
                    event_type,
                    metric_name,
                    metric_value,
                    unit,
                    json.dumps(metadata or {})
                )
            )
            
            result = cursor.fetchone()
            event_id = result['id']
            
            self.metadata_conn.commit()
            
            self._log_structured(
                "info",
                "Usage event recorded",
                tenant_id=str(tenant_id),
                event_type=event_type,
                metric_name=metric_name,
                metric_value=metric_value
            )
            
            return event_id
            
        except Exception as e:
            self.metadata_conn.rollback()
            self._log_structured(
                "error",
                "Failed to record usage event",
                tenant_id=str(tenant_id),
                error=str(e)
            )
            raise
    
    def record_data_migration(
        self,
        tenant_id: UUID,
        job_id: UUID,
        rows_processed: int,
        bytes_processed: int
    ):
        """Record data migration usage."""
        gb_migrated = bytes_processed / (1024 ** 3)  # Convert to GB
        
        # Record GB migrated
        self.record_event(
            tenant_id=tenant_id,
            job_id=job_id,
            event_type='data_migrated',
            metric_name='gb_migrated',
            metric_value=gb_migrated,
            unit='gb',
            metadata={'rows': rows_processed, 'bytes': bytes_processed}
        )
        
        # Record rows processed
        self.record_event(
            tenant_id=tenant_id,
            job_id=job_id,
            event_type='data_migrated',
            metric_name='rows_processed',
            metric_value=rows_processed,
            unit='rows'
        )
    
    def record_compute_time(
        self,
        tenant_id: UUID,
        job_id: UUID,
        duration_seconds: float
    ):
        """Record compute time usage."""
        hours = duration_seconds / 3600
        
        self.record_event(
            tenant_id=tenant_id,
            job_id=job_id,
            event_type='compute_time',
            metric_name='compute_hours',
            metric_value=hours,
            unit='hours',
            metadata={'duration_seconds': duration_seconds}
        )
    
    def record_api_call(
        self,
        tenant_id: UUID,
        endpoint: str,
        method: str,
        status_code: int
    ):
        """Record API call."""
        self.record_event(
            tenant_id=tenant_id,
            event_type='api_call',
            metric_name='api_requests',
            metric_value=1,
            unit='count',
            metadata={
                'endpoint': endpoint,
                'method': method,
                'status_code': status_code
            }
        )
    
    def get_tenant_usage(
        self,
        tenant_id: UUID,
        start_date: datetime = None,
        end_date: datetime = None
    ) -> Dict[str, Any]:
        """
        Get aggregated usage for a tenant.
        
        Returns:
            Dictionary with usage metrics
        """
        cursor = self.metadata_conn.get_cursor()
        
        query = """
            SELECT 
                metric_name,
                SUM(metric_value) as total_value,
                unit,
                COUNT(*) as event_count
            FROM usage_events
            WHERE tenant_id = %s
        """
        params = [str(tenant_id)]
        
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND timestamp < %s"
            params.append(end_date)
        
        query += " GROUP BY metric_name, unit"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        usage = {}
        for row in results:
            usage[row['metric_name']] = {
                'value': float(row['total_value']),
                'unit': row['unit'],
                'count': row['event_count']
            }
        
        return usage
    
    def get_current_month_usage(self, tenant_id: UUID) -> Dict[str, Any]:
        """Get usage for current billing month."""
        cursor = self.metadata_conn.get_cursor()
        
        cursor.execute(
            """
            SELECT * FROM tenant_usage_current_month
            WHERE tenant_id = %s
            """,
            (str(tenant_id),)
        )
        
        result = cursor.fetchone()
        
        if not result:
            return {
                'gb_migrated': 0,
                'rows_processed': 0,
                'jobs_created': 0,
                'compute_hours': 0,
                'usage_percentage': 0
            }
        
        return {
            'gb_migrated': float(result['total_gb_migrated'] or 0),
            'rows_processed': int(result['total_rows_processed'] or 0),
            'jobs_created': result['total_jobs_created'] or 0,
            'compute_hours': float(result['total_compute_hours'] or 0),
            'plan_limit_gb': result['plan_limit_gb'],
            'usage_percentage': float(result['usage_percentage'] or 0)
        }
    
    def check_quota_exceeded(self, tenant_id: UUID) -> Dict[str, Any]:
        """
        Check if tenant has exceeded any quotas.
        
        Returns:
            Dictionary with quota status
        """
        cursor = self.metadata_conn.get_cursor()
        
        cursor.execute(
            """
            SELECT * FROM tenant_current_usage
            WHERE tenant_id = %s
            """,
            (str(tenant_id),)
        )
        
        result = cursor.fetchone()
        
        if not result:
            return {
                'exceeded': False,
                'warnings': []
            }
        
        warnings = []
        exceeded = False
        
        # Check GB limit
        if result['gb_used_this_month'] >= result['max_gb_per_month']:
            warnings.append(f"Monthly data transfer limit exceeded ({result['gb_used_this_month']:.2f} GB / {result['max_gb_per_month']} GB)")
            exceeded = True
        elif result['gb_used_this_month'] >= result['max_gb_per_month'] * 0.8:
            warnings.append(f"Approaching monthly data limit ({result['gb_used_this_month']:.2f} GB / {result['max_gb_per_month']} GB)")
        
        # Check concurrent jobs
        if result['active_jobs'] >= result['max_concurrent_jobs']:
            warnings.append(f"Concurrent job limit reached ({result['active_jobs']} / {result['max_concurrent_jobs']})")
            exceeded = True
        
        return {
            'exceeded': exceeded,
            'warnings': warnings,
            'usage': {
                'gb_used': float(result['gb_used_this_month']),
                'gb_limit': result['max_gb_per_month'],
                'active_jobs': result['active_jobs'],
                'max_jobs': result['max_concurrent_jobs']
            }
        }


def get_usage_tracker() -> UsageTracker:
    """Get usage tracker instance."""
    db = get_metadata_db()
    metadata_conn = MetadataConnection(
        host=db.host,
        port=db.port,
        database=db.database,
        user=db.user,
        password=db.password
    )
    return UsageTracker(metadata_conn)
