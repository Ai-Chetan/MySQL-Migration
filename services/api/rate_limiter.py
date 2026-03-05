"""
Rate Limiting Middleware
Per-tenant API rate limiting based on subscription plan
"""
import json
from typing import Callable
from datetime import datetime, timedelta
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from uuid import UUID

from services.worker.db import MetadataConnection
from services.api.metadata import get_metadata_db
from shared.utils import setup_logger

logger = setup_logger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware based on tenant plans."""
    
    async def dispatch(self, request: Request, call_next: Callable):
        """Process request with rate limiting."""
        
        # Skip rate limiting for health and auth endpoints
        if request.url.path in ["/health", "/api/auth/login", "/api/auth/signup"]:
            return await call_next(request)
        
        # Get tenant_id from request (set by auth middleware)
        tenant_id = request.state.tenant_id if hasattr(request.state, 'tenant_id') else None
        
        if not tenant_id:
            # No tenant context - skip rate limiting
            return await call_next(request)
        
        try:
            # Check rate limit
            allowed, remaining, reset_at = self._check_rate_limit(
                tenant_id=UUID(tenant_id),
                endpoint=request.url.path,
                method=request.method
            )
            
            if not allowed:
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "detail": "Rate limit exceeded",
                        "retry_after": int((reset_at - datetime.now()).total_seconds())
                    },
                    headers={
                        "X-RateLimit-Limit": str(self._get_tenant_limit(UUID(tenant_id))),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(reset_at.timestamp())),
                        "Retry-After": str(int((reset_at - datetime.now()).total_seconds()))
                    }
                )
            
            # Process request
            response = await call_next(request)
            
            # Add rate limit headers
            response.headers["X-RateLimit-Limit"] = str(self._get_tenant_limit(UUID(tenant_id)))
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(int(reset_at.timestamp()))
            
            return response
            
        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            # On error, allow request to proceed
            return await call_next(request)
    
    def _get_tenant_limit(self, tenant_id: UUID) -> int:
        """Get rate limit for tenant based on their plan."""
        db = get_metadata_db()
        metadata_conn = MetadataConnection(
            host=db.host,
            port=db.port,
            database=db.database,
            user=db.user,
            password=db.password
        )
        cursor = metadata_conn.get_cursor()
        
        try:
            cursor.execute(
                """
                SELECT tp.api_rate_limit_per_minute
                FROM tenants t
                JOIN tenant_plans tp ON t.plan_id = tp.id
                WHERE t.id = %s
                """,
                (str(tenant_id),)
            )
            
            result = cursor.fetchone()
            
            if result:
                return result['api_rate_limit_per_minute']
            
            return 60  # Default limit
            
        except Exception as e:
            logger.error(f"Failed to get tenant limit: {e}")
            return 60  # Default on error
        finally:
            metadata_conn.close()
    
    def _check_rate_limit(
        self,
        tenant_id: UUID,
        endpoint: str,
        method: str
    ) -> tuple[bool, int, datetime]:
        """
        Check if request is within rate limit.
        
        Returns:
            (allowed, remaining_requests, reset_timestamp)
        """
        db = get_metadata_db()
        metadata_conn = MetadataConnection(
            host=db.host,
            port=db.port,
            database=db.database,
            user=db.user,
            password=db.password
        )
        cursor = metadata_conn.get_cursor()
        
        try:
            # Get tenant's rate limit
            limit = self._get_tenant_limit(tenant_id)
            
            # Get current window (1 minute)
            now = datetime.now()
            window_start = now.replace(second=0, microsecond=0)
            window_end = window_start + timedelta(minutes=1)
            
            # Get or create tracking record
            cursor.execute(
                """
                INSERT INTO rate_limit_tracking 
                (tenant_id, endpoint, window_start, window_duration_seconds, request_count)
                VALUES (%s, %s, %s, 60, 1)
                ON CONFLICT (tenant_id, endpoint, window_start)
                DO UPDATE SET 
                    request_count = rate_limit_tracking.request_count + 1,
                    updated_at = NOW()
                RETURNING request_count
                """,
                (str(tenant_id), f"{method} {endpoint}", window_start)
            )
            
            result = cursor.fetchone()
            current_count = result['request_count']
            
            metadata_conn.commit()
            
            # Check if limit exceeded
            allowed = current_count <= limit
            remaining = max(0, limit - current_count)
            
            return (allowed, remaining, window_end)
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            # On error, allow request
            return (True, limit, window_end)
        finally:
            metadata_conn.close()
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "level": level.upper(),
            "service": "rate_limiter",
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


class RateLimitChecker:
    """Programmatic rate limit checking."""
    
    @staticmethod
    def check_quota(tenant_id: UUID, action: str = "job_creation") -> dict:
        """
        Check if tenant can perform an action based on quotas.
        
        Args:
            tenant_id: Tenant ID
            action: Action type ('job_creation', 'concurrent_jobs')
            
        Returns:
            Dictionary with allowed status and details
        """
        db = get_metadata_db()
        metadata_conn = MetadataConnection(
            host=db.host,
            port=db.port,
            database=db.database,
            user=db.user,
            password=db.password
        )
        cursor = metadata_conn.get_cursor()
        
        try:
            cursor.execute(
                """
                SELECT * FROM tenant_current_usage
                WHERE tenant_id = %s
                """,
                (str(tenant_id),)
            )
            
            result = cursor.fetchone()
            
            if not result:
                return {"allowed": False, "reason": "Tenant not found"}
            
            if action == "concurrent_jobs":
                if result['active_jobs'] >= result['max_concurrent_jobs']:
                    return {
                        "allowed": False,
                        "reason": f"Concurrent job limit reached ({result['active_jobs']}/{result['max_concurrent_jobs']})",
                        "current": result['active_jobs'],
                        "limit": result['max_concurrent_jobs']
                    }
            
            elif action == "job_creation":
                if result['gb_used_this_month'] >= result['max_gb_per_month']:
                    return {
                        "allowed": False,
                        "reason": f"Monthly data transfer limit exceeded ({result['gb_used_this_month']:.2f}/{result['max_gb_per_month']} GB)",
                        "current": float(result['gb_used_this_month']),
                        "limit": result['max_gb_per_month']
                    }
            
            return {"allowed": True}
            
        except Exception as e:
            logger.error(f"Quota check failed: {e}")
            return {"allowed": True}  # Allow on error
        finally:
            metadata_conn.close()
