"""
Audit Logs API Endpoints
Security and compliance audit trail
"""
from typing import List, Dict, Any, Optional
from uuid import UUID
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from services.api.routers.auth import get_current_user
from services.api.metadata import get_metadata_db, MetadataRepository

router = APIRouter(
    prefix="/audit",
    tags=["Audit Logs"]
)


def get_metadata_repo() -> MetadataRepository:
    """Dependency: Get metadata repository."""
    return MetadataRepository(get_metadata_db())


class AuditLogEntry(BaseModel):
    """Audit log entry."""
    id: str
    user_id: str = None
    user_email: str = None
    action: str
    resource_type: str = None
    resource_id: str = None
    details: Dict[str, Any]
    ip_address: str = None
    user_agent: str = None
    status: str
    error_message: str = None
    timestamp: str


class AuditSummary(BaseModel):
    """Audit activity summary."""
    total_actions: int
    successful_actions: int
    failed_actions: int
    unique_users: int
    action_breakdown: Dict[str, int]


@router.get("/logs", response_model=List[AuditLogEntry])
async def list_audit_logs(
    action: Optional[str] = Query(None, description="Filter by action type"),
    user_id: Optional[UUID] = Query(None, description="Filter by user"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    status: Optional[str] = Query(None, description="Filter by status (success/failed)"),
    days: int = Query(7, ge=1, le=90, description="Number of days of history"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Dict = Depends(get_current_user),
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    List audit logs for the authenticated tenant.
    
    Returns filtered audit trail with user actions and outcomes.
    """
    tenant_id = current_user['tenant_id']
    conn = repo.db.get_connection()
    cursor = conn.cursor()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                al.*,
                u.email as user_email
            FROM audit_logs al
            LEFT JOIN users u ON al.user_id = u.id
            WHERE al.tenant_id = %s
            AND al.created_at >= %s
        """
        params = [tenant_id, start_date]
        
        if action:
            query += " AND al.action = %s"
            params.append(action)
        
        if user_id:
            query += " AND al.user_id = %s"
            params.append(str(user_id))
        
        if resource_type:
            query += " AND al.resource_type = %s"
            params.append(resource_type)
        
        if status:
            query += " AND al.status = %s"
            params.append(status)
        
        query += " ORDER BY al.created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        logs = cursor.fetchall()
        
        return [
            AuditLogEntry(
                id=str(log['id']),
                user_id=str(log['user_id']) if log['user_id'] else None,
                user_email=log['user_email'],
                action=log['action'],
                resource_type=log['resource_type'],
                resource_id=str(log['resource_id']) if log['resource_id'] else None,
                details=log['details'] or {},
                ip_address=str(log['ip_address']) if log['ip_address'] else None,
                user_agent=log['user_agent'],
                status=log['status'] or 'success',
                error_message=log['error_message'],
                timestamp=log['created_at'].isoformat()
            )
            for log in logs
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch audit logs: {str(e)}")
    finally:
        repo.db.return_connection(conn)


@router.get("/summary", response_model=AuditSummary)
async def get_audit_summary(
    days: int = Query(30, ge=1, le=365),
    current_user: Dict = Depends(get_current_user),
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    Get audit activity summary.
    
    Returns aggregated statistics on user actions.
    """
    tenant_id = current_user['tenant_id']
    conn = repo.db.get_connection()
    cursor = conn.cursor()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        cursor.execute(
            """
            SELECT 
                COUNT(*) as total_actions,
                COUNT(*) FILTER (WHERE status = 'success') as successful_actions,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_actions,
                COUNT(DISTINCT user_id) as unique_users
            FROM audit_logs
            WHERE tenant_id = %s
            AND created_at >= %s
            """,
            (tenant_id, start_date)
        )
        
        summary = cursor.fetchone()
        
        # Get action breakdown
        cursor.execute(
            """
            SELECT action, COUNT(*) as count
            FROM audit_logs
            WHERE tenant_id = %s
            AND created_at >= %s
            GROUP BY action
            ORDER BY count DESC
            LIMIT 20
            """,
            (tenant_id, start_date)
        )
        
        action_breakdown = {row['action']: row['count'] for row in cursor.fetchall()}
        
        return AuditSummary(
            total_actions=summary['total_actions'],
            successful_actions=summary['successful_actions'],
            failed_actions=summary['failed_actions'],
            unique_users=summary['unique_users'],
            action_breakdown=action_breakdown
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch summary: {str(e)}")
    finally:
        repo.db.return_connection(conn)


@router.get("/actions")
async def list_action_types(
    current_user: Dict = Depends(get_current_user),
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    List all unique action types available for filtering.
    
    Returns list of action types that have been logged.
    """
    tenant_id = current_user['tenant_id']
    conn = repo.db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            SELECT DISTINCT action
            FROM audit_logs
            WHERE tenant_id = %s
            ORDER BY action ASC
            """,
            (tenant_id,)
        )
        
        actions = [row['action'] for row in cursor.fetchall()]
        
        return {"actions": actions}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch actions: {str(e)}")
    finally:
        repo.db.return_connection(conn)


@router.get("/user-activity/{user_id}")
async def get_user_activity(
    user_id: UUID,
    days: int = Query(30, ge=1, le=365),
    current_user: Dict = Depends(get_current_user),
    repo: MetadataRepository = Depends(get_metadata_repo)
):
    """
    Get activity history for a specific user.
    
    Returns user's actions and timestamps.
    """
    tenant_id = current_user['tenant_id']
    conn = repo.db.get_connection()
    cursor = conn.cursor()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        cursor.execute(
            """
            SELECT 
                al.action,
                al.resource_type,
                al.status,
                al.created_at,
                al.ip_address,
                COUNT(*) OVER (PARTITION BY action) as action_count
            FROM audit_logs al
            WHERE al.tenant_id = %s
            AND al.user_id = %s
            AND al.created_at >= %s
            ORDER BY al.created_at DESC
            LIMIT 100
            """,
            (tenant_id, str(user_id), start_date)
        )
        
        activities = cursor.fetchall()
        
        return [
            {
                "action": act['action'],
                "resource_type": act['resource_type'],
                "status": act['status'],
                "timestamp": act['created_at'].isoformat(),
                "ip_address": str(act['ip_address']) if act['ip_address'] else None,
                "action_count": act['action_count']
            }
            for act in activities
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch user activity: {str(e)}")
    finally:
        repo.db.return_connection(conn)


def log_audit_event(
    tenant_id: UUID,
    user_id: UUID,
    action: str,
    resource_type: str = None,
    resource_id: UUID = None,
    details: Dict[str, Any] = None,
    ip_address: str = None,
    user_agent: str = None,
    status: str = "success",
    error_message: str = None
):
    """
    Helper function to log audit events from other services.
    
    Args:
        tenant_id: Tenant ID
        user_id: User ID
        action: Action type (e.g., 'job.created', 'user.invited')
        resource_type: Type of resource ('job', 'user', 'secret')
        resource_id: ID of the resource
        details: Additional details as JSON
        ip_address: Client IP address
        user_agent: User agent string
        status: 'success' or 'failed'
        error_message: Error message if failed
    """
    repo = get_metadata_repo()
    conn = repo.db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            INSERT INTO audit_logs
            (tenant_id, user_id, action, resource_type, resource_id, details, 
             ip_address, user_agent, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(tenant_id),
                str(user_id) if user_id else None,
                action,
                resource_type,
                str(resource_id) if resource_id else None,
                details,
                ip_address,
                user_agent,
                status,
                error_message
            )
        )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Failed to log audit event: {e}")
    finally:
        repo.db.return_connection(conn)
