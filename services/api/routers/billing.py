"""
Billing & Usage API Endpoints
Tenant billing, usage tracking, plan management
"""
from typing import List, Dict, Any
from uuid import UUID
from datetime import datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from services.api.routers.auth import get_current_user
from services.api.usage_tracker import get_usage_tracker
from services.worker.db import MetadataConnection

router = APIRouter(
    prefix="/billing",
    tags=["Billing & Usage"]
)


class PlanInfo(BaseModel):
    """Subscription plan information."""
    id: str
    name: str
    display_name: str
    description: str
    price_monthly: float
    price_per_gb: float
    max_concurrent_jobs: int
    max_workers_per_job: int
    max_gb_per_month: int
    api_rate_limit_per_minute: int
    support_level: str
    features: Dict[str, Any]


class UsageSummary(BaseModel):
    """Current usage summary."""
    gb_migrated: float
    rows_processed: int
    jobs_created: int
    compute_hours: float
    plan_limit_gb: int
    usage_percentage: float
    gb_remaining: float
    warnings: List[str]


class Invoice(BaseModel):
    """Invoice details."""
    id: str
    invoice_number: str
    billing_period_start: str
    billing_period_end: str
    subtotal: float
    tax: float
    total: float
    currency: str
    status: str
    due_date: str
    paid_at: str = None
    line_items: List[Dict[str, Any]]


class UsageEvent(BaseModel):
    """Usage event record."""
    event_type: str
    metric_name: str
    metric_value: float
    unit: str
    timestamp: str
    metadata: Dict[str, Any]


@router.get("/plans", response_model=List[PlanInfo])
async def list_plans(
    current_user: Dict = Depends(get_current_user)
):
    """
    List all available subscription plans.
    
    Returns plan details including pricing and limits.
    """
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        cursor.execute(
            """
            SELECT * FROM tenant_plans
            ORDER BY price_monthly ASC
            """
        )
        
        plans = cursor.fetchall()
        
        return [
            PlanInfo(
                id=str(plan['id']),
                name=plan['name'],
                display_name=plan['display_name'],
                description=plan['description'],
                price_monthly=float(plan['price_monthly']),
                price_per_gb=float(plan['price_per_gb']),
                max_concurrent_jobs=plan['max_concurrent_jobs'],
                max_workers_per_job=plan['max_workers_per_job'],
                max_gb_per_month=plan['max_gb_per_month'],
                api_rate_limit_per_minute=plan['api_rate_limit_per_minute'],
                support_level=plan['support_level'],
                features=plan['features'] or {}
            )
            for plan in plans
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch plans: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/usage/current", response_model=UsageSummary)
async def get_current_usage(
    current_user: Dict = Depends(get_current_user)
):
    """
    Get current month usage for the authenticated tenant.
    
    Returns GB migrated, rows processed, jobs created, and quota status.
    """
    tenant_id = UUID(current_user['tenant_id'])
    usage_tracker = get_usage_tracker()
    
    try:
        # Get current month usage
        usage = usage_tracker.get_current_month_usage(tenant_id)
        
        # Check for quota warnings
        quota_status = usage_tracker.check_quota_exceeded(tenant_id)
        
        return UsageSummary(
            gb_migrated=usage['gb_migrated'],
            rows_processed=usage['rows_processed'],
            jobs_created=usage['jobs_created'],
            compute_hours=usage['compute_hours'],
            plan_limit_gb=usage['plan_limit_gb'],
            usage_percentage=usage['usage_percentage'],
            gb_remaining=usage['plan_limit_gb'] - usage['gb_migrated'],
            warnings=quota_status['warnings']
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch usage: {str(e)}")


@router.get("/usage/history")
async def get_usage_history(
    days: int = Query(30, ge=1, le=365, description="Number of days of history"),
    current_user: Dict = Depends(get_current_user)
):
    """
    Get usage history over time for charts.
    
    Returns daily aggregated usage metrics.
    """
    tenant_id = current_user['tenant_id']
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        cursor.execute(
            """
            SELECT 
                DATE(timestamp) as date,
                SUM(CASE WHEN metric_name = 'gb_migrated' THEN metric_value ELSE 0 END) as gb_migrated,
                SUM(CASE WHEN metric_name = 'rows_processed' THEN metric_value ELSE 0 END) as rows_processed,
                COUNT(DISTINCT job_id) as jobs_count
            FROM usage_events
            WHERE tenant_id = %s
            AND timestamp >= %s
            GROUP BY DATE(timestamp)
            ORDER BY date ASC
            """,
            (tenant_id, start_date)
        )
        
        results = cursor.fetchall()
        
        return [
            {
                "date": row['date'].isoformat(),
                "gb_migrated": float(row['gb_migrated']),
                "rows_processed": int(row['rows_processed']),
                "jobs_count": row['jobs_count']
            }
            for row in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/invoices", response_model=List[Invoice])
async def list_invoices(
    limit: int = Query(10, ge=1, le=100),
    current_user: Dict = Depends(get_current_user)
):
    """
    List invoices for the authenticated tenant.
    
    Returns invoice history with payment status.
    """
    tenant_id = current_user['tenant_id']
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        cursor.execute(
            """
            SELECT * FROM invoices
            WHERE tenant_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_id, limit)
        )
        
        invoices = cursor.fetchall()
        
        return [
            Invoice(
                id=str(inv['id']),
                invoice_number=inv['invoice_number'],
                billing_period_start=inv['billing_period_start'].isoformat(),
                billing_period_end=inv['billing_period_end'].isoformat(),
                subtotal=float(inv['subtotal']),
                tax=float(inv['tax']),
                total=float(inv['total']),
                currency=inv['currency'],
                status=inv['status'],
                due_date=inv['due_date'].isoformat(),
                paid_at=inv['paid_at'].isoformat() if inv['paid_at'] else None,
                line_items=inv['line_items'] or []
            )
            for inv in invoices
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch invoices: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/plan/current")
async def get_current_plan(
    current_user: Dict = Depends(get_current_user)
):
    """
    Get current subscription plan for the authenticated tenant.
    
    Returns plan details and subscription status.
    """
    tenant_id = current_user['tenant_id']
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        cursor.execute(
            """
            SELECT 
                t.subscription_status,
                t.subscription_start_date,
                t.subscription_end_date,
                t.billing_cycle,
                tp.*
            FROM tenants t
            JOIN tenant_plans tp ON t.plan_id = tp.id
            WHERE t.id = %s
            """,
            (tenant_id,)
        )
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Tenant not found")
        
        return {
            "plan": {
                "id": str(result['id']),
                "name": result['name'],
                "display_name": result['display_name'],
                "price_monthly": float(result['price_monthly']),
                "price_per_gb": float(result['price_per_gb']),
                "max_concurrent_jobs": result['max_concurrent_jobs'],
                "max_gb_per_month": result['max_gb_per_month'],
                "support_level": result['support_level']
            },
            "subscription": {
                "status": result['subscription_status'],
                "start_date": result['subscription_start_date'].isoformat() if result['subscription_start_date'] else None,
                "end_date": result['subscription_end_date'].isoformat() if result['subscription_end_date'] else None,
                "billing_cycle": result['billing_cycle']
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch plan: {str(e)}")
    finally:
        metadata_conn.close()


@router.post("/plan/upgrade")
async def upgrade_plan(
    plan_id: UUID,
    current_user: Dict = Depends(get_current_user)
):
    """
    Upgrade subscription plan.
    
    In production, this would integrate with payment gateway.
    """
    tenant_id = current_user['tenant_id']
    metadata_conn = MetadataConnection()
    cursor = metadata_conn.get_cursor()
    
    try:
        # Verify plan exists
        cursor.execute(
            "SELECT name, display_name, price_monthly FROM tenant_plans WHERE id = %s",
            (str(plan_id),)
        )
        
        plan = cursor.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        
        # Update tenant plan
        cursor.execute(
            """
            UPDATE tenants
            SET plan_id = %s,
                subscription_start_date = NOW()
            WHERE id = %s
            """,
            (str(plan_id), tenant_id)
        )
        
        metadata_conn.commit()
        
        # Log audit event
        cursor.execute(
            """
            INSERT INTO audit_logs
            (tenant_id, user_id, action, resource_type, resource_id, details, status)
            VALUES (%s, %s, 'plan.upgraded', 'subscription', %s, %s, 'success')
            """,
            (
                tenant_id,
                current_user['user_id'],
                str(plan_id),
                f'{{"new_plan": "{plan["name"]}", "price": {plan["price_monthly"]}}}'
            )
        )
        
        metadata_conn.commit()
        
        return {
            "success": True,
            "message": f"Plan upgraded to {plan['display_name']}",
            "plan_name": plan['name']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        metadata_conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to upgrade plan: {str(e)}")
    finally:
        metadata_conn.close()


@router.get("/quota/check")
async def check_quota(
    action: str = Query("job_creation", description="Action to check (job_creation, concurrent_jobs)"),
    current_user: Dict = Depends(get_current_user)
):
    """
    Check if tenant can perform an action based on quotas.
    
    Returns allowed status and current usage details.
    """
    tenant_id = UUID(current_user['tenant_id'])
    usage_tracker = get_usage_tracker()
    
    try:
        quota_status = usage_tracker.check_quota_exceeded(tenant_id)
        
        return {
            "allowed": not quota_status['exceeded'],
            "usage": quota_status.get('usage', {}),
            "warnings": quota_status.get('warnings', [])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check quota: {str(e)}")
