-- Fix SaaS schema errors and add missing fields

-- Fix audit_logs (add missing columns)
ALTER TABLE audit_logs 
ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'success',
ADD COLUMN IF NOT EXISTS error_message TEXT;

-- Recreate tenant_current_usage view with proper tenant_id casting
DROP VIEW IF EXISTS tenant_current_usage CASCADE;

CREATE OR REPLACE VIEW tenant_current_usage AS
SELECT 
    t.id as tenant_id,
    t.name as tenant_name,
    tp.name as plan_name,
    tp.max_concurrent_jobs,
    tp.max_gb_per_month,
    COUNT(DISTINCT mj.id) FILTER (WHERE mj.status = 'running') as active_jobs,
    COUNT(DISTINCT mj.id) FILTER (WHERE mj.created_at >= DATE_TRUNC('month', NOW())) as jobs_this_month,
    COALESCE(SUM(ue.metric_value) FILTER (WHERE ue.metric_name = 'gb_migrated' AND ue.timestamp >= DATE_TRUNC('month', NOW())), 0) as gb_used_this_month,
    tp.max_gb_per_month - COALESCE(SUM(ue.metric_value) FILTER (WHERE ue.metric_name = 'gb_migrated' AND ue.timestamp >= DATE_TRUNC('month', NOW())), 0) as gb_remaining_this_month
FROM tenants t
JOIN tenant_plans tp ON t.plan_id = tp.id
LEFT JOIN migration_jobs mj ON t.id::text = mj.tenant_id::text AND (mj.lifecycle_status = 'active' OR mj.lifecycle_status IS NULL)
LEFT JOIN usage_events ue ON t.id = ue.tenant_id
GROUP BY t.id, t.name, tp.name, tp.max_concurrent_jobs, tp.max_gb_per_month;

COMMENT ON VIEW tenant_current_usage IS 'Real-time resource usage and limits per tenant';
