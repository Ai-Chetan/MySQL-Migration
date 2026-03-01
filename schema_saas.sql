-- Phase 4: SaaS Layer Schema
-- Multi-tenant billing, usage metering, secrets vault, audit logs, rate limiting

-- ============================================================================
-- 1. TENANT PLANS & LIMITS
-- ============================================================================

CREATE TABLE IF NOT EXISTS tenant_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE, -- 'free', 'starter', 'professional', 'enterprise'
    display_name VARCHAR(200) NOT NULL,
    description TEXT,
    price_monthly DECIMAL(10,2) DEFAULT 0.00,
    price_per_gb DECIMAL(10,4) DEFAULT 0.00, -- Cost per GB migrated
    max_concurrent_jobs INTEGER DEFAULT 1,
    max_workers_per_job INTEGER DEFAULT 4,
    max_gb_per_month INTEGER DEFAULT 10,
    max_tables_per_job INTEGER DEFAULT 50,
    api_rate_limit_per_minute INTEGER DEFAULT 60,
    support_level VARCHAR(50) DEFAULT 'community', -- 'community', 'email', 'priority', '24/7'
    features JSONB DEFAULT '{}', -- Additional features
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert default plans
INSERT INTO tenant_plans (name, display_name, description, price_monthly, price_per_gb, max_concurrent_jobs, max_workers_per_job, max_gb_per_month, api_rate_limit_per_minute, support_level)
VALUES 
    ('free', 'Free Tier', 'Perfect for testing and small projects', 0.00, 0.00, 1, 2, 10, 30, 'community'),
    ('starter', 'Starter', 'For small teams and growing businesses', 49.00, 0.10, 3, 4, 100, 120, 'email'),
    ('professional', 'Professional', 'For professional teams with high volume', 199.00, 0.05, 10, 8, 500, 300, 'priority'),
    ('enterprise', 'Enterprise', 'Custom solutions for large organizations', 999.00, 0.02, 50, 16, 5000, 1000, '24/7')
ON CONFLICT (name) DO NOTHING;

-- Add plan_id to tenants table
ALTER TABLE tenants 
ADD COLUMN IF NOT EXISTS plan_id UUID REFERENCES tenant_plans(id),
ADD COLUMN IF NOT EXISTS subscription_status VARCHAR(50) DEFAULT 'active', -- 'active', 'suspended', 'cancelled'
ADD COLUMN IF NOT EXISTS subscription_start_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS subscription_end_date TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS billing_cycle VARCHAR(20) DEFAULT 'monthly'; -- 'monthly', 'annual'

-- Set default plan for existing tenants
UPDATE tenants SET plan_id = (SELECT id FROM tenant_plans WHERE name = 'free' LIMIT 1) WHERE plan_id IS NULL;

-- ============================================================================
-- 2. USAGE METERING & EVENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS usage_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    job_id UUID REFERENCES migration_jobs(id) ON DELETE SET NULL,
    event_type VARCHAR(50) NOT NULL, -- 'data_migrated', 'job_created', 'compute_time', 'api_call', 'validation'
    metric_name VARCHAR(100) NOT NULL, -- 'gb_migrated', 'rows_processed', 'hours_compute', etc.
    metric_value DECIMAL(20,4) NOT NULL,
    unit VARCHAR(20), -- 'gb', 'rows', 'hours', 'count'
    metadata JSONB DEFAULT '{}',
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_usage_events_tenant_time ON usage_events(tenant_id, timestamp DESC);
CREATE INDEX idx_usage_events_job ON usage_events(job_id);
CREATE INDEX idx_usage_events_type ON usage_events(event_type);

-- Usage aggregation view for current month
CREATE OR REPLACE VIEW tenant_usage_current_month AS
SELECT 
    ue.tenant_id,
    t.name as tenant_name,
    tp.name as plan_name,
    DATE_TRUNC('month', NOW()) as month,
    SUM(CASE WHEN ue.metric_name = 'gb_migrated' THEN ue.metric_value ELSE 0 END) as total_gb_migrated,
    SUM(CASE WHEN ue.metric_name = 'rows_processed' THEN ue.metric_value ELSE 0 END) as total_rows_processed,
    SUM(CASE WHEN ue.event_type = 'job_created' THEN 1 ELSE 0 END) as total_jobs_created,
    SUM(CASE WHEN ue.metric_name = 'compute_hours' THEN ue.metric_value ELSE 0 END) as total_compute_hours,
    tp.max_gb_per_month as plan_limit_gb,
    (SUM(CASE WHEN ue.metric_name = 'gb_migrated' THEN ue.metric_value ELSE 0 END) / NULLIF(tp.max_gb_per_month, 0) * 100) as usage_percentage
FROM usage_events ue
JOIN tenants t ON ue.tenant_id = t.id
JOIN tenant_plans tp ON t.plan_id = tp.id
WHERE ue.timestamp >= DATE_TRUNC('month', NOW())
GROUP BY ue.tenant_id, t.name, tp.name, tp.max_gb_per_month;

-- ============================================================================
-- 3. BILLING & INVOICES
-- ============================================================================

CREATE TABLE IF NOT EXISTS invoices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    invoice_number VARCHAR(50) UNIQUE NOT NULL,
    billing_period_start DATE NOT NULL,
    billing_period_end DATE NOT NULL,
    subtotal DECIMAL(10,2) DEFAULT 0.00,
    tax DECIMAL(10,2) DEFAULT 0.00,
    total DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'paid', 'overdue', 'cancelled'
    due_date DATE NOT NULL,
    paid_at TIMESTAMP WITH TIME ZONE,
    line_items JSONB DEFAULT '[]', -- Array of line items with descriptions and amounts
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_invoices_tenant ON invoices(tenant_id);
CREATE INDEX idx_invoices_status ON invoices(status);
CREATE INDEX idx_invoices_due_date ON invoices(due_date);

-- ============================================================================
-- 4. SECRETS VAULT (Encrypted Credentials Storage)
-- ============================================================================

CREATE TABLE IF NOT EXISTS secrets_vault (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    secret_type VARCHAR(50) NOT NULL, -- 'database', 'api_key', 'ssh_key', 'certificate'
    secret_name VARCHAR(200) NOT NULL,
    encrypted_value TEXT NOT NULL, -- AES-256 encrypted
    encryption_key_id VARCHAR(100) NOT NULL, -- Reference to encryption key rotation
    metadata JSONB DEFAULT '{}',
    created_by UUID REFERENCES users(id),
    last_accessed_at TIMESTAMP WITH TIME ZONE,
    access_count INTEGER DEFAULT 0,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tenant_id, secret_name)
);

CREATE INDEX idx_secrets_vault_tenant ON secrets_vault(tenant_id);
CREATE INDEX idx_secrets_vault_type ON secrets_vault(secret_type);

COMMENT ON TABLE secrets_vault IS 'Encrypted storage for database credentials and API keys';
COMMENT ON COLUMN secrets_vault.encrypted_value IS 'AES-256-GCM encrypted credential value';

-- ============================================================================
-- 5. ENHANCED AUDIT LOGS
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL, -- 'job.created', 'job.cancelled', 'user.invited', 'plan.upgraded'
    resource_type VARCHAR(50), -- 'job', 'user', 'secret', 'invoice'
    resource_id UUID,
    details JSONB DEFAULT '{}',
    ip_address INET,
    user_agent TEXT,
    status VARCHAR(20) DEFAULT 'success', -- 'success', 'failed', 'blocked'
    error_message TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_tenant_time ON audit_logs(tenant_id, timestamp DESC);
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_action ON audit_logs(action);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_id);

COMMENT ON TABLE audit_logs IS 'Comprehensive audit trail for compliance and security';

-- ============================================================================
-- 6. RATE LIMITING
-- ============================================================================

CREATE TABLE IF NOT EXISTS rate_limit_tracking (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    endpoint VARCHAR(200) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_duration_seconds INTEGER DEFAULT 60,
    request_count INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tenant_id, endpoint, window_start)
);

CREATE INDEX idx_rate_limit_tenant_time ON rate_limit_tracking(tenant_id, window_start);

-- ============================================================================
-- 7. JOB LIFECYCLE ENHANCEMENTS
-- ============================================================================

-- Add new status values and control fields to migration_jobs
ALTER TABLE migration_jobs
ADD COLUMN IF NOT EXISTS lifecycle_status VARCHAR(20) DEFAULT 'active', -- 'active', 'paused', 'cancelled', 'archived'
ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS paused_by UUID REFERENCES users(id),
ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS cancelled_by UUID REFERENCES users(id),
ADD COLUMN IF NOT EXISTS cancellation_reason TEXT,
ADD COLUMN IF NOT EXISTS estimated_cost DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS actual_cost DECIMAL(10,2);

-- ============================================================================
-- 8. TENANT RESOURCE USAGE (Current State)
-- ============================================================================

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
LEFT JOIN migration_jobs mj ON t.id = mj.tenant_id AND mj.lifecycle_status = 'active'
LEFT JOIN usage_events ue ON t.id = ue.tenant_id
GROUP BY t.id, t.name, tp.name, tp.max_concurrent_jobs, tp.max_gb_per_month;

-- ============================================================================
-- 9. WEBHOOKS (For Event Notifications)
-- ============================================================================

CREATE TABLE IF NOT EXISTS tenant_webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    url TEXT NOT NULL,
    secret VARCHAR(100) NOT NULL, -- For HMAC signature verification
    events TEXT[] NOT NULL, -- ['job.completed', 'job.failed', 'invoice.generated']
    enabled BOOLEAN DEFAULT true,
    retry_count INTEGER DEFAULT 3,
    timeout_seconds INTEGER DEFAULT 30,
    last_triggered_at TIMESTAMP WITH TIME ZONE,
    last_status VARCHAR(20), -- 'success', 'failed'
    failure_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_webhooks_tenant ON tenant_webhooks(tenant_id);
CREATE INDEX idx_webhooks_enabled ON tenant_webhooks(enabled);

-- ============================================================================
-- 10. API KEYS (For Programmatic Access)
-- ============================================================================

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    key_name VARCHAR(200) NOT NULL,
    key_hash TEXT NOT NULL UNIQUE, -- SHA-256 hash of the actual key
    key_prefix VARCHAR(20) NOT NULL, -- First few characters for identification
    scopes TEXT[] DEFAULT ARRAY['read'], -- 'read', 'write', 'admin'
    last_used_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    revoked_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id);
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);

COMMENT ON TABLE api_keys IS 'API keys for programmatic access to the platform';
COMMENT ON COLUMN api_keys.key_hash IS 'SHA-256 hash of the actual API key (never store plaintext)';

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE tenant_plans IS 'Subscription plans with resource limits and pricing';
COMMENT ON TABLE usage_events IS 'Time-series usage tracking for billing and analytics';
COMMENT ON TABLE invoices IS 'Monthly invoices with line items for billing';
COMMENT ON TABLE rate_limit_tracking IS 'Per-tenant API rate limiting enforcement';
COMMENT ON VIEW tenant_usage_current_month IS 'Aggregated usage metrics for billing period';
COMMENT ON VIEW tenant_current_usage IS 'Real-time resource usage and limits per tenant';
