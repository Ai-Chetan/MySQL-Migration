-- Migration Platform Metadata Database Schema
-- Database: PostgreSQL
-- Purpose: Single source of truth for migration orchestration

-- ==========================================
-- AUTHENTICATION & MULTI-TENANCY
-- ==========================================

-- Tenants table
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    plan VARCHAR(50) DEFAULT 'free' NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    subscription_end_date TIMESTAMP,
    
    -- Billing
    stripe_customer_id VARCHAR(255),
    stripe_subscription_id VARCHAR(255),
    
    -- Usage limits
    max_migrations_per_month INTEGER DEFAULT 10,
    max_concurrent_migrations INTEGER DEFAULT 1,
    
    -- Stats
    total_migrations INTEGER DEFAULT 0,
    total_rows_migrated BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tenants_plan ON tenants(plan);
CREATE INDEX IF NOT EXISTS idx_tenants_active ON tenants(is_active);

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) DEFAULT 'user' NOT NULL CHECK (role IN ('admin', 'user', 'viewer')),
    
    is_active BOOLEAN DEFAULT TRUE,
    email_verified BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP,
    last_activity TIMESTAMP,
    
    -- Profile
    full_name VARCHAR(255),
    avatar_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

-- Audit logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100),
    resource_id UUID,
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant ON audit_logs(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);

-- ==========================================
-- 1. MIGRATION JOBS TABLE
-- ==========================================
CREATE TABLE IF NOT EXISTS migration_jobs (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(100) DEFAULT 'local' NOT NULL,
    
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('pending','planning','running','completed','failed','paused')
    ),
    
    source_config JSONB NOT NULL,
    target_config JSONB NOT NULL,
    
    -- Counters
    total_tables INTEGER DEFAULT 0,
    total_chunks INTEGER DEFAULT 0,
    completed_chunks INTEGER DEFAULT 0,
    failed_chunks INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Error tracking
    last_error TEXT
);

-- Indexes for migration_jobs
CREATE INDEX IF NOT EXISTS idx_jobs_status ON migration_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON migration_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_id ON migration_jobs(tenant_id);

-- ==========================================
-- 2. MIGRATION TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS migration_tables (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    
    table_name VARCHAR(255) NOT NULL,
    primary_key_column VARCHAR(255),
    
    -- Row tracking
    total_rows BIGINT,
    total_chunks INTEGER DEFAULT 0,
    completed_chunks INTEGER DEFAULT 0,
    failed_chunks INTEGER DEFAULT 0,
    
    status VARCHAR(30) CHECK (
        status IN ('pending','running','completed','failed')
    ) DEFAULT 'pending',
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for migration_tables
CREATE INDEX IF NOT EXISTS idx_tables_job_id ON migration_tables(job_id);
CREATE INDEX IF NOT EXISTS idx_tables_status ON migration_tables(status);
CREATE INDEX IF NOT EXISTS idx_tables_job_table ON migration_tables(job_id, table_name);

-- ==========================================
-- 3. MIGRATION CHUNKS (MOST CRITICAL)
-- ==========================================
CREATE TABLE IF NOT EXISTS migration_chunks (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    table_id UUID NOT NULL REFERENCES migration_tables(id) ON DELETE CASCADE,
    
    table_name VARCHAR(255) NOT NULL,
    
    -- Chunk range
    pk_start BIGINT NOT NULL,
    pk_end BIGINT NOT NULL,
    
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('pending','running','completed','failed')
    ) DEFAULT 'pending',
    
    -- Retry logic
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Metrics
    rows_processed BIGINT DEFAULT 0,
    checksum VARCHAR(128),
    duration_ms BIGINT,
    
    -- Timestamps
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    last_heartbeat TIMESTAMP,
    
    -- Error tracking
    last_error TEXT,
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Critical indexes for migration_chunks
CREATE INDEX IF NOT EXISTS idx_chunks_job_id ON migration_chunks(job_id);
CREATE INDEX IF NOT EXISTS idx_chunks_status ON migration_chunks(status);
CREATE INDEX IF NOT EXISTS idx_chunks_table_id ON migration_chunks(table_id);
CREATE INDEX IF NOT EXISTS idx_chunks_heartbeat ON migration_chunks(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_chunks_pending ON migration_chunks(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_chunks_running ON migration_chunks(status, last_heartbeat) WHERE status = 'running';

-- ==========================================
-- 4. WORKER HEARTBEATS (Optional for Phase 1)
-- ==========================================
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id VARCHAR(100) PRIMARY KEY,
    last_seen TIMESTAMP NOT NULL,
    current_chunk UUID,
    status VARCHAR(30) DEFAULT 'idle'
);

-- Index for worker_heartbeats
CREATE INDEX IF NOT EXISTS idx_worker_last_seen ON worker_heartbeats(last_seen DESC);

-- ==========================================
-- 5. HELPER VIEWS
-- ==========================================

-- View for job progress summary
CREATE OR REPLACE VIEW v_job_progress AS
SELECT 
    j.id,
    j.tenant_id,
    j.status,
    j.total_tables,
    j.total_chunks,
    j.completed_chunks,
    j.failed_chunks,
    CASE 
        WHEN j.total_chunks > 0 THEN 
            ROUND((j.completed_chunks::DECIMAL / j.total_chunks) * 100, 2)
        ELSE 0 
    END as progress_percentage,
    j.created_at,
    j.started_at,
    j.completed_at,
    EXTRACT(EPOCH FROM (COALESCE(j.completed_at, NOW()) - j.started_at)) as duration_seconds
FROM migration_jobs j;

-- View for table progress
CREATE OR REPLACE VIEW v_table_progress AS
SELECT 
    t.id,
    t.job_id,
    t.table_name,
    t.total_rows,
    t.total_chunks,
    t.completed_chunks,
    t.failed_chunks,
    t.status,
    CASE 
        WHEN t.total_chunks > 0 THEN 
            ROUND((t.completed_chunks::DECIMAL / t.total_chunks) * 100, 2)
        ELSE 0 
    END as progress_percentage,
    SUM(c.rows_processed) as total_rows_processed
FROM migration_tables t
LEFT JOIN migration_chunks c ON c.table_id = t.id AND c.status = 'completed'
GROUP BY t.id, t.job_id, t.table_name, t.total_rows, t.total_chunks, 
         t.completed_chunks, t.failed_chunks, t.status;

-- ==========================================
-- 6. UTILITY FUNCTIONS
-- ==========================================

-- Function to update job counters
CREATE OR REPLACE FUNCTION update_job_counters()
RETURNS TRIGGER AS $$
BEGIN
    -- Update job totals when chunk status changes
    IF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        UPDATE migration_jobs
        SET 
            completed_chunks = (
                SELECT COUNT(*) FROM migration_chunks 
                WHERE job_id = NEW.job_id AND status = 'completed'
            ),
            failed_chunks = (
                SELECT COUNT(*) FROM migration_chunks 
                WHERE job_id = NEW.job_id AND status = 'failed'
            )
        WHERE id = NEW.job_id;
        
        -- Update table counters
        UPDATE migration_tables
        SET 
            completed_chunks = (
                SELECT COUNT(*) FROM migration_chunks 
                WHERE table_id = NEW.table_id AND status = 'completed'
            ),
            failed_chunks = (
                SELECT COUNT(*) FROM migration_chunks 
                WHERE table_id = NEW.table_id AND status = 'failed'
            ),
            updated_at = NOW()
        WHERE id = NEW.table_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update counters
CREATE TRIGGER trigger_update_counters
AFTER UPDATE ON migration_chunks
FOR EACH ROW
EXECUTE FUNCTION update_job_counters();

-- Function to detect stale running chunks
CREATE OR REPLACE FUNCTION detect_stale_chunks(heartbeat_threshold_seconds INTEGER DEFAULT 120)
RETURNS TABLE(chunk_id UUID, job_id UUID, table_name VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        job_id,
        table_name::VARCHAR
    FROM migration_chunks
    WHERE status = 'running'
    AND last_heartbeat < NOW() - (heartbeat_threshold_seconds || ' seconds')::INTERVAL;
END;
$$ LANGUAGE plpgsql;
