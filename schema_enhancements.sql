-- ==========================================
-- Production Schema Enhancements
-- Advanced Migration Features
-- ==========================================

-- Add enhanced columns to migration_chunks
ALTER TABLE migration_chunks 
ADD COLUMN IF NOT EXISTS worker_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS source_row_count BIGINT,
ADD COLUMN IF NOT EXISTS target_row_count BIGINT,
ADD COLUMN IF NOT EXISTS validation_status VARCHAR(30) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'validated', 'failed'));

-- Add index for worker assignment queries
CREATE INDEX IF NOT EXISTS idx_chunks_worker ON migration_chunks(worker_id);

-- Add index for retry scheduling
CREATE INDEX IF NOT EXISTS idx_chunks_retry ON migration_chunks(next_retry_at) WHERE status = 'failed' AND retry_count < max_retries;

-- Add columns for failure escalation tracking in migration_jobs
ALTER TABLE migration_jobs
ADD COLUMN IF NOT EXISTS failure_threshold_percent INTEGER DEFAULT 5,
ADD COLUMN IF NOT EXISTS auto_failed_at TIMESTAMP;

-- Create job health view
CREATE OR REPLACE VIEW job_health_view AS
SELECT 
    j.id as job_id,
    j.status,
    j.total_chunks,
    j.completed_chunks,
    j.failed_chunks,
    CASE 
        WHEN j.total_chunks > 0 
        THEN ROUND((j.failed_chunks::DECIMAL / j.total_chunks) * 100, 2)
        ELSE 0 
    END as failure_rate_percent,
    CASE 
        WHEN j.total_chunks > 0 
        THEN ROUND((j.completed_chunks::DECIMAL / j.total_chunks) * 100, 2)
        ELSE 0 
    END as completion_percent,
    j.created_at,
    j.started_at,
    j.completed_at
FROM migration_jobs j;

-- Create chunk execution audit log
CREATE TABLE IF NOT EXISTS chunk_execution_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chunk_id UUID NOT NULL REFERENCES migration_chunks(id) ON DELETE CASCADE,
    worker_id VARCHAR(100),
    attempt_number INTEGER NOT NULL,
    status VARCHAR(30) NOT NULL,
    rows_processed BIGINT DEFAULT 0,
    source_row_count BIGINT,
    target_row_count BIGINT,
    duration_ms BIGINT,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exec_log_chunk ON chunk_execution_log(chunk_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_exec_log_worker ON chunk_execution_log(worker_id, created_at DESC);

COMMENT ON TABLE chunk_execution_log IS 'Audit trail for all chunk execution attempts with retry history';
