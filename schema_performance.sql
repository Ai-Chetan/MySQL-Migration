-- ==========================================
-- Performance Optimization Schema
-- High-throughput migration features
-- ==========================================

-- Add performance tracking columns to migration_chunks
ALTER TABLE migration_chunks 
ADD COLUMN IF NOT EXISTS throughput_rows_per_sec DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS throughput_mb_per_sec DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS memory_peak_mb INTEGER,
ADD COLUMN IF NOT EXISTS insert_latency_ms INTEGER,
ADD COLUMN IF NOT EXISTS batch_size_used INTEGER DEFAULT 5000,
ADD COLUMN IF NOT EXISTS bulk_insert_method VARCHAR(50) DEFAULT 'standard';

-- Add performance tracking to jobs
ALTER TABLE migration_jobs
ADD COLUMN IF NOT EXISTS avg_throughput_rows_per_sec DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS peak_memory_mb INTEGER,
ADD COLUMN IF NOT EXISTS total_bytes_migrated BIGINT,
ADD COLUMN IF NOT EXISTS optimization_method VARCHAR(100) DEFAULT 'standard';

-- Create performance metrics table for time-series tracking
CREATE TABLE IF NOT EXISTS performance_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    metric_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Throughput metrics
    rows_per_second DECIMAL(15,2),
    mb_per_second DECIMAL(10,2),
    chunks_per_minute INTEGER,
    
    -- Resource metrics
    active_workers INTEGER,
    queue_depth INTEGER,
    memory_usage_mb INTEGER,
    cpu_usage_percent DECIMAL(5,2),
    
    -- Database metrics
    source_db_latency_ms INTEGER,
    target_db_latency_ms INTEGER,
    insert_latency_ms INTEGER,
    
    -- Worker metrics
    worker_id VARCHAR(100),
    current_batch_size INTEGER,
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_perf_metrics_job ON performance_metrics(job_id, metric_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_perf_metrics_timestamp ON performance_metrics(metric_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_perf_metrics_worker ON performance_metrics(worker_id, metric_timestamp DESC);

-- Table constraints tracking (for optimization) 
CREATE TABLE IF NOT EXISTS table_constraints_backup (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    constraint_type VARCHAR(50) NOT NULL, -- 'foreign_key', 'index', 'check', 'unique'
    constraint_name VARCHAR(255) NOT NULL,
    constraint_definition TEXT NOT NULL,
    dropped_at TIMESTAMP,
    restored_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_constraints_backup_job ON table_constraints_backup(job_id, table_name);

-- Adaptive batch size history
CREATE TABLE IF NOT EXISTS batch_size_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id VARCHAR(100) NOT NULL,
    chunk_id UUID REFERENCES migration_chunks(id) ON DELETE CASCADE,
    old_batch_size INTEGER NOT NULL,
    new_batch_size INTEGER NOT NULL,
    avg_latency_ms INTEGER NOT NULL,
    target_latency_ms INTEGER NOT NULL,
    adjustment_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_history_worker ON batch_size_history(worker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_batch_history_chunk ON batch_size_history(chunk_id);

-- Real-time performance view
CREATE OR REPLACE VIEW realtime_performance AS
SELECT 
    j.id as job_id,
    j.status,
    -- Current throughput
    ROUND(AVG(c.throughput_rows_per_sec), 2) as current_avg_throughput,
    MAX(c.throughput_rows_per_sec) as peak_throughput,
    -- Progress
    j.completed_chunks,
    j.total_chunks,
    ROUND((j.completed_chunks::DECIMAL / NULLIF(j.total_chunks, 0)) * 100, 2) as completion_percent,
    -- Resources
    MAX(c.memory_peak_mb) as peak_memory_mb,
    AVG(c.insert_latency_ms) as avg_insert_latency_ms,
    -- Active processing
    COUNT(CASE WHEN c.status = 'running' THEN 1 END) as currently_running,
    -- Time estimates
    CASE 
        WHEN j.completed_chunks > 0 AND j.started_at IS NOT NULL THEN
            ROUND(EXTRACT(EPOCH FROM (NOW() - j.started_at)) / 60, 1)
        ELSE 0 
    END as elapsed_minutes,
    CASE 
        WHEN j.completed_chunks > 0 AND j.started_at IS NOT NULL THEN
            ROUND(
                (EXTRACT(EPOCH FROM (NOW() - j.started_at)) / j.completed_chunks) * 
                (j.total_chunks - j.completed_chunks) / 60,
                1
            )
        ELSE NULL
    END as eta_minutes
FROM migration_jobs j
LEFT JOIN migration_chunks c ON c.job_id = j.id
WHERE j.status IN ('running', 'planning')
GROUP BY j.id, j.status, j.completed_chunks, j.total_chunks, j.started_at;

COMMENT ON TABLE performance_metrics IS 'Time-series performance tracking for observability';
COMMENT ON TABLE table_constraints_backup IS 'Backup of dropped constraints for bulk insert optimization';
COMMENT ON TABLE batch_size_history IS 'Adaptive batch sizing history for performance tuning';
