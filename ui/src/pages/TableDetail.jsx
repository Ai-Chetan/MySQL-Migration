import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import apiClient from '../services/api.js';
import { ChunkStatus } from '../types/index.js';
import {
  ArrowLeft,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  RefreshCw,
  AlertTriangle,
  TrendingUp
} from 'lucide-react';
import { formatDistanceToNow, format } from 'date-fns';

export default function TableDetail() {
  const { jobId, tableId } = useParams();
  const navigate = useNavigate();
  const [table, setTable] = useState(null);
  const [chunks, setChunks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [retryingChunks, setRetryingChunks] = useState(new Set());

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 3000); // Refresh every 3 seconds
    return () => clearInterval(interval);
  }, [jobId, tableId]);

  const loadData = async () => {
    try {
      const [tablesData, chunksData] = await Promise.all([
        apiClient.getJobTables(jobId),
        apiClient.getTableChunks(jobId, tableId)
      ]);
      
      const tableData = tablesData.find(t => t.id === tableId);
      setTable(tableData);
      setChunks(chunksData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRetryChunk = async (chunkId) => {
    setRetryingChunks(prev => new Set(prev).add(chunkId));
    try {
      await apiClient.retryChunk(jobId, chunkId);
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setRetryingChunks(prev => {
        const next = new Set(prev);
        next.delete(chunkId);
        return next;
      });
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case ChunkStatus.COMPLETED:
        return 'text-accent-600 bg-accent-50 border-accent-200';
      case ChunkStatus.RUNNING:
        return 'text-primary-600 bg-primary-50 border-primary-200';
      case ChunkStatus.FAILED:
        return 'text-error-600 bg-error-50 border-error-200';
      default:
        return 'text-neutral-600 bg-neutral-50 border-neutral-200';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case ChunkStatus.COMPLETED:
        return <CheckCircle2 className="w-4 h-4" />;
      case ChunkStatus.RUNNING:
        return <Loader2 className="w-4 h-4 animate-spin" />;
      case ChunkStatus.FAILED:
        return <XCircle className="w-4 h-4" />;
      default:
        return <Clock className="w-4 h-4" />;
    }
  };

  const calculateProgress = () => {
    if (!table || table.total_chunks === 0) return 0;
    return Math.round((table.completed_chunks / table.total_chunks) * 100);
  };

  const getChunksByStatus = (status) => {
    return chunks.filter(c => c.status === status).length;
  };

  if (loading && !table) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  if (!table) {
    return (
      <div className="text-center py-12">
        <XCircle className="w-16 h-16 text-neutral-300 mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-neutral-900 mb-2">Table not found</h2>
        <button onClick={() => navigate(`/jobs/${jobId}`)} className="btn-primary mt-4">
          Back to Job
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Back Navigation */}
      <button
        onClick={() => navigate(`/jobs/${jobId}`)}
        className="flex items-center space-x-2 text-neutral-600 hover:text-neutral-900 transition-colors"
      >
        <ArrowLeft className="w-4 h-4" />
        <span>Back to Job</span>
      </button>

      {/* Table Header */}
      <div className="card p-6">
        <div className="flex items-start justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-neutral-900 mb-2">{table.table_name}</h1>
            <p className="text-neutral-600 text-sm">
              Primary Key: <span className="font-mono font-medium">{table.primary_key_column}</span>
            </p>
          </div>

          <button
            onClick={loadData}
            className="btn-secondary inline-flex items-center space-x-2"
          >
            <RefreshCw className="w-4 h-4" />
            <span>Refresh</span>
          </button>
        </div>

        {/* Progress Bar */}
        <div className="mb-6">
          <div className="flex items-center justify-between text-sm mb-2">
            <span className="text-neutral-600">Migration Progress</span>
            <span className="font-semibold text-neutral-900">{calculateProgress()}%</span>
          </div>
          <div className="h-3 bg-neutral-100 rounded-full overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-primary-600 to-accent-500 transition-all duration-500"
              style={{ width: `${calculateProgress()}%` }}
            />
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-neutral-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-neutral-900 mb-1">
              {table.total_rows.toLocaleString()}
            </p>
            <p className="text-xs text-neutral-600">Total Rows</p>
          </div>
          <div className="bg-accent-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-accent-600 mb-1">{table.completed_chunks}</p>
            <p className="text-xs text-neutral-600">Completed Chunks</p>
          </div>
          <div className="bg-primary-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-primary-600 mb-1">
              {getChunksByStatus(ChunkStatus.RUNNING)}
            </p>
            <p className="text-xs text-neutral-600">Running Chunks</p>
          </div>
          <div className="bg-error-50 rounded-lg p-4">
            <p className="text-2xl font-bold text-error-600 mb-1">{table.failed_chunks}</p>
            <p className="text-xs text-neutral-600">Failed Chunks</p>
          </div>
        </div>
      </div>

      {/* Error Message */}
      {table.error_message && (
        <div className="bg-error-50 border border-error-200 rounded-lg p-4">
          <div className="flex items-start space-x-3">
            <AlertTriangle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
            <div>
              <h3 className="text-sm font-medium text-error-800 mb-1">Table Error</h3>
              <p className="text-sm text-error-700">{table.error_message}</p>
            </div>
          </div>
        </div>
      )}

      {/* Chunks List */}
      <div>
        <h2 className="text-xl font-semibold text-neutral-900 mb-4">
          Chunks ({chunks.length})
        </h2>

        {chunks.length === 0 ? (
          <div className="card p-8 text-center">
            <Loader2 className="w-8 h-8 text-neutral-300 animate-spin mx-auto mb-3" />
            <p className="text-neutral-600">Loading chunks...</p>
          </div>
        ) : (
          <div className="space-y-2">
            {chunks.map((chunk) => (
              <div
                key={chunk.id}
                className={`card p-4 border ${getStatusColor(chunk.status)}`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4 flex-1">
                    {/* Status */}
                    <span className="inline-flex items-center space-x-1.5 text-xs font-medium">
                      {getStatusIcon(chunk.status)}
                      <span>{chunk.status}</span>
                    </span>

                    {/* Chunk Info */}
                    <div className="flex-1">
                      <p className="text-sm font-medium text-neutral-900 mb-1">
                        Chunk #{chunk.chunk_number}
                        <span className="text-neutral-500 font-normal ml-2">
                          PK Range: {chunk.start_pk} - {chunk.end_pk}
                        </span>
                      </p>
                      <div className="flex items-center space-x-4 text-xs text-neutral-600">
                        <span>Est. {chunk.estimated_rows.toLocaleString()} rows</span>
                        {chunk.actual_rows && (
                          <>
                            <span>•</span>
                            <span>Actual: {chunk.actual_rows.toLocaleString()}</span>
                          </>
                        )}
                        {chunk.retry_count > 0 && (
                          <>
                            <span>•</span>
                            <span className="text-warning-600">Retry: {chunk.retry_count}</span>
                          </>
                        )}
                        {chunk.worker_id && (
                          <>
                            <span>•</span>
                            <span className="font-mono">Worker: {chunk.worker_id.slice(0, 8)}</span>
                          </>
                        )}
                      </div>
                    </div>

                    {/* Timing */}
                    {chunk.started_at && (
                      <div className="text-xs text-neutral-600">
                        {chunk.completed_at ? (
                          <span>
                            Completed {formatDistanceToNow(new Date(chunk.completed_at), { addSuffix: true })}
                          </span>
                        ) : chunk.status === ChunkStatus.RUNNING ? (
                          <span>
                            Running for {formatDistanceToNow(new Date(chunk.started_at))}
                          </span>
                        ) : (
                          <span>
                            Started {formatDistanceToNow(new Date(chunk.started_at), { addSuffix: true })}
                          </span>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Actions */}
                  {chunk.status === ChunkStatus.FAILED && (
                    <button
                      onClick={() => handleRetryChunk(chunk.id)}
                      disabled={retryingChunks.has(chunk.id)}
                      className="btn-secondary text-xs px-3 py-1.5 inline-flex items-center space-x-1.5"
                    >
                      {retryingChunks.has(chunk.id) ? (
                        <>
                          <Loader2 className="w-3 h-3 animate-spin" />
                          <span>Retrying...</span>
                        </>
                      ) : (
                        <>
                          <RefreshCw className="w-3 h-3" />
                          <span>Retry</span>
                        </>
                      )}
                    </button>
                  )}
                </div>

                {/* Error Message */}
                {chunk.error_message && (
                  <div className="mt-3 pt-3 border-t border-error-200">
                    <p className="text-xs text-error-700 font-mono">{chunk.error_message}</p>
                  </div>
                )}

                {/* Heartbeat Warning */}
                {chunk.status === ChunkStatus.RUNNING && chunk.last_heartbeat && (
                  <div className="mt-2">
                    <p className="text-xs text-neutral-500">
                      Last heartbeat: {formatDistanceToNow(new Date(chunk.last_heartbeat), { addSuffix: true })}
                    </p>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Timestamps */}
      {(table.started_at || table.completed_at) && (
        <div className="card p-4">
          <div className="grid grid-cols-3 gap-6 text-sm">
            <div>
              <p className="text-neutral-500 mb-1">Created</p>
              <p className="font-medium text-neutral-900 text-xs">
                {format(new Date(table.created_at), 'PPpp')}
              </p>
            </div>
            {table.started_at && (
              <div>
                <p className="text-neutral-500 mb-1">Started</p>
                <p className="font-medium text-neutral-900 text-xs">
                  {format(new Date(table.started_at), 'PPpp')}
                </p>
              </div>
            )}
            {table.completed_at && (
              <div>
                <p className="text-neutral-500 mb-1">Completed</p>
                <p className="font-medium text-neutral-900 text-xs">
                  {format(new Date(table.completed_at), 'PPpp')}
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
