import { useState, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import apiClient from '../services/api.js';
import { JobStatus, TableStatus } from '../types/index.js';
import {
  ArrowLeft,
  Database,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  RefreshCw,
  Play,
  TrendingUp,
  Activity,
  ChevronRight,
  BarChart3
} from 'lucide-react';
import { formatDistanceToNow, format } from 'date-fns';

export default function JobDetail() {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const [job, setJob] = useState(null);
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [resuming, setResuming] = useState(false);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 3000); // Refresh every 3 seconds
    return () => clearInterval(interval);
  }, [jobId]);

  const loadData = async () => {
    try {
      const [jobData, tablesData] = await Promise.all([
        apiClient.getJob(jobId),
        apiClient.getJobTables(jobId)
      ]);
      setJob(jobData);
      setTables(tablesData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleResume = async () => {
    setResuming(true);
    try {
      await apiClient.resumeJob(jobId);
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setResuming(false);
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case JobStatus.COMPLETED:
      case TableStatus.COMPLETED:
        return 'text-accent-600 bg-accent-50';
      case JobStatus.RUNNING:
      case TableStatus.RUNNING:
        return 'text-primary-600 bg-primary-50';
      case JobStatus.FAILED:
      case TableStatus.FAILED:
        return 'text-error-600 bg-error-50';
      default:
        return 'text-neutral-600 bg-neutral-50';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case JobStatus.COMPLETED:
      case TableStatus.COMPLETED:
        return <CheckCircle2 className="w-5 h-5" />;
      case JobStatus.RUNNING:
      case TableStatus.RUNNING:
        return <Loader2 className="w-5 h-5 animate-spin" />;
      case JobStatus.FAILED:
      case TableStatus.FAILED:
        return <XCircle className="w-5 h-5" />;
      default:
        return <Clock className="w-5 h-5" />;
    }
  };

  const calculateProgress = (completed, total) => {
    if (total === 0) return 0;
    return Math.round((completed / total) * 100);
  };

  if (loading && !job) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-center py-12">
        <XCircle className="w-16 h-16 text-neutral-300 mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-neutral-900 mb-2">Job not found</h2>
        <button onClick={() => navigate('/')} className="btn-primary mt-4">
          Back to Dashboard
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Back Navigation */}
      <button
        onClick={() => navigate('/')}
        className="flex items-center space-x-2 text-neutral-600 hover:text-neutral-900 transition-colors"
      >
        <ArrowLeft className="w-4 h-4" />
        <span>Back to Dashboard</span>
      </button>

      {/* Job Header */}
      <div className="card p-6">
        <div className="flex items-start justify-between mb-6">
          <div>
            <div className="flex items-center space-x-3 mb-2">
              <span className={`inline-flex items-center space-x-2 px-4 py-2 rounded-lg text-sm font-medium ${getStatusColor(job.status)}`}>
                {getStatusIcon(job.status)}
                <span>{job.status}</span>
              </span>
              <span className="text-sm text-neutral-500">
                Created {formatDistanceToNow(new Date(job.created_at), { addSuffix: true })}
              </span>
            </div>
            <h1 className="text-2xl font-bold text-neutral-900 mb-1">Migration Job</h1>
            <p className="text-neutral-600 font-mono text-sm">ID: {job.id}</p>
          </div>

          <div className="flex items-center space-x-3">
            <Link
              to={`/jobs/${jobId}/performance`}
              className="btn-secondary inline-flex items-center space-x-2"
            >
              <BarChart3 className="w-4 h-4" />
              <span>Performance Metrics</span>
            </Link>

            {(job.status === JobStatus.FAILED || job.status === JobStatus.PAUSED) && (
              <button
                onClick={handleResume}
                disabled={resuming}
                className="btn-accent inline-flex items-center space-x-2"
              >
                {resuming ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    <span>Resuming...</span>
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4" />
                    <span>Resume Job</span>
                  </>
                )}
              </button>
            )}
          </div>
        </div>

        {/* Progress Bar */}
        {job.status === JobStatus.RUNNING && (
          <div className="mb-6">
            <div className="flex items-center justify-between text-sm mb-2">
              <span className="text-neutral-600">Overall Progress</span>
              <span className="font-semibold text-neutral-900">
                {calculateProgress(job.completed_chunks, job.total_chunks)}%
              </span>
            </div>
            <div className="h-3 bg-neutral-100 rounded-full overflow-hidden">
              <div
                className="h-full bg-gradient-to-r from-primary-600 via-primary-500 to-accent-500 transition-all duration-500"
                style={{ width: `${calculateProgress(job.completed_chunks, job.total_chunks)}%` }}
              />
            </div>
          </div>
        )}

        {/* Database Info */}
        <div className="grid grid-cols-2 gap-6">
          <div>
            <p className="text-xs font-medium text-neutral-500 mb-2">SOURCE DATABASE</p>
            <div className="flex items-start space-x-3">
              <div className="w-10 h-10 bg-primary-50 rounded-lg flex items-center justify-center flex-shrink-0">
                <Database className="w-5 h-5 text-primary-600" />
              </div>
              <div>
                <p className="font-semibold text-neutral-900">{job.source_config.database}</p>
                <p className="text-sm text-neutral-600">{job.source_config.host}:{job.source_config.port}</p>
                <p className="text-xs text-neutral-500 mt-1">User: {job.source_config.user}</p>
              </div>
            </div>
          </div>

          <div>
            <p className="text-xs font-medium text-neutral-500 mb-2">TARGET DATABASE</p>
            <div className="flex items-start space-x-3">
              <div className="w-10 h-10 bg-accent-50 rounded-lg flex items-center justify-center flex-shrink-0">
                <Database className="w-5 h-5 text-accent-600" />
              </div>
              <div>
                <p className="font-semibold text-neutral-900">{job.target_config.database}</p>
                <p className="text-sm text-neutral-600">{job.target_config.host}:{job.target_config.port}</p>
                <p className="text-xs text-neutral-500 mt-1">User: {job.target_config.user}</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Statistics Grid */}
      <div className="grid grid-cols-4 gap-4">
        <StatCard
          label="Total Tables"
          value={job.total_tables}
          icon={<Database className="w-5 h-5 text-neutral-600" />}
        />
        <StatCard
          label="Completed Tables"
          value={job.completed_tables}
          icon={<CheckCircle2 className="w-5 h-5 text-accent-600" />}
        />
        <StatCard
          label="Total Chunks"
          value={job.total_chunks}
          icon={<Activity className="w-5 h-5 text-primary-600" />}
        />
        <StatCard
          label="Migrated Rows"
          value={job.migrated_rows.toLocaleString()}
          icon={<TrendingUp className="w-5 h-5 text-accent-600" />}
        />
      </div>

      {/* Error Message */}
      {job.error_message && (
        <div className="bg-error-50 border border-error-200 rounded-lg p-4">
          <div className="flex items-start space-x-3">
            <XCircle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
            <div>
              <h3 className="text-sm font-medium text-error-800 mb-1">Error</h3>
              <p className="text-sm text-error-700">{job.error_message}</p>
            </div>
          </div>
        </div>
      )}

      {/* Tables List */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-neutral-900">Tables</h2>
          <button
            onClick={loadData}
            className="btn-secondary inline-flex items-center space-x-2"
          >
            <RefreshCw className="w-4 h-4" />
            <span>Refresh</span>
          </button>
        </div>

        {tables.length === 0 ? (
          <div className="card p-8 text-center">
            <Loader2 className="w-8 h-8 text-neutral-300 animate-spin mx-auto mb-3" />
            <p className="text-neutral-600">Loading tables...</p>
          </div>
        ) : (
          <div className="space-y-3">
            {tables.map((table) => (
              <Link
                key={table.id}
                to={`/jobs/${jobId}/tables/${table.id}`}
                className="card p-5 hover:shadow-medium transition-all duration-200 block"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4 flex-1">
                    <span className={`inline-flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(table.status)}`}>
                      {getStatusIcon(table.status)}
                      <span>{table.status}</span>
                    </span>

                    <div className="flex-1">
                      <p className="font-semibold text-neutral-900 mb-1">{table.table_name}</p>
                      <div className="flex items-center space-x-4 text-sm text-neutral-600">
                        <span>{table.total_rows.toLocaleString()} rows</span>
                        <span>•</span>
                        <span>{table.completed_chunks}/{table.total_chunks} chunks</span>
                        {table.failed_chunks > 0 && (
                          <>
                            <span>•</span>
                            <span className="text-error-600">{table.failed_chunks} failed</span>
                          </>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Progress Bar for Running Tables */}
                  {table.status === TableStatus.RUNNING && (
                    <div className="w-48 mr-4">
                      <div className="h-2 bg-neutral-100 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-primary-600 transition-all duration-500"
                          style={{ width: `${calculateProgress(table.completed_chunks, table.total_chunks)}%` }}
                        />
                      </div>
                    </div>
                  )}

                  <ChevronRight className="w-5 h-5 text-neutral-400" />
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>

      {/* Timestamps */}
      {(job.started_at || job.completed_at) && (
        <div className="card p-5">
          <div className="grid grid-cols-3 gap-6 text-sm">
            <div>
              <p className="text-neutral-500 mb-1">Created At</p>
              <p className="font-medium text-neutral-900">
                {format(new Date(job.created_at), 'PPpp')}
              </p>
            </div>
            {job.started_at && (
              <div>
                <p className="text-neutral-500 mb-1">Started At</p>
                <p className="font-medium text-neutral-900">
                  {format(new Date(job.started_at), 'PPpp')}
                </p>
              </div>
            )}
            {job.completed_at && (
              <div>
                <p className="text-neutral-500 mb-1">Completed At</p>
                <p className="font-medium text-neutral-900">
                  {format(new Date(job.completed_at), 'PPpp')}
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function StatCard({ label, value, icon }) {
  return (
    <div className="card p-4">
      <div className="flex items-center justify-between mb-2">
        {icon}
        <p className="text-2xl font-bold text-neutral-900">{value}</p>
      </div>
      <p className="text-xs text-neutral-600">{label}</p>
    </div>
  );
}
