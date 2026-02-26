import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import apiClient from '../services/api.js';
import { JobStatus } from '../types/index.js';
import { 
  Database, 
  Clock, 
  CheckCircle2, 
  XCircle, 
  Loader2,
  TrendingUp,
  Server,
  Activity
} from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

export default function Dashboard() {
  const [jobs, setJobs] = useState([]);
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const loadData = async () => {
    try {
      const [jobsData, metricsData] = await Promise.all([
        apiClient.listJobs(),
        apiClient.getMetrics()
      ]);
      setJobs(jobsData);
      setMetrics(metricsData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case JobStatus.COMPLETED:
        return 'text-accent-600 bg-accent-50';
      case JobStatus.RUNNING:
        return 'text-primary-600 bg-primary-50';
      case JobStatus.FAILED:
        return 'text-error-600 bg-error-50';
      case JobStatus.PENDING:
        return 'text-neutral-600 bg-neutral-50';
      default:
        return 'text-neutral-600 bg-neutral-50';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case JobStatus.COMPLETED:
        return <CheckCircle2 className="w-4 h-4" />;
      case JobStatus.RUNNING:
        return <Loader2 className="w-4 h-4 animate-spin" />;
      case JobStatus.FAILED:
        return <XCircle className="w-4 h-4" />;
      default:
        return <Clock className="w-4 h-4" />;
    }
  };

  const calculateProgress = (job) => {
    if (job.total_chunks === 0) return 0;
    return Math.round((job.completed_chunks / job.total_chunks) * 100);
  };

  if (loading && jobs.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Page Header */}
      <div>
        <h1 className="text-3xl font-bold text-neutral-900">Dashboard</h1>
        <p className="text-neutral-600 mt-2">Monitor and manage your database migrations</p>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="bg-error-50 border border-error-200 rounded-lg p-4">
          <div className="flex items-center space-x-2">
            <XCircle className="w-5 h-5 text-error-600" />
            <p className="text-error-800 font-medium">Error loading data: {error}</p>
          </div>
        </div>
      )}

      {/* Metrics Cards */}
      {metrics && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <MetricCard
            icon={<Database className="w-6 h-6 text-primary-600" />}
            label="Total Jobs"
            value={metrics.total_jobs}
            bgColor="bg-primary-50"
          />
          <MetricCard
            icon={<Activity className="w-6 h-6 text-accent-600" />}
            label="Running Jobs"
            value={metrics.running_jobs}
            bgColor="bg-accent-50"
          />
          <MetricCard
            icon={<Server className="w-6 h-6 text-neutral-600" />}
            label="Active Workers"
            value={metrics.active_workers}
            bgColor="bg-neutral-50"
          />
          <MetricCard
            icon={<TrendingUp className="w-6 h-6 text-primary-600" />}
            label="Queue Length"
            value={metrics.queue_length}
            bgColor="bg-primary-50"
          />
        </div>
      )}

      {/* Jobs List */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-neutral-900">Migration Jobs</h2>
          <Link to="/jobs/new" className="btn-primary">
            Create New Migration
          </Link>
        </div>

        {jobs.length === 0 ? (
          <div className="card p-12 text-center">
            <Database className="w-16 h-16 text-neutral-300 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-neutral-900 mb-2">No migrations yet</h3>
            <p className="text-neutral-600 mb-6">Create your first migration job to get started</p>
            <Link to="/jobs/new" className="btn-primary inline-block">
              Create Migration Job
            </Link>
          </div>
        ) : (
          <div className="space-y-4">
            {jobs.map((job) => (
              <Link
                key={job.id}
                to={`/jobs/${job.id}`}
                className="card p-6 hover:shadow-medium transition-all duration-200 block"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center space-x-3 mb-2">
                      <span className={`inline-flex items-center space-x-1.5 px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(job.status)}`}>
                        {getStatusIcon(job.status)}
                        <span>{job.status}</span>
                      </span>
                      <span className="text-sm text-neutral-500">
                        {formatDistanceToNow(new Date(job.created_at), { addSuffix: true })}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4 mb-4">
                      <div>
                        <p className="text-xs text-neutral-500 mb-1">Source Database</p>
                        <p className="text-sm font-medium text-neutral-900">
                          {job.source_config.database}@{job.source_config.host}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-neutral-500 mb-1">Target Database</p>
                        <p className="text-sm font-medium text-neutral-900">
                          {job.target_config.database}@{job.target_config.host}
                        </p>
                      </div>
                    </div>

                    {/* Progress Bar */}
                    {job.status === JobStatus.RUNNING && (
                      <div className="mb-3">
                        <div className="flex items-center justify-between text-xs text-neutral-600 mb-1">
                          <span>Progress</span>
                          <span className="font-medium">{calculateProgress(job)}%</span>
                        </div>
                        <div className="h-2 bg-neutral-100 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-gradient-to-r from-primary-600 to-primary-500 transition-all duration-500"
                            style={{ width: `${calculateProgress(job)}%` }}
                          />
                        </div>
                      </div>
                    )}

                    {/* Stats */}
                    <div className="flex items-center space-x-6 text-sm">
                      <div className="flex items-center space-x-2">
                        <Database className="w-4 h-4 text-neutral-400" />
                        <span className="text-neutral-600">
                          {job.completed_tables}/{job.total_tables} tables
                        </span>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Activity className="w-4 h-4 text-neutral-400" />
                        <span className="text-neutral-600">
                          {job.completed_chunks}/{job.total_chunks} chunks
                        </span>
                      </div>
                      {job.migrated_rows > 0 && (
                        <div className="flex items-center space-x-2">
                          <TrendingUp className="w-4 h-4 text-neutral-400" />
                          <span className="text-neutral-600">
                            {job.migrated_rows.toLocaleString()} rows
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function MetricCard({ icon, label, value, bgColor }) {
  return (
    <div className="card p-6">
      <div className={`w-12 h-12 ${bgColor} rounded-lg flex items-center justify-center mb-4`}>
        {icon}
      </div>
      <p className="text-2xl font-bold text-neutral-900 mb-1">{value}</p>
      <p className="text-sm text-neutral-600">{label}</p>
    </div>
  );
}
