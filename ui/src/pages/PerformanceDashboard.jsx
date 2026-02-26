import { useState, useEffect } from 'react';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Activity, TrendingUp, Users, Clock, AlertCircle, CheckCircle2, Zap } from 'lucide-react';
import { format } from 'date-fns';
import apiClient from '../services/api.js';

export default function PerformanceDashboard() {
  const [realtimeMetrics, setRealtimeMetrics] = useState(null);
  const [activeWorkers, setActiveWorkers] = useState([]);
  const [throughputHistory, setThroughputHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadMetrics();
    const interval = setInterval(loadMetrics, 5000);
    return () => clearInterval(interval);
  }, []);

  const loadMetrics = async () => {
    try {
      const [realtime, workers] = await Promise.all([
        apiClient.getRealtimePerformance(),
        apiClient.getActiveWorkers()
      ]);

      setRealtimeMetrics(realtime);
      setActiveWorkers(workers);

      // Add current metrics to history for charts
      setThroughputHistory(prev => {
        const newEntry = {
          timestamp: new Date().toLocaleTimeString(),
          rowsPerSecond: realtime.rows_per_second,
          chunksPerMinute: realtime.chunks_per_minute,
          activeWorkers: realtime.active_workers
        };
        const updated = [...prev, newEntry].slice(-20); // Keep last 20 data points
        return updated;
      });

      setLoading(false);
      setError(null);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  if (loading && !realtimeMetrics) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-neutral-600">Loading performance metrics...</p>
        </div>
      </div>
    );
  }

  if (error && !realtimeMetrics) {
    return (
      <div className="card bg-error-50 border-error-200 p-6">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-6 h-6 text-error-600 flex-shrink-0" />
          <div>
            <h3 className="font-semibold text-error-900">Failed to load metrics</h3>
            <p className="text-sm text-error-700 mt-1">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  const getWorkerStatusColor = (worker) => {
    const lastHeartbeat = new Date(worker.last_heartbeat);
    const now = new Date();
    const diffSeconds = (now - lastHeartbeat) / 1000;

    if (diffSeconds < 30) return 'text-accent-600';
    if (diffSeconds < 120) return 'text-warning-600';
    return 'text-error-600';
  };

  const getWorkerStatusBadge = (worker) => {
    const lastHeartbeat = new Date(worker.last_heartbeat);
    const now = new Date();
    const diffSeconds = (now - lastHeartbeat) / 1000;

    if (diffSeconds < 30) {
      return <span className="badge-success">Active</span>;
    } else if (diffSeconds < 120) {
      return <span className="badge-warning">Stale</span>;
    } else {
      return <span className="badge-error">Inactive</span>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-neutral-900">Performance Dashboard</h1>
        <p className="text-neutral-600 mt-2">Real-time monitoring and analytics</p>
      </div>

      {/* Realtime Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="card p-6">
          <div className="flex items-center justify-between mb-3">
            <div className="w-12 h-12 bg-primary-100 rounded-xl flex items-center justify-center">
              <TrendingUp className="w-6 h-6 text-primary-600" />
            </div>
            <span className="text-sm text-accent-600 font-medium">Live</span>
          </div>
          <p className="text-sm text-neutral-600 mb-1">Throughput</p>
          <p className="text-3xl font-bold text-neutral-900">
            {realtimeMetrics?.rows_per_second?.toLocaleString() || 0}
          </p>
          <p className="text-sm text-neutral-500 mt-1">rows/second</p>
        </div>

        <div className="card p-6">
          <div className="flex items-center justify-between mb-3">
            <div className="w-12 h-12 bg-accent-100 rounded-xl flex items-center justify-center">
              <Zap className="w-6 h-6 text-accent-600" />
            </div>
            <span className="text-sm text-accent-600 font-medium">Live</span>
          </div>
          <p className="text-sm text-neutral-600 mb-1">Chunks Processed</p>
          <p className="text-3xl font-bold text-neutral-900">
            {realtimeMetrics?.chunks_per_minute || 0}
          </p>
          <p className="text-sm text-neutral-500 mt-1">chunks/minute</p>
        </div>

        <div className="card p-6">
          <div className="flex items-center justify-between mb-3">
            <div className="w-12 h-12 bg-indigo-100 rounded-xl flex items-center justify-center">
              <Users className="w-6 h-6 text-indigo-600" />
            </div>
            <span className="text-sm text-accent-600 font-medium">Live</span>
          </div>
          <p className="text-sm text-neutral-600 mb-1">Active Workers</p>
          <p className="text-3xl font-bold text-neutral-900">
            {realtimeMetrics?.active_workers || 0}
          </p>
          <p className="text-sm text-neutral-500 mt-1">processing now</p>
        </div>

        <div className="card p-6">
          <div className="flex items-center justify-between mb-3">
            <div className="w-12 h-12 bg-orange-100 rounded-xl flex items-center justify-center">
              <Clock className="w-6 h-6 text-orange-600" />
            </div>
            <span className="text-sm text-neutral-600 font-medium">Queue</span>
          </div>
          <p className="text-sm text-neutral-600 mb-1">Queue Depth</p>
          <p className="text-3xl font-bold text-neutral-900">
            {realtimeMetrics?.queue_depth || 0}
          </p>
          <p className="text-sm text-neutral-500 mt-1">chunks pending</p>
        </div>
      </div>

      {/* Throughput Chart */}
      <div className="card p-6">
        <h2 className="text-xl font-semibold text-neutral-900 mb-6">Throughput Over Time</h2>
        {throughputHistory.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={throughputHistory}>
              <defs>
                <linearGradient id="colorRows" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorChunks" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="timestamp" 
                stroke="#6b7280"
                style={{ fontSize: '0.75rem' }}
              />
              <YAxis 
                stroke="#6b7280"
                style={{ fontSize: '0.75rem' }}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#ffffff',
                  border: '1px solid #e5e7eb',
                  borderRadius: '0.5rem',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                }}
              />
              <Legend />
              <Area 
                type="monotone" 
                dataKey="rowsPerSecond" 
                stroke="#6366f1" 
                fillOpacity={1}
                fill="url(#colorRows)"
                name="Rows/Second"
              />
              <Area 
                type="monotone" 
                dataKey="chunksPerMinute" 
                stroke="#10b981" 
                fillOpacity={1}
                fill="url(#colorChunks)"
                name="Chunks/Minute"
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-300 flex items-center justify-center text-neutral-500">
            No data available yet. Metrics will appear once migrations start.
          </div>
        )}
      </div>

      {/* Worker Status */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-neutral-900">Worker Pool Status</h2>
          <span className="badge-primary">
            {activeWorkers.length} Workers
          </span>
        </div>

        {activeWorkers.length > 0 ? (
          <div className="space-y-4">
            {activeWorkers.map((worker) => (
              <div key={worker.worker_id} className="border border-neutral-200 rounded-lg p-4 hover:border-primary-300 transition-colors">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center space-x-3">
                    <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                      getWorkerStatusColor(worker) === 'text-accent-600' ? 'bg-accent-100' :
                      getWorkerStatusColor(worker) === 'text-warning-600' ? 'bg-warning-100' : 'bg-error-100'
                    }`}>
                      <Activity className={`w-5 h-5 ${getWorkerStatusColor(worker)}`} />
                    </div>
                    <div>
                      <p className="font-semibold text-neutral-900">{worker.worker_id}</p>
                      <p className="text-sm text-neutral-600">
                        Processing: {worker.current_job_id ? `Job #${worker.current_job_id}` : 'Idle'}
                      </p>
                    </div>
                  </div>
                  {getWorkerStatusBadge(worker)}
                </div>

                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-neutral-600">Last Heartbeat</p>
                    <p className="font-medium text-neutral-900">
                      {format(new Date(worker.last_heartbeat), 'HH:mm:ss')}
                    </p>
                  </div>
                  <div>
                    <p className="text-neutral-600">Chunks Processed</p>
                    <p className="font-medium text-neutral-900">{worker.chunks_processed || 0}</p>
                  </div>
                  <div>
                    <p className="text-neutral-600">Status</p>
                    <p className="font-medium text-neutral-900">{worker.status || 'Active'}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-12">
            <Users className="w-12 h-12 text-neutral-400 mx-auto mb-3" />
            <p className="text-neutral-600">No active workers</p>
            <p className="text-sm text-neutral-500 mt-1">Workers will appear when processing jobs</p>
          </div>
        )}
      </div>
    </div>
  );
}
