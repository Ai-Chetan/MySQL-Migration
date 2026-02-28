import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { 
  LineChart, Line, AreaChart, Area, BarChart, Bar, 
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts';
import { 
  Activity, TrendingUp, Users, Clock, AlertCircle, 
  CheckCircle2, Zap, Database, Gauge, Memory, ArrowLeft 
} from 'lucide-react';
import { format } from 'date-fns';
import apiClient from '../services/api.js';

export default function PerformanceMetricsPage() {
  const { jobId } = useParams();
  const navigate = useNavigate();
  
  const [realtimeMetrics, setRealtimeMetrics] = useState(null);
  const [workerStats, setWorkerStats] = useState([]);
  const [throughputHistory, setThroughputHistory] = useState([]);
  const [batchSizeHistory, setBatchSizeHistory] = useState([]);
  const [constraintStatus, setConstraintStatus] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedView, setSelectedView] = useState('overview'); // overview, workers, batches, constraints

  useEffect(() => {
    if (jobId) {
      loadMetrics();
      const interval = setInterval(loadMetrics, 3000); // Refresh every 3 seconds
      return () => clearInterval(interval);
    }
  }, [jobId]);

  const loadMetrics = async () => {
    try {
      const [realtime, workers, history, batches, constraints] = await Promise.all([
        apiClient.getPerformanceRealtime(jobId),
        apiClient.getWorkerStats(jobId),
        apiClient.getPerformanceHistory(jobId, 1),
        apiClient.getBatchSizeHistory(jobId),
        apiClient.getConstraintStatus(jobId)
      ]);

      setRealtimeMetrics(realtime);
      setWorkerStats(workers);
      setThroughputHistory(history.map(item => ({
        ...item,
        time: format(new Date(item.timestamp), 'HH:mm:ss')
      })));
      setBatchSizeHistory(batches);
      setConstraintStatus(constraints);
      setLoading(false);
      setError(null);
    } catch (err) {
      console.error('Failed to load metrics:', err);
      setError(err.message);
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-neutral-600">Loading performance metrics...</p>
        </div>
      </div>
    );
  }

  if (error) {
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

  const getTrendIndicator = (trend) => {
    if (trend === 'increasing') return <TrendingUp className="w-4 h-4 text-accent-600" />;
    if (trend === 'decreasing') return <TrendingUp className="w-4 h-4 text-warning-600 rotate-180" />;
    return <Activity className="w-4 h-4 text-neutral-500" />;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <button
            onClick={() => navigate(`/jobs/${jobId}`)}
            className="btn-secondary p-2"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>
          <div>
            <h1 className="text-2xl font-bold text-neutral-900">Performance Metrics</h1>
            <p className="text-sm text-neutral-600 mt-1">Job ID: {jobId}</p>
          </div>
        </div>
        
        {/* View Tabs */}
        <div className="flex space-x-2 bg-neutral-100 p-1 rounded-lg">
          <button
            onClick={() => setSelectedView('overview')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              selectedView === 'overview' 
                ? 'bg-white text-primary-700 shadow-sm' 
                : 'text-neutral-600 hover:text-neutral-900'
            }`}
          >
            Overview
          </button>
          <button
            onClick={() => setSelectedView('workers')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              selectedView === 'workers' 
                ? 'bg-white text-primary-700 shadow-sm' 
                : 'text-neutral-600 hover:text-neutral-900'
            }`}
          >
            Workers
          </button>
          <button
            onClick={() => setSelectedView('batches')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              selectedView === 'batches' 
                ? 'bg-white text-primary-700 shadow-sm' 
                : 'text-neutral-600 hover:text-neutral-900'
            }`}
          >
            Batch Sizing
          </button>
          <button
            onClick={() => setSelectedView('constraints')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              selectedView === 'constraints' 
                ? 'bg-white text-primary-700 shadow-sm' 
                : 'text-neutral-600 hover:text-neutral-900'
            }`}
          >
            Constraints
          </button>
        </div>
      </div>

      {/* Realtime Metrics Cards */}
      {selectedView === 'overview' && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {/* Throughput - Rows/sec */}
            <div className="card p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center justify-between mb-4">
                <div className="p-3 bg-primary-100 rounded-lg">
                  <Zap className="w-6 h-6 text-primary-600" />
                </div>
                {getTrendIndicator(realtimeMetrics?.throughput_trend)}
              </div>
              <h3 className="text-sm font-medium text-neutral-600 mb-1">Throughput</h3>
              <p className="text-3xl font-bold text-neutral-900">
                {realtimeMetrics?.rows_per_second?.toLocaleString() || 0}
              </p>
              <p className="text-sm text-neutral-500 mt-1">rows/sec</p>
              <p className="text-xs text-neutral-400 mt-2">
                {realtimeMetrics?.mb_per_second?.toFixed(2) || 0} MB/sec
              </p>
            </div>

            {/* Memory Usage */}
            <div className="card p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center justify-between mb-4">
                <div className="p-3 bg-accent-100 rounded-lg">
                  <Memory className="w-6 h-6 text-accent-600" />
                </div>
              </div>
              <h3 className="text-sm font-medium text-neutral-600 mb-1">Memory Usage</h3>
              <p className="text-3xl font-bold text-neutral-900">
                {realtimeMetrics?.memory_usage_mb || 0}
              </p>
              <p className="text-sm text-neutral-500 mt-1">MB</p>
              <div className="mt-2">
                <div className="w-full bg-neutral-200 rounded-full h-2">
                  <div 
                    className="bg-accent-600 h-2 rounded-full transition-all"
                    style={{ width: `${Math.min((realtimeMetrics?.memory_usage_mb / 2048) * 100, 100)}%` }}
                  ></div>
                </div>
              </div>
            </div>

            {/* Active Workers */}
            <div className="card p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center justify-between mb-4">
                <div className="p-3 bg-warning-100 rounded-lg">
                  <Users className="w-6 h-6 text-warning-600" />
                </div>
              </div>
              <h3 className="text-sm font-medium text-neutral-600 mb-1">Active Workers</h3>
              <p className="text-3xl font-bold text-neutral-900">
                {realtimeMetrics?.active_workers || 0}
              </p>
              <p className="text-sm text-neutral-500 mt-1">processing chunks</p>
              <p className="text-xs text-neutral-400 mt-2">
                Batch size: {realtimeMetrics?.current_batch_size?.toLocaleString() || 5000}
              </p>
            </div>

            {/* Insert Latency */}
            <div className="card p-6 hover:shadow-lg transition-shadow">
              <div className="flex items-center justify-between mb-4">
                <div className="p-3 bg-error-100 rounded-lg">
                  <Gauge className="w-6 h-6 text-error-600" />
                </div>
              </div>
              <h3 className="text-sm font-medium text-neutral-600 mb-1">Avg Latency</h3>
              <p className="text-3xl font-bold text-neutral-900">
                {realtimeMetrics?.avg_insert_latency_ms || 0}
              </p>
              <p className="text-sm text-neutral-500 mt-1">ms/batch</p>
              <p className="text-xs text-neutral-400 mt-2">
                ETA: {realtimeMetrics?.estimated_completion || 'Calculating...'}
              </p>
            </div>
          </div>

          {/* Throughput History Chart */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold text-neutral-900 mb-4">Throughput Over Time</h3>
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={throughputHistory}>
                <defs>
                  <linearGradient id="colorRows" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="colorMB" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="time" stroke="#6b7280" style={{ fontSize: '12px' }} />
                <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'white', 
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'
                  }}
                />
                <Legend />
                <Area 
                  type="monotone" 
                  dataKey="rows_per_second" 
                  stroke="#3b82f6" 
                  fillOpacity={1} 
                  fill="url(#colorRows)"
                  name="Rows/sec"
                />
                <Area 
                  type="monotone" 
                  dataKey="mb_per_second" 
                  stroke="#10b981" 
                  fillOpacity={1} 
                  fill="url(#colorMB)"
                  name="MB/sec"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Memory & Latency Trends */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Memory Trend */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold text-neutral-900 mb-4">Memory Usage Trend</h3>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={throughputHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis dataKey="time" stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <Tooltip />
                  <Line 
                    type="monotone" 
                    dataKey="memory_usage_mb" 
                    stroke="#10b981" 
                    strokeWidth={2}
                    dot={false}
                    name="Memory (MB)"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Latency Trend */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold text-neutral-900 mb-4">Insert Latency Trend</h3>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={throughputHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis dataKey="time" stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                  <Tooltip />
                  <Line 
                    type="monotone" 
                    dataKey="insert_latency_ms" 
                    stroke="#ef4444" 
                    strokeWidth={2}
                    dot={false}
                    name="Latency (ms)"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </>
      )}

      {/* Worker Stats View */}
      {selectedView === 'workers' && (
        <div className="card overflow-hidden">
          <div className="p-6 border-b border-neutral-200">
            <h3 className="text-lg font-semibold text-neutral-900">Worker Statistics</h3>
            <p className="text-sm text-neutral-600 mt-1">Performance metrics per worker</p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-neutral-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Worker ID
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Rows Processed
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Throughput (rows/sec)
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Throughput (MB/sec)
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Peak Memory
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Avg Latency
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Last Update
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-neutral-200">
                {workerStats.map((worker, idx) => (
                  <tr key={idx} className="hover:bg-neutral-50">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <Users className="w-4 h-4 text-neutral-400 mr-2" />
                        <span className="text-sm font-medium text-neutral-900">
                          {worker.worker_id}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {worker.rows_processed.toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {worker.throughput_rows_per_sec.toFixed(2)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {worker.throughput_mb_per_sec.toFixed(2)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {worker.memory_peak_mb} MB
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {worker.avg_latency_ms} ms
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-500">
                      {format(new Date(worker.last_update), 'HH:mm:ss')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {workerStats.length === 0 && (
            <div className="p-8 text-center text-neutral-500">
              <Users className="w-12 h-12 mx-auto mb-3 text-neutral-300" />
              <p>No worker statistics available yet</p>
            </div>
          )}
        </div>
      )}

      {/* Batch Size History View */}
      {selectedView === 'batches' && (
        <div className="space-y-6">
          <div className="card p-6">
            <h3 className="text-lg font-semibold text-neutral-900 mb-4">Adaptive Batch Size History</h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={batchSizeHistory.slice(-50)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="timestamp" 
                  stroke="#6b7280" 
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => format(new Date(value), 'HH:mm')}
                />
                <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
                <Tooltip 
                  labelFormatter={(value) => format(new Date(value), 'HH:mm:ss')}
                  contentStyle={{ 
                    backgroundColor: 'white', 
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px'
                  }}
                />
                <Legend />
                <Line 
                  type="stepAfter" 
                  dataKey="new_batch_size" 
                  stroke="#3b82f6" 
                  strokeWidth={2}
                  name="Batch Size"
                />
                <Line 
                  type="monotone" 
                  dataKey="avg_latency_ms" 
                  stroke="#ef4444" 
                  strokeWidth={2}
                  dot={false}
                  name="Avg Latency (ms)"
                  yAxisId="right"
                />
                <YAxis yAxisId="right" orientation="right" stroke="#ef4444" style={{ fontSize: '12px' }} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="card overflow-hidden">
            <div className="p-6 border-b border-neutral-200">
              <h3 className="text-lg font-semibold text-neutral-900">Adjustment History</h3>
              <p className="text-sm text-neutral-600 mt-1">Dynamic batch size adjustments based on performance</p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-neutral-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Timestamp
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Table
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Old Size
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      New Size
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Change
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Avg Latency
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Reason
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-neutral-200">
                  {batchSizeHistory.slice(0, 20).map((item, idx) => {
                    const change = ((item.new_batch_size - item.old_batch_size) / item.old_batch_size * 100).toFixed(1);
                    const isIncrease = item.new_batch_size > item.old_batch_size;
                    
                    return (
                      <tr key={idx} className="hover:bg-neutral-50">
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-500">
                          {format(new Date(item.timestamp), 'MMM dd, HH:mm:ss')}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-neutral-900">
                          {item.table_name}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                          {item.old_batch_size.toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                          {item.new_batch_size.toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`inline-flex items-center px-2 py-1 rounded text-xs font-medium ${
                            isIncrease 
                              ? 'bg-accent-100 text-accent-800' 
                              : 'bg-warning-100 text-warning-800'
                          }`}>
                            {isIncrease ? '+' : ''}{change}%
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                          {item.avg_latency_ms} ms
                        </td>
                        <td className="px-6 py-4 text-sm text-neutral-600 max-w-xs truncate">
                          {item.reason}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {batchSizeHistory.length === 0 && (
              <div className="p-8 text-center text-neutral-500">
                <Activity className="w-12 h-12 mx-auto mb-3 text-neutral-300" />
                <p>No batch size adjustments recorded yet</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Constraints View */}
      {selectedView === 'constraints' && (
        <div className="card overflow-hidden">
          <div className="p-6 border-b border-neutral-200">
            <h3 className="text-lg font-semibold text-neutral-900">Constraint Management</h3>
            <p className="text-sm text-neutral-600 mt-1">
              Indexes and foreign keys dropped for bulk insert optimization
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-neutral-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Table
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Type
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Dropped At
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Restored At
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-neutral-200">
                {constraintStatus.map((item, idx) => (
                  <tr key={idx} className="hover:bg-neutral-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-neutral-900">
                      {item.table_name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`inline-flex items-center px-2 py-1 rounded text-xs font-medium ${
                        item.constraint_type === 'index' 
                          ? 'bg-primary-100 text-primary-800' 
                          : 'bg-warning-100 text-warning-800'
                      }`}>
                        {item.constraint_type === 'index' ? 'Index' : 'Foreign Key'}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-900">
                      {item.constraint_name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-500">
                      {item.dropped_at ? format(new Date(item.dropped_at), 'MMM dd, HH:mm:ss') : '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-neutral-500">
                      {item.restored_at ? format(new Date(item.restored_at), 'MMM dd, HH:mm:ss') : '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {item.status === 'restored' ? (
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-accent-100 text-accent-800">
                          <CheckCircle2 className="w-3 h-3 mr-1" />
                          Restored
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-warning-100 text-warning-800">
                          <Clock className="w-3 h-3 mr-1" />
                          Pending
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {constraintStatus.length === 0 && (
            <div className="p-8 text-center text-neutral-500">
              <Database className="w-12 h-12 mx-auto mb-3 text-neutral-300" />
              <p>No constraint operations performed yet</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
