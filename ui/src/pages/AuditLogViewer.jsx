import { useState, useEffect } from 'react';
import { 
  Shield, Filter, Search, Download, AlertCircle, CheckCircle2, 
  XCircle, User, Activity, Calendar, ChevronDown
} from 'lucide-react';
import { format } from 'date-fns';
import apiClient from '../services/api.js';

export default function AuditLogViewer() {
  const [logs, setLogs] = useState([]);
  const [summary, setSummary] = useState(null);
  const [actionTypes, setActionTypes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Filters
  const [filters, setFilters] = useState({
    action: '',
    status: '',
    days: 7,
    limit: 100
  });
  const [showFilters, setShowFilters] = useState(false);

  useEffect(() => {
    loadData();
  }, [filters]);

  const loadData = async () => {
    try {
      const [logsData, summaryData, actionsData] = await Promise.all([
        apiClient.getAuditLogs(filters),
        apiClient.getAuditSummary(filters.days),
        apiClient.getAuditActionTypes()
      ]);
      
      setLogs(logsData);
      setSummary(summaryData);
      setActionTypes(actionsData.actions);
      setLoading(false);
    } catch (err) {
      console.error('Failed to load audit logs:', err);
      setError(err.message);
      setLoading(false);
    }
  };

  const handleFilterChange = (key, value) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'success': return <CheckCircle2 className="w-4 h-4 text-accent-600" />;
      case 'failed': return <XCircle className="w-4 h-4 text-error-600" />;
      default: return <Activity className="w-4 h-4 text-neutral-500" />;
    }
  };

  const getStatusBadge = (status) => {
    switch (status) {
      case 'success':
        return <span className="px-2 py-1 rounded-full text-xs font-medium bg-accent-100 text-accent-800">Success</span>;
      case 'failed':
        return <span className="px-2 py-1 rounded-full text-xs font-medium bg-error-100 text-error-800">Failed</span>;
      case 'blocked':
        return <span className="px-2 py-1 rounded-full text-xs font-medium bg-warning-100 text-warning-800">Blocked</span>;
      default:
        return <span className="px-2 py-1 rounded-full text-xs font-medium bg-neutral-100 text-neutral-800">{status}</span>;
    }
  };

  const getActionColor = (action) => {
    if (action.includes('delete') || action.includes('cancel')) return 'text-error-700 bg-error-50';
    if (action.includes('create') || action.includes('upgrade')) return 'text-accent-700 bg-accent-50';
    if (action.includes('update') || action.includes('modify')) return 'text-warning-700 bg-warning-50';
    return 'text-primary-700 bg-primary-50';
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-10 h-10 sm:w-12 sm:h-12 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-sm sm:text-base text-neutral-600">Loading audit logs...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4 sm:space-y-6 pb-6">
      {/* Header */}
      <div className="px-4 sm:px-0">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-3 sm:space-y-0">
          <div>
            <h1 className="text-2xl sm:text-3xl font-bold text-neutral-900">Audit Logs</h1>
            <p className="text-sm sm:text-base text-neutral-600 mt-1">Security and compliance audit trail</p>
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="btn-secondary inline-flex items-center space-x-2 text-sm"
          >
            <Filter className="w-4 h-4" />
            <span>Filters</span>
            <ChevronDown className={`w-4 h-4 transition-transform ${showFilters ? 'rotate-180' : ''}`} />
          </button>
        </div>
      </div>

      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 px-4 sm:px-0">
          <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
            <div className="flex items-center space-x-2 mb-2">
              <Activity className="w-4 h-4 text-primary-600" />
              <p className="text-xs sm:text-sm text-neutral-600">Total Actions</p>
            </div>
            <p className="text-2xl sm:text-3xl font-bold text-neutral-900">{summary.total_actions}</p>
          </div>

          <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
            <div className="flex items-center space-x-2 mb-2">
              <CheckCircle2 className="w-4 h-4 text-accent-600" />
              <p className="text-xs sm:text-sm text-neutral-600">Successful</p>
            </div>
            <p className="text-2xl sm:text-3xl font-bold text-accent-600">{summary.successful_actions}</p>
          </div>

          <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
            <div className="flex items-center space-x-2 mb-2">
              <XCircle className="w-4 h-4 text-error-600" />
              <p className="text-xs sm:text-sm text-neutral-600">Failed</p>
            </div>
            <p className="text-2xl sm:text-3xl font-bold text-error-600">{summary.failed_actions}</p>
          </div>

          <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
            <div className="flex items-center space-x-2 mb-2">
              <User className="w-4 h-4 text-warning-600" />
              <p className="text-xs sm:text-sm text-neutral-600">Unique Users</p>
            </div>
            <p className="text-2xl sm:text-3xl font-bold text-neutral-900">{summary.unique_users}</p>
          </div>
        </div>
      )}

      {/* Filters - Collapsible on Mobile */}
      {showFilters && (
        <div className="card p-4 sm:p-6 mx-4 sm:mx-0">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <div>
              <label className="block text-xs sm:text-sm font-medium text-neutral-700 mb-2">Action Type</label>
              <select
                value={filters.action}
                onChange={(e) => handleFilterChange('action', e.target.value)}
                className="input w-full text-sm"
              >
                <option value="">All Actions</option>
                {actionTypes.map(action => (
                  <option key={action} value={action}>{action}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs sm:text-sm font-medium text-neutral-700 mb-2">Status</label>
              <select
                value={filters.status}
                onChange={(e) => handleFilterChange('status', e.target.value)}
                className="input w-full text-sm"
              >
                <option value="">All Statuses</option>
                <option value="success">Success</option>
                <option value="failed">Failed</option>
                <option value="blocked">Blocked</option>
              </select>
            </div>

            <div>
              <label className="block text-xs sm:text-sm font-medium text-neutral-700 mb-2">Time Range</label>
              <select
                value={filters.days}
                onChange={(e) => handleFilterChange('days', parseInt(e.target.value))}
                className="input w-full text-sm"
              >
                <option value="1">Last 24 hours</option>
                <option value="7">Last 7 days</option>
                <option value="30">Last 30 days</option>
                <option value="90">Last 90 days</option>
              </select>
            </div>

            <div>
              <label className="block text-xs sm:text-sm font-medium text-neutral-700 mb-2">Results Limit</label>
              <select
                value={filters.limit}
                onChange={(e) => handleFilterChange('limit', parseInt(e.target.value))}
                className="input w-full text-sm"
              >
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="500">500</option>
                <option value="1000">1000</option>
              </select>
            </div>
          </div>
        </div>
      )}

      {/* Audit Logs Table - Mobile Responsive */}
      <div className="card overflow-hidden mx-4 sm:mx-0">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-neutral-50">
              <tr>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                  Timestamp
                </th>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                  User
                </th>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                  Action
                </th>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                  Resource
                </th>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-3 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider hidden lg:table-cell">
                  IP Address
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-neutral-200">
              {logs.map((log) => (
                <tr key={log.id} className="hover:bg-neutral-50 cursor-pointer">
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm text-neutral-500">
                    {format(new Date(log.timestamp), 'MMM dd, HH:mm:ss')}
                  </td>
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      <User className="w-3 h-3 sm:w-4 sm:h-4 text-neutral-400" />
                      <span className="text-xs sm:text-sm text-neutral-900">{log.user_email || 'System'}</span>
                    </div>
                  </td>
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${getActionColor(log.action)}`}>
                      {log.action}
                    </span>
                  </td>
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm text-neutral-600">
                    {log.resource_type || '-'}
                  </td>
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap">
                    {getStatusBadge(log.status)}
                  </td>
                  <td className="px-3 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm text-neutral-500 hidden lg:table-cell">
                    {log.ip_address || '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {logs.length === 0 && (
          <div className="p-8 text-center text-neutral-500">
            <Shield className="w-10 h-10 sm:w-12 sm:h-12 mx-auto mb-3 text-neutral-300" />
            <p className="text-sm sm:text-base">No audit logs found</p>
            <p className="text-xs sm:text-sm mt-1">Try adjusting your filters</p>
          </div>
        )}
      </div>

      {/* Load More */}
      {logs.length === filters.limit && (
        <div className="text-center px-4 sm:px-0">
          <button
            onClick={() => handleFilterChange('limit', filters.limit + 100)}
            className="btn-secondary text-sm"
          >
            Load More
          </button>
        </div>
      )}
    </div>
  );
}
