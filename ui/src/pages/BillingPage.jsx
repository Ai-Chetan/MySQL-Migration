import { useState, useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext.jsx';
import { Building2, CreditCard, BarChart3, TrendingUp, AlertCircle } from 'lucide-react';
import { format, subDays, subMonths } from 'date-fns';
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import apiClient from '../services/api.js';

export default function BillingPage() {
  const { tenant } = useAuth();
  const [usageStats, setUsageStats] = useState(null);
  const [period, setPeriod] = useState('month');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadUsageStats();
  }, [period]);

  const loadUsageStats = async () => {
    try {
      const stats = await apiClient.getUsageStats(period);
      setUsageStats(stats);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const getPlanBadge = (plan) => {
    if (plan === 'enterprise') {
      return <span className="badge-primary text-base">Enterprise</span>;
    } else if (plan === 'pro') {
      return <span className="badge-secondary text-base">Pro</span>;
    } else {
      return <span className="badge-neutral text-base">Free</span>;
    }
  };

  const getPlanLimits = (plan) => {
    if (plan === 'enterprise') {
      return { migrations: 'Unlimited', storage: 'Unlimited', workers: 'Unlimited' };
    } else if (plan === 'pro') {
      return { migrations: '100/month', storage: '500GB', workers: '10' };
    } else {
      return { migrations: '10/month', storage: '10GB', workers: '2' };
    }
  };

  const limits = tenant ? getPlanLimits(tenant.plan) : {};

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-neutral-600">Loading billing information...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-neutral-900">Billing & Usage</h1>
        <p className="text-neutral-600 mt-2">Manage your subscription and track usage</p>
      </div>

      {/* Current Plan */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-4">
            <div className="w-14 h-14 bg-gradient-to-br from-primary-600 to-primary-800 rounded-xl flex items-center justify-center">
              <Building2 className="w-7 h-7 text-white" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-neutral-900">{tenant?.name}</h2>
              <p className="text-sm text-neutral-600">Organization ID: {tenant?.id}</p>
            </div>
          </div>
          {tenant && getPlanBadge(tenant.plan)}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="border border-neutral-200 rounded-lg p-4">
            <p className="text-sm text-neutral-600 mb-1">Migrations</p>
            <p className="text-2xl font-bold text-neutral-900">{limits.migrations}</p>
          </div>
          <div className="border border-neutral-200 rounded-lg p-4">
            <p className="text-sm text-neutral-600 mb-1">Storage</p>
            <p className="text-2xl font-bold text-neutral-900">{limits.storage}</p>
          </div>
          <div className="border border-neutral-200 rounded-lg p-4">
            <p className="text-sm text-neutral-600 mb-1">Max Workers</p>
            <p className="text-2xl font-bold text-neutral-900">{limits.workers}</p>
          </div>
        </div>

        {tenant?.plan === 'free' && (
          <div className="mt-6 bg-primary-50 border border-primary-200 rounded-lg p-4">
            <div className="flex items-start space-x-3">
              <TrendingUp className="w-5 h-5 text-primary-600 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-primary-900">Upgrade to Pro or Enterprise</p>
                <p className="text-sm text-primary-700 mt-1">
                  Unlock more migrations, storage, and parallel processing power
                </p>
                <button className="btn-primary mt-3">
                  View Plans
                </button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Usage Overview */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-neutral-900">Usage Overview</h2>
          <div className="flex space-x-2">
            <button
              onClick={() => setPeriod('day')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                period === 'day'
                  ? 'bg-primary-600 text-white'
                  : 'bg-neutral-100 text-neutral-700 hover:bg-neutral-200'
              }`}
            >
              24 Hours
            </button>
            <button
              onClick={() => setPeriod('week')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                period === 'week'
                  ? 'bg-primary-600 text-white'
                  : 'bg-neutral-100 text-neutral-700 hover:bg-neutral-200'
              }`}
            >
              7 Days
            </button>
            <button
              onClick={() => setPeriod('month')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                period === 'month'
                  ? 'bg-primary-600 text-white'
                  : 'bg-neutral-100 text-neutral-700 hover:bg-neutral-200'
              }`}
            >
              30 Days
            </button>
          </div>
        </div>

        {usageStats && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div className="border border-neutral-200 rounded-lg p-4">
              <div className="flex items-center space-x-3 mb-2">
                <BarChart3 className="w-5 h-5 text-primary-600" />
                <p className="text-sm text-neutral-600">Migrations</p>
              </div>
              <p className="text-3xl font-bold text-neutral-900">{usageStats.total_migrations || 0}</p>
            </div>
            <div className="border border-neutral-200 rounded-lg p-4">
              <div className="flex items-center space-x-3 mb-2">
                <TrendingUp className="w-5 h-5 text-accent-600" />
                <p className="text-sm text-neutral-600">Rows Migrated</p>
              </div>
              <p className="text-3xl font-bold text-neutral-900">
                {(usageStats.total_rows_migrated || 0).toLocaleString()}
              </p>
            </div>
            <div className="border border-neutral-200 rounded-lg p-4">
              <div className="flex items-center space-x-3 mb-2">
                <CreditCard className="w-5 h-5 text-indigo-600" />
                <p className="text-sm text-neutral-600">Compute Hours</p>
              </div>
              <p className="text-3xl font-bold text-neutral-900">
                {(usageStats.total_compute_hours || 0).toFixed(1)}
              </p>
            </div>
            <div className="border border-neutral-200 rounded-lg p-4">
              <div className="flex items-center space-x-3 mb-2">
                <BarChart3 className="w-5 h-5 text-orange-600" />
                <p className="text-sm text-neutral-600">Storage Used</p>
              </div>
              <p className="text-3xl font-bold text-neutral-900">
                {((usageStats.storage_used_gb || 0)).toFixed(2)} GB
              </p>
            </div>
          </div>
        )}

        {/* Usage Chart */}
        {usageStats?.daily_usage && usageStats.daily_usage.length > 0 && (
          <div>
            <h3 className="font-semibold text-neutral-900 mb-4">Daily Activity</h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={usageStats.daily_usage}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="date" 
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
                    borderRadius: '0.5rem'
                  }}
                />
                <Line 
                  type="monotone" 
                  dataKey="migrations" 
                  stroke="#6366f1" 
                  strokeWidth={2}
                  name="Migrations"
                />
                <Line 
                  type="monotone" 
                  dataKey="rows" 
                  stroke="#10b981" 
                  strokeWidth={2}
                  name="Rows (thousands)"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Billing History */}
      <div className="card p-6">
        <h2 className="text-xl font-semibold text-neutral-900 mb-6">Billing History</h2>
        <div className="space-y-3">
          {tenant?.plan === 'free' ? (
            <div className="text-center py-12">
              <CreditCard className="w-12 h-12 text-neutral-400 mx-auto mb-3" />
              <p className="text-neutral-600">No billing history on Free plan</p>
              <p className="text-sm text-neutral-500 mt-1">Upgrade to Pro or Enterprise to see invoices</p>
            </div>
          ) : (
            <div className="border border-neutral-200 rounded-lg p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-semibold text-neutral-900">December 2024</p>
                  <p className="text-sm text-neutral-600">Invoice #INV-2024-12</p>
                </div>
                <div className="text-right">
                  <p className="text-2xl font-bold text-neutral-900">$49.00</p>
                  <span className="badge-success">Paid</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
