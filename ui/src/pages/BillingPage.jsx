import { useState, useEffect } from 'react';
import { 
  CreditCard, TrendingUp, AlertCircle, Check, X, Zap, Crown, Shield, 
  ArrowUp, Download, Calendar, Database, Activity, DollarSign, Package
} from 'lucide-react';
import { format } from 'date-fns';
import { AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import apiClient from '../services/api.js';

export default function BillingPage() {
  const [currentUsage, setCurrentUsage] = useState(null);
  const [usageHistory, setUsageHistory] = useState([]);
  const [currentPlan, setCurrentPlan] = useState(null);
  const [allPlans, setAllPlans] = useState([]);
  const [invoices, setInvoices] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedView, setSelectedView] = useState('overview'); // overview, plans, invoices

  useEffect(() => {
    loadBillingData();
  }, []);

  const loadBillingData = async () => {
    try {
      const [usage, history, plan, plans, invoicesList] = await Promise.all([
        apiClient.getCurrentUsage(),
        apiClient.getUsageHistory(30),
        apiClient.getCurrentPlan(),
        apiClient.getPlans(),
        apiClient.getInvoices(10)
      ]);
      
      setCurrentUsage(usage);
      setUsageHistory(history);
      setCurrentPlan(plan);
      setAllPlans(plans);
      setInvoices(invoicesList);
      setLoading(false);
    } catch (err) {
      console.error('Failed to load billing data:', err);
      setError(err.message);
      setLoading(false);
    }
  };

  const getPlanIcon = (planName) => {
    switch (planName) {
      case 'enterprise': return <Crown className="w-5 h-5 sm:w-6 sm:h-6" />;
      case 'professional': return <Shield className="w-5 h-5 sm:w-6 sm:h-6" />;
      case 'starter': return <Zap className="w-5 h-5 sm:w-6 sm:h-6" />;
      default: return <Database className="w-5 h-5 sm:w-6 sm:h-6" />;
    }
  };

  const getPlanColor = (planName) => {
    switch (planName) {
      case 'enterprise': return 'from-purple-600 to-pink-600';
      case 'professional': return 'from-primary-600 to-accent-600';
      case 'starter': return 'from-warning-500 to-warning-600';
      default: return 'from-neutral-400 to-neutral-500';
    }
  };

  const handleUpgradePlan = async (planId) => {
    try {
      await apiClient.upgradePlan(planId);
      await loadBillingData();
      alert('Plan upgraded successfully!');
    } catch (err) {
      alert(`Failed to upgrade plan: ${err.message}`);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-10 h-10 sm:w-12 sm:h-12 border-4 border-primary-600 border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-sm sm:text-base text-neutral-600">Loading billing information...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="card bg-error-50 border-error-200 p-4 sm:p-6">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-5 h-5 sm:w-6 sm:h-6 text-error-600 flex-shrink-0" />
          <div>
            <h3 className="font-semibold text-error-900 text-sm sm:text-base">Failed to load billing data</h3>
            <p className="text-xs sm:text-sm text-error-700 mt-1">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4 sm:space-y-6 pb-6">
      {/* Header */}
      <div className="px-4 sm:px-0">
        <h1 className="text-2xl sm:text-3xl font-bold text-neutral-900">Billing & Usage</h1>
        <p className="text-sm sm:text-base text-neutral-600 mt-1 sm:mt-2">Manage your subscription and track usage</p>
      </div>

      {/* View Tabs - Mobile Responsive */}
      <div className="flex overflow-x-auto space-x-2 bg-neutral-100 p-1 rounded-lg mx-4 sm:mx-0">
        <button
          onClick={() => setSelectedView('overview')}
          className={`flex-shrink-0 px-3 sm:px-4 py-2 rounded-md text-xs sm:text-sm font-medium transition-colors ${
            selectedView === 'overview' 
              ? 'bg-white text-primary-700 shadow-sm' 
              : 'text-neutral-600 hover:text-neutral-900'
          }`}
        >
          Overview
        </button>
        <button
          onClick={() => setSelectedView('plans')}
          className={`flex-shrink-0 px-3 sm:px-4 py-2 rounded-md text-xs sm:text-sm font-medium transition-colors ${
            selectedView === 'plans' 
              ? 'bg-white text-primary-700 shadow-sm' 
              : 'text-neutral-600 hover:text-neutral-900'
          }`}
        >
          Plans
        </button>
        <button
          onClick={() => setSelectedView('invoices')}
          className={`flex-shrink-0 px-3 sm:px-4 py-2 rounded-md text-xs sm:text-sm font-medium transition-colors ${
            selectedView === 'invoices' 
              ? 'bg-white text-primary-700 shadow-sm' 
              : 'text-neutral-600 hover:text-neutral-900'
          }`}
        >
          Invoices
        </button>
      </div>

      {/* Overview Tab */}
      {selectedView === 'overview' && (
        <div className="space-y-4 sm:space-y-6">
          {/* Current Plan Card */}
          {currentPlan && (
            <div className="card p-4 sm:p-6 mx-4 sm:mx-0">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-4 sm:mb-6 space-y-3 sm:space-y-0">
                <div className="flex items-center space-x-3 sm:space-x-4">
                  <div className={`w-12 h-12 sm:w-14 sm:h-14 bg-gradient-to-br ${getPlanColor(currentPlan.plan.name)} rounded-xl flex items-center justify-center text-white`}>
                    {getPlanIcon(currentPlan.plan.name)}
                  </div>
                  <div>
                    <h2 className="text-lg sm:text-xl font-semibold text-neutral-900">{currentPlan.plan.display_name}</h2>
                    <p className="text-xs sm:text-sm text-neutral-600">${currentPlan.plan.price_monthly}/month</p>
                  </div>
                </div>
                <span className={`px-3 py-1 rounded-full text-xs sm:text-sm font-medium ${
                  currentPlan.subscription.status === 'active' 
                    ? 'bg-accent-100 text-accent-800' 
                    : 'bg-error-100 text-error-800'
                }`}>
                  {currentPlan.subscription.status}
                </span>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 sm:gap-4">
                <div className="border border-neutral-200 rounded-lg p-3 sm:p-4">
                  <p className="text-xs sm:text-sm text-neutral-600 mb-1">Concurrent Jobs</p>
                  <p className="text-xl sm:text-2xl font-bold text-neutral-900">{currentPlan.plan.max_concurrent_jobs}</p>
                </div>
                <div className="border border-neutral-200 rounded-lg p-3 sm:p-4">
                  <p className="text-xs sm:text-sm text-neutral-600 mb-1">Monthly GB</p>
                  <p className="text-xl sm:text-2xl font-bold text-neutral-900">{currentPlan.plan.max_gb_per_month}</p>
                </div>
                <div className="border border-neutral-200 rounded-lg p-3 sm:p-4 col-span-2 sm:col-span-1">
                  <p className="text-xs sm:text-sm text-neutral-600 mb-1">Support</p>
                  <p className="text-xl sm:text-2xl font-bold text-neutral-900 capitalize">{currentPlan.plan.support_level}</p>
                </div>
              </div>
            </div>
          )}

          {/* Usage Stats Cards */}
          {currentUsage && (
            <>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 px-4 sm:px-0">
                <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
                  <div className="flex items-center space-x-2 sm:space-x-3 mb-2">
                    <Database className="w-4 h-4 sm:w-5 sm:h-5 text-primary-600" />
                    <p className="text-xs sm:text-sm text-neutral-600">GB Migrated</p>
                  </div>
                  <p className="text-xl sm:text-3xl font-bold text-neutral-900">{currentUsage.gb_migrated.toFixed(2)}</p>
                  <p className="text-xs text-neutral-500 mt-1">of {currentUsage.plan_limit_gb} GB</p>
                  <div className="mt-2 w-full bg-neutral-200 rounded-full h-1.5 sm:h-2">
                    <div 
                      className="bg-primary-600 h-1.5 sm:h-2 rounded-full transition-all"
                      style={{ width: `${Math.min(currentUsage.usage_percentage, 100)}%` }}
                    ></div>
                  </div>
                </div>

                <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
                  <div className="flex items-center space-x-2 sm:space-x-3 mb-2">
                    <Activity className="w-4 h-4 sm:w-5 sm:h-5 text-accent-600" />
                    <p className="text-xs sm:text-sm text-neutral-600">Rows Processed</p>
                  </div>
                  <p className="text-xl sm:text-3xl font-bold text-neutral-900">
                    {(currentUsage.rows_processed / 1000000).toFixed(1)}M
                  </p>
                  <p className="text-xs text-neutral-500 mt-1">this month</p>
                </div>

                <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
                  <div className="flex items-center space-x-2 sm:space-x-3 mb-2">
                    <Package className="w-4 h-4 sm:w-5 sm:h-5 text-warning-600" />
                    <p className="text-xs sm:text-sm text-neutral-600">Jobs Created</p>
                  </div>
                  <p className="text-xl sm:text-3xl font-bold text-neutral-900">{currentUsage.jobs_created}</p>
                  <p className="text-xs text-neutral-500 mt-1">migrations</p>
                </div>

                <div className="card p-3 sm:p-4 hover:shadow-lg transition-shadow">
                  <div className="flex items-center space-x-2 sm:space-x-3 mb-2">
                    <TrendingUp className="w-4 h-4 sm:w-5 sm:h-5 text-error-600" />
                    <p className="text-xs sm:text-sm text-neutral-600">Compute Hours</p>
                  </div>
                  <p className="text-xl sm:text-3xl font-bold text-neutral-900">{currentUsage.compute_hours.toFixed(1)}</p>
                  <p className="text-xs text-neutral-500 mt-1">hours</p>
                </div>
              </div>

              {/* Warnings */}
              {currentUsage.warnings && currentUsage.warnings.length > 0 && (
                <div className="card bg-warning-50 border-warning-200 p-4 sm:p-6 mx-4 sm:mx-0">
                  <div className="flex items-start space-x-3">
                    <AlertCircle className="w-5 h-5 sm:w-6 sm:h-6 text-warning-600 flex-shrink-0" />
                    <div className="flex-1">
                      <h3 className="font-semibold text-warning-900 text-sm sm:text-base">Usage Warnings</h3>
                      <ul className="mt-2 space-y-1">
                        {currentUsage.warnings.map((warning, idx) => (
                          <li key={idx} className="text-xs sm:text-sm text-warning-700">{warning}</li>
                        ))}
                      </ul>
                      <button
                        onClick={() => setSelectedView('plans')}
                        className="btn-warning mt-3 text-xs sm:text-sm"
                      >
                        Upgrade Plan
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Usage History Chart */}
          {usageHistory.length > 0 && (
            <div className="card p-4 sm:p-6 mx-4 sm:mx-0">
              <h3 className="text-base sm:text-lg font-semibold text-neutral-900 mb-4">Usage Trend (30 Days)</h3>
              <div className="w-full overflow-x-auto">
                <ResponsiveContainer width="100%" height={250} minWidth={300}>
                  <AreaChart data={usageHistory}>
                    <defs>
                      <linearGradient id="colorGB" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                        <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis 
                      dataKey="date" 
                      stroke="#6b7280"
                      style={{ fontSize: '0.7rem' }}
                      tickFormatter={(date) => format(new Date(date), 'MM/dd')}
                    />
                    <YAxis stroke="#6b7280" style={{ fontSize: '0.7rem' }} />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: 'white', 
                        border: '1px solid #e5e7eb',
                        borderRadius: '8px',
                        fontSize: '0.75rem'
                      }}
                      labelFormatter={(date) => format(new Date(date), 'MMM dd, yyyy')}
                    />
                    <Area 
                      type="monotone" 
                      dataKey="gb_migrated" 
                      stroke="#3b82f6" 
                      fillOpacity={1} 
                      fill="url(#colorGB)"
                      name="GB Migrated"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Plans Tab */}
      {selectedView === 'plans' && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 sm:gap-6 px-4 sm:px-0">
          {allPlans.map((plan) => {
            const isCurrentPlan = currentPlan && currentPlan.plan.name === plan.name;
            
            return (
              <div 
                key={plan.id}
                className={`card p-4 sm:p-6 ${isCurrentPlan ? 'border-2 border-primary-600' : ''} hover:shadow-xl transition-all relative`}
              >
                {isCurrentPlan && (
                  <div className="absolute top-0 right-0 bg-primary-600 text-white px-2 sm:px-3 py-1 rounded-bl-lg rounded-tr-lg text-xs font-medium">
                    Current
                  </div>
                )}
                
                <div className={`w-12 h-12 sm:w-14 sm:h-14 bg-gradient-to-br ${getPlanColor(plan.name)} rounded-xl flex items-center justify-center text-white mb-4`}>
                  {getPlanIcon(plan.name)}
                </div>
                
                <h3 className="text-lg sm:text-xl font-bold text-neutral-900 mb-1">{plan.display_name}</h3>
                <p className="text-xs sm:text-sm text-neutral-600 mb-4">{plan.description}</p>
                
                <div className="mb-4 sm:mb-6">
                  <span className="text-3xl sm:text-4xl font-bold text-neutral-900">${plan.price_monthly}</span>
                  <span className="text-sm text-neutral-600">/month</span>
                  {plan.price_per_gb > 0 && (
                    <p className="text-xs text-neutral-500 mt-1">+ ${plan.price_per_gb}/GB</p>
                  )}
                </div>
                
                <ul className="space-y-2 sm:space-y-3 mb-6">
                  <li className="flex items-start space-x-2">
                    <Check className="w-4 h-4 sm:w-5 sm:h-5 text-accent-600 flex-shrink-0 mt-0.5" />
                    <span className="text-xs sm:text-sm text-neutral-700">{plan.max_concurrent_jobs} concurrent jobs</span>
                  </li>
                  <li className="flex items-start space-x-2">
                    <Check className="w-4 h-4 sm:w-5 sm:h-5 text-accent-600 flex-shrink-0 mt-0.5" />
                    <span className="text-xs sm:text-sm text-neutral-700">{plan.max_gb_per_month} GB/month</span>
                  </li>
                  <li className="flex items-start space-x-2">
                    <Check className="w-4 h-4 sm:w-5 sm:h-5 text-accent-600 flex-shrink-0 mt-0.5" />
                    <span className="text-xs sm:text-sm text-neutral-700">{plan.max_workers_per_job} workers per job</span>
                  </li>
                  <li className="flex items-start space-x-2">
                    <Check className="w-4 h-4 sm:w-5 sm:h-5 text-accent-600 flex-shrink-0 mt-0.5" />
                    <span className="text-xs sm:text-sm text-neutral-700 capitalize">{plan.support_level} support</span>
                  </li>
                </ul>
                
                <button
                  onClick={() => !isCurrentPlan && handleUpgradePlan(plan.id)}
                  disabled={isCurrentPlan}
                  className={`w-full text-sm sm:text-base ${
                    isCurrentPlan
                      ? 'btn-secondary cursor-not-allowed'
                      : 'btn-primary'
                  }`}
                >
                  {isCurrentPlan ? 'Current Plan' : 'Upgrade'}
                </button>
              </div>
            );
          })}
        </div>
      )}

      {/* Invoices Tab */}
      {selectedView === 'invoices' && (
        <div className="card overflow-hidden mx-4 sm:mx-0">
          <div className="p-4 sm:p-6 border-b border-neutral-200">
            <h3 className="text-base sm:text-lg font-semibold text-neutral-900">Invoice History</h3>
            <p className="text-xs sm:text-sm text-neutral-600 mt-1">Download and view past invoices</p>
          </div>
          
          {invoices.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-neutral-50">
                  <tr>
                    <th className="px-4 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Invoice #
                    </th>
                    <th className="px-4 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Period
                    </th>
                    <th className="px-4 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Amount
                    </th>
                    <th className="px-4 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-4 sm:px-6 py-3 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-neutral-200">
                  {invoices.map((invoice) => (
                    <tr key={invoice.id} className="hover:bg-neutral-50">
                      <td className="px-4 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm font-medium text-neutral-900">
                        {invoice.invoice_number}
                      </td>
                      <td className="px-4 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm text-neutral-600">
                        {format(new Date(invoice.billing_period_start), 'MMM dd')} - {format(new Date(invoice.billing_period_end), 'MMM dd, yyyy')}
                      </td>
                      <td className="px-4 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm text-neutral-900">
                        ${invoice.total.toFixed(2)}
                      </td>
                      <td className="px-4 sm:px-6 py-4 whitespace-nowrap">
                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                          invoice.status === 'paid'
                            ? 'bg-accent-100 text-accent-800'
                            : invoice.status === 'pending'
                            ? 'bg-warning-100 text-warning-800'
                            : 'bg-error-100 text-error-800'
                        }`}>
                          {invoice.status}
                        </span>
                      </td>
                      <td className="px-4 sm:px-6 py-4 whitespace-nowrap text-xs sm:text-sm">
                        <button className="text-primary-600 hover:text-primary-800 inline-flex items-center space-x-1">
                          <Download className="w-3 h-3 sm:w-4 sm:h-4" />
                          <span>Download</span>
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center text-neutral-500">
              <Calendar className="w-10 h-10 sm:w-12 sm:h-12 mx-auto mb-3 text-neutral-300" />
              <p className="text-sm sm:text-base">No invoices yet</p>
              <p className="text-xs sm:text-sm mt-1">Invoices will appear here after your first billing cycle</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
