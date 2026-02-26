import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import apiClient from '../services/api.js';
import { Database, ArrowRight, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';

export default function CreateJob() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  
  const [formData, setFormData] = useState({
    sourceHost: '',
    sourcePort: '3306',
    sourceUser: '',
    sourcePassword: '',
    sourceDatabase: '',
    targetHost: '',
    targetPort: '3306',
    targetUser: '',
    targetPassword: '',
    targetDatabase: '',
    chunkSize: '100000',
    batchSize: '1000'
  });

  const handleChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const request = {
        source_config: {
          host: formData.sourceHost,
          port: parseInt(formData.sourcePort),
          user: formData.sourceUser,
          password: formData.sourcePassword,
          database: formData.sourceDatabase
        },
        target_config: {
          host: formData.targetHost,
          port: parseInt(formData.targetPort),
          user: formData.targetUser,
          password: formData.targetPassword,
          database: formData.targetDatabase
        },
        chunk_size: parseInt(formData.chunkSize),
        batch_size: parseInt(formData.batchSize)
      };

      const job = await apiClient.createJob(request);
      setSuccess(true);
      
      setTimeout(() => {
        navigate(`/jobs/${job.id}`);
      }, 1500);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  if (success) {
    return (
      <div className="max-w-2xl mx-auto">
        <div className="card p-12 text-center">
          <div className="w-16 h-16 bg-accent-50 rounded-full flex items-center justify-center mx-auto mb-4">
            <CheckCircle2 className="w-8 h-8 text-accent-600" />
          </div>
          <h2 className="text-2xl font-bold text-neutral-900 mb-2">Migration Job Created!</h2>
          <p className="text-neutral-600 mb-4">Your migration job has been queued and will start processing shortly.</p>
          <Loader2 className="w-6 h-6 text-primary-600 animate-spin mx-auto" />
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto">
      {/* Page Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-neutral-900 mb-2">Create Migration Job</h1>
        <p className="text-neutral-600">Configure source and target databases to start a new migration</p>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="bg-error-50 border border-error-200 rounded-lg p-4 mb-6">
          <div className="flex items-start space-x-3">
            <AlertCircle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
            <div>
              <h3 className="text-sm font-medium text-error-800 mb-1">Failed to create migration job</h3>
              <p className="text-sm text-error-700">{error}</p>
            </div>
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-8">
        {/* Source Database */}
        <div className="card p-6">
          <div className="flex items-center space-x-3 mb-6">
            <div className="w-10 h-10 bg-primary-50 rounded-lg flex items-center justify-center">
              <Database className="w-5 h-5 text-primary-600" />
            </div>
            <div>
              <h2 className="text-lg font-semibold text-neutral-900">Source Database</h2>
              <p className="text-sm text-neutral-600">Database to migrate data from</p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="col-span-2">
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Host
              </label>
              <input
                type="text"
                name="sourceHost"
                value={formData.sourceHost}
                onChange={handleChange}
                placeholder="localhost or IP address"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Port
              </label>
              <input
                type="number"
                name="sourcePort"
                value={formData.sourcePort}
                onChange={handleChange}
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Database Name
              </label>
              <input
                type="text"
                name="sourceDatabase"
                value={formData.sourceDatabase}
                onChange={handleChange}
                placeholder="my_database"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Username
              </label>
              <input
                type="text"
                name="sourceUser"
                value={formData.sourceUser}
                onChange={handleChange}
                placeholder="root"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Password
              </label>
              <input
                type="password"
                name="sourcePassword"
                value={formData.sourcePassword}
                onChange={handleChange}
                placeholder="••••••••"
                required
                className="input"
              />
            </div>
          </div>
        </div>

        {/* Arrow Indicator */}
        <div className="flex justify-center">
          <div className="w-12 h-12 bg-neutral-100 rounded-full flex items-center justify-center">
            <ArrowRight className="w-6 h-6 text-neutral-600" />
          </div>
        </div>

        {/* Target Database */}
        <div className="card p-6">
          <div className="flex items-center space-x-3 mb-6">
            <div className="w-10 h-10 bg-accent-50 rounded-lg flex items-center justify-center">
              <Database className="w-5 h-5 text-accent-600" />
            </div>
            <div>
              <h2 className="text-lg font-semibold text-neutral-900">Target Database</h2>
              <p className="text-sm text-neutral-600">Database to migrate data to</p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="col-span-2">
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Host
              </label>
              <input
                type="text"
                name="targetHost"
                value={formData.targetHost}
                onChange={handleChange}
                placeholder="localhost or IP address"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Port
              </label>
              <input
                type="number"
                name="targetPort"
                value={formData.targetPort}
                onChange={handleChange}
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Database Name
              </label>
              <input
                type="text"
                name="targetDatabase"
                value={formData.targetDatabase}
                onChange={handleChange}
                placeholder="my_database"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Username
              </label>
              <input
                type="text"
                name="targetUser"
                value={formData.targetUser}
                onChange={handleChange}
                placeholder="root"
                required
                className="input"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Password
              </label>
              <input
                type="password"
                name="targetPassword"
                value={formData.targetPassword}
                onChange={handleChange}
                placeholder="••••••••"
                required
                className="input"
              />
            </div>
          </div>
        </div>

        {/* Performance Settings */}
        <div className="card p-6">
          <h2 className="text-lg font-semibold text-neutral-900 mb-4">Performance Settings</h2>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Chunk Size (rows)
              </label>
              <input
                type="number"
                name="chunkSize"
                value={formData.chunkSize}
                onChange={handleChange}
                min="1000"
                step="1000"
                required
                className="input"
              />
              <p className="text-xs text-neutral-500 mt-1">Number of rows per chunk (default: 100,000)</p>
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">
                Batch Size (rows)
              </label>
              <input
                type="number"
                name="batchSize"
                value={formData.batchSize}
                onChange={handleChange}
                min="100"
                step="100"
                required
                className="input"
              />
              <p className="text-xs text-neutral-500 mt-1">Number of rows per INSERT (default: 1,000)</p>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center justify-end space-x-4">
          <button
            type="button"
            onClick={() => navigate('/')}
            className="btn-secondary"
            disabled={loading}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="btn-primary inline-flex items-center space-x-2"
            disabled={loading}
          >
            {loading && <Loader2 className="w-4 h-4 animate-spin" />}
            <span>{loading ? 'Creating...' : 'Create Migration Job'}</span>
          </button>
        </div>
      </form>
    </div>
  );
}
