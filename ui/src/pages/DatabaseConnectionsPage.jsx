import { useState, useEffect } from 'react';
import { Plus, Database, Trash2, CheckCircle, XCircle, Loader } from 'lucide-react';
import apiClient from '../services/api';

export default function DatabaseConnectionsPage() {
  const [connections, setConnections] = useState([]);
  const [showAddForm, setShowAddForm] = useState(false);
  const [testing, setTesting] = useState({});
  const [loading, setLoading] = useState(true);

  const [formData, setFormData] = useState({
    name: '',
    db_type: 'mysql',
    host: 'localhost',
    port: 3306,
    database: '',
    username: '',
    password: '',
    ssl: false
  });

  useEffect(() => {
    loadConnections();
  }, []);

  const loadConnections = async () => {
    try {
      const response = await apiClient.get('/schema-migration/connections');
      setConnections(response.data.connections || []);
    } catch (error) {
      console.error('Failed to load connections:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await apiClient.post('/schema-migration/connections', formData);
      setShowAddForm(false);
      setFormData({
        name: '',
        db_type: 'mysql',
        host: 'localhost',
        port: 3306,
        database: '',
        username: '',
        password: '',
        ssl: false
      });
      loadConnections();
    } catch (error) {
      console.error('Failed to create connection:', error);
      alert('Failed to create connection: ' + (error.response?.data?.detail || error.message));
    }
  };

  const testConnection = async (connId) => {
    setTesting(prev => ({ ...prev, [connId]: 'testing' }));
    try {
      const response = await apiClient.post(`/schema-migration/connections/${connId}/test`);
      setTesting(prev => ({
        ...prev,
        [connId]: response.data.success ? 'success' : 'failed'
      }));
      setTimeout(() => {
        setTesting(prev => {
          const updated = { ...prev };
          delete updated[connId];
          return updated;
        });
      }, 3000);
    } catch (error) {
      setTesting(prev => ({ ...prev, [connId]: 'failed' }));
      setTimeout(() => {
        setTesting(prev => {
          const updated = { ...prev };
          delete updated[connId];
          return updated;
        });
      }, 3000);
    }
  };

  const deleteConnection = async (connId) => {
    if (!confirm('Are you sure you want to delete this connection?')) return;
    
    try {
      await apiClient.delete(`/schema-migration/connections/${connId}`);
      loadConnections();
    } catch (error) {
      console.error('Failed to delete connection:', error);
      alert('Failed to delete connection');
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader className="animate-spin" size={32} />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Database Connections</h1>
          <p className="text-gray-600 mt-1">Manage connections to source and target databases</p>
        </div>
        <button
          onClick={() => setShowAddForm(true)}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          <Plus size={20} />
          Add Connection
        </button>
      </div>

      {showAddForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <h2 className="text-xl font-bold mb-4">Add Database Connection</h2>
            
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Connection Name
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Database Type
                  </label>
                  <select
                    value={formData.db_type}
                    onChange={(e) => setFormData({
                      ...formData,
                      db_type: e.target.value,
                      port: e.target.value === 'postgresql' ? 5432 : 3306
                    })}
                    className="w-full px-3 py-2 border rounded-lg"
                  >
                    <option value="mysql">MySQL</option>
                    <option value="postgresql">PostgreSQL</option>
                    <option value="mariadb">MariaDB</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Host
                  </label>
                  <input
                    type="text"
                    value={formData.host}
                    onChange={(e) => setFormData({ ...formData, host: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Port
                  </label>
                  <input
                    type="number"
                    value={formData.port}
                    onChange={(e) => setFormData({ ...formData, port: parseInt(e.target.value) })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Database Name
                  </label>
                  <input
                    type="text"
                    value={formData.database}
                    onChange={(e) => setFormData({ ...formData, database: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Username
                  </label>
                  <input
                    type="text"
                    value={formData.username}
                    onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Password
                  </label>
                  <input
                    type="password"
                    value={formData.password}
                    onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg"
                    required
                  />
                </div>

                <div className="col-span-2">
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={formData.ssl}
                      onChange={(e) => setFormData({ ...formData, ssl: e.target.checked })}
                      className="rounded"
                    />
                    <span className="text-sm text-gray-700">Use SSL</span>
                  </label>
                </div>
              </div>

              <div className="flex gap-3 justify-end">
                <button
                  type="button"
                  onClick={() => setShowAddForm(false)}
                  className="px-4 py-2 border rounded-lg hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  Add Connection
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      <div className="grid gap-4">
        {connections.length === 0 ? (
          <div className="text-center py-12 bg-gray-50 rounded-lg">
            <Database size={48} className="mx-auto text-gray-400 mb-3" />
            <p className="text-gray-600 mb-4">No database connections yet</p>
            <button
              onClick={() => setShowAddForm(true)}
              className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              <Plus size={20} />
              Add Your First Connection
            </button>
          </div>
        ) : (
          connections.map((conn) => (
            <div
              key={conn.id}
              className="bg-white border rounded-lg p-4 hover:shadow-md transition-shadow"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <Database size={24} className="text-blue-600" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-lg">{conn.name}</h3>
                    <div className="flex gap-4 mt-2 text-sm text-gray-600">
                      <span className="flex items-center gap-1">
                        <span className="font-medium">Type:</span> {conn.db_type.toUpperCase()}
                      </span>
                      <span className="flex items-center gap-1">
                        <span className="font-medium">Host:</span> {conn.host}:{conn.port}
                      </span>
                      <span className="flex items-center gap-1">
                        <span className="font-medium">Database:</span> {conn.database}
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  {testing[conn.id] === 'testing' && (
                    <button className="flex items-center gap-2 px-3 py-1.5 text-sm border rounded-lg">
                      <Loader size={16} className="animate-spin" />
                      Testing...
                    </button>
                  )}
                  {testing[conn.id] === 'success' && (
                    <div className="flex items-center gap-2 px-3 py-1.5 text-sm bg-green-50 text-green-700 rounded-lg">
                      <CheckCircle size={16} />
                      Connected
                    </div>
                  )}
                  {testing[conn.id] === 'failed' && (
                    <div className="flex items-center gap-2 px-3 py-1.5 text-sm bg-red-50 text-red-700 rounded-lg">
                      <XCircle size={16} />
                      Failed
                    </div>
                  )}
                  {!testing[conn.id] && (
                    <button
                      onClick={() => testConnection(conn.id)}
                      className="px-3 py-1.5 text-sm border rounded-lg hover:bg-gray-50"
                    >
                      Test
                    </button>
                  )}
                  <button
                    onClick={() => deleteConnection(conn.id)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
