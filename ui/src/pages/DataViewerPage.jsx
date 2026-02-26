import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import axios from 'axios';
import { Database, Download, ChevronLeft, ChevronRight, Search, AlertCircle, Loader2, FileDown } from 'lucide-react';

export default function DataViewerPage() {
  const [searchParams] = useSearchParams();
  const connectionId = searchParams.get('connection');
  const tableName = searchParams.get('table');
  const tableType = searchParams.get('type') || 'old'; // 'old' or 'new'

  const [connection, setConnection] = useState(null);
  const [tableData, setTableData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [exporting, setExporting] = useState(false);

  // Pagination
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [totalRows, setTotalRows] = useState(0);

  // Search/Filter
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredData, setFilteredData] = useState([]);

  useEffect(() => {
    if (connectionId) {
      loadConnection();
    }
  }, [connectionId]);

  useEffect(() => {
    if (connectionId && tableName) {
      loadTableData();
    }
  }, [connectionId, tableName, currentPage, pageSize]);

  useEffect(() => {
    // Client-side filtering
    if (searchTerm) {
      const filtered = tableData.filter(row =>
        Object.values(row).some(val =>
          String(val).toLowerCase().includes(searchTerm.toLowerCase())
        )
      );
      setFilteredData(filtered);
    } else {
      setFilteredData(tableData);
    }
  }, [searchTerm, tableData]);

  const loadConnection = async () => {
    try {
      const response = await axios.get(`http://localhost:8000/api/schema-migration/connections`);
      const conn = response.data.find(c => c.id === connectionId);
      setConnection(conn);
    } catch (err) {
      console.error('Failed to load connection:', err);
      setError('Failed to load connection details');
    }
  };

  const loadTableData = async () => {
    setLoading(true);
    setError(null);

    try {
      const offset = (currentPage - 1) * pageSize;
      const response = await axios.get(
        `http://localhost:8000/api/schema-migration/connections/${connectionId}/tables/${tableName}/data`,
        {
          params: { limit: pageSize, offset }
        }
      );

      setTableData(response.data.rows || []);
      setTotalRows(response.data.total || 0);
      
      // Extract columns from first row
      if (response.data.rows && response.data.rows.length > 0) {
        setColumns(Object.keys(response.data.rows[0]));
      } else {
        // If no data, try to get schema
        const schemaResponse = await axios.get(
          `http://localhost:8000/api/schema-migration/connections/${connectionId}/tables/${tableName}/schema`
        );
        setColumns(schemaResponse.data.columns.map(col => col.name));
      }
    } catch (err) {
      console.error('Failed to load table data:', err);
      setError(err.response?.data?.detail || 'Failed to load table data');
    } finally {
      setLoading(false);
    }
  };

  const exportData = async (format) => {
    setExporting(true);
    try {
      const response = await axios.post(
        `http://localhost:8000/api/schema-migration/export/data`,
        {
          connection_id: connectionId,
          table_name: tableName,
          format: format,
          limit: null // Export all data
        },
        { responseType: 'blob' }
      );

      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `${tableName}.${format}`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Export failed:', err);
      setError('Failed to export data');
    } finally {
      setExporting(false);
    }
  };

  const totalPages = Math.ceil(totalRows / pageSize);

  if (!connectionId || !tableName) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <AlertCircle className="w-12 h-12 text-neutral-400 mx-auto mb-4" />
          <p className="text-neutral-600">No table selected</p>
          <p className="text-sm text-neutral-500 mt-2">
            Navigate from Schema Migration page
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center space-x-3">
            <div className="w-12 h-12 bg-gradient-to-br from-primary-600 to-primary-800 rounded-lg flex items-center justify-center">
              <Database className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-neutral-900">Data Viewer</h1>
              <p className="text-neutral-600 mt-1">
                {connection?.name} • Table: <span className="font-mono font-semibold">{tableName}</span>
                {tableType === 'new' && <span className="ml-2 text-xs px-2 py-1 bg-accent-100 text-accent-700 rounded">New/Generated</span>}
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={() => exportData('csv')}
            disabled={exporting || loading}
            className="flex items-center space-x-2 px-4 py-2 bg-white border border-neutral-300 text-neutral-700 rounded-lg hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {exporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <FileDown className="w-4 h-4" />}
            <span>Export CSV</span>
          </button>
          <button
            onClick={() => exportData('json')}
            disabled={exporting || loading}
            className="flex items-center space-x-2 px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {exporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
            <span>Export JSON</span>
          </button>
        </div>
      </div>

      {/* Stats & Search */}
      <div className="bg-white rounded-lg border border-neutral-200 p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-6">
            <div>
              <p className="text-sm text-neutral-500">Total Rows</p>
              <p className="text-2xl font-bold text-neutral-900">{totalRows.toLocaleString()}</p>
            </div>
            <div className="h-10 w-px bg-neutral-200"></div>
            <div>
              <p className="text-sm text-neutral-500">Columns</p>
              <p className="text-2xl font-bold text-neutral-900">{columns.length}</p>
            </div>
            <div className="h-10 w-px bg-neutral-200"></div>
            <div>
              <p className="text-sm text-neutral-500">Showing</p>
              <p className="text-2xl font-bold text-neutral-900">
                {Math.min((currentPage - 1) * pageSize + 1, totalRows)}-{Math.min(currentPage * pageSize, totalRows)}
              </p>
            </div>
          </div>

          <div className="flex items-center space-x-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-neutral-400" />
              <input
                type="text"
                placeholder="Search in current page..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            <select
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value));
                setCurrentPage(1);
              }}
              className="px-3 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500"
            >
              <option value={25}>25 rows</option>
              <option value={50}>50 rows</option>
              <option value={100}>100 rows</option>
              <option value={250}>250 rows</option>
            </select>
          </div>
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div className="bg-error-50 border border-error-200 rounded-lg p-4">
          <div className="flex items-start space-x-3">
            <AlertCircle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-error-900">Error Loading Data</p>
              <p className="text-sm text-error-700 mt-1">{error}</p>
            </div>
          </div>
        </div>
      )}

      {/* Data Table */}
      <div className="bg-white rounded-lg border border-neutral-200 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center h-96">
            <Loader2 className="w-8 h-8 animate-spin text-primary-600" />
          </div>
        ) : filteredData.length === 0 ? (
          <div className="flex items-center justify-center h-96">
            <div className="text-center">
              <Database className="w-12 h-12 text-neutral-400 mx-auto mb-4" />
              <p className="text-neutral-600">No data found</p>
              {searchTerm && (
                <p className="text-sm text-neutral-500 mt-2">
                  Try adjusting your search term
                </p>
              )}
            </div>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-neutral-50 border-b border-neutral-200">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-neutral-600 uppercase tracking-wider">
                    #
                  </th>
                  {columns.map((col) => (
                    <th
                      key={col}
                      className="px-4 py-3 text-left text-xs font-semibold text-neutral-600 uppercase tracking-wider"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-200">
                {filteredData.map((row, idx) => (
                  <tr key={idx} className="hover:bg-neutral-50 transition-colors">
                    <td className="px-4 py-3 text-sm text-neutral-500 font-mono">
                      {(currentPage - 1) * pageSize + idx + 1}
                    </td>
                    {columns.map((col) => (
                      <td key={col} className="px-4 py-3 text-sm text-neutral-900">
                        {row[col] === null ? (
                          <span className="text-neutral-400 italic">NULL</span>
                        ) : typeof row[col] === 'boolean' ? (
                          <span className={`px-2 py-1 rounded text-xs font-medium ${
                            row[col] ? 'bg-success-100 text-success-700' : 'bg-neutral-100 text-neutral-700'
                          }`}>
                            {row[col] ? 'TRUE' : 'FALSE'}
                          </span>
                        ) : typeof row[col] === 'object' ? (
                          <span className="text-xs font-mono text-neutral-500">
                            {JSON.stringify(row[col])}
                          </span>
                        ) : String(row[col]).length > 100 ? (
                          <span className="text-xs" title={String(row[col])}>
                            {String(row[col]).substring(0, 100)}...
                          </span>
                        ) : (
                          String(row[col])
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between bg-white border border-neutral-200 rounded-lg px-4 py-3">
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setCurrentPage(1)}
              disabled={currentPage === 1}
              className="px-3 py-1 text-sm border border-neutral-300 rounded hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              First
            </button>
            <button
              onClick={() => setCurrentPage(currentPage - 1)}
              disabled={currentPage === 1}
              className="p-2 border border-neutral-300 rounded hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
          </div>

          <div className="flex items-center space-x-2">
            <span className="text-sm text-neutral-600">
              Page {currentPage} of {totalPages}
            </span>
          </div>

          <div className="flex items-center space-x-2">
            <button
              onClick={() => setCurrentPage(currentPage + 1)}
              disabled={currentPage === totalPages}
              className="p-2 border border-neutral-300 rounded hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
            <button
              onClick={() => setCurrentPage(totalPages)}
              disabled={currentPage === totalPages}
              className="px-3 py-1 text-sm border border-neutral-300 rounded hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Last
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
