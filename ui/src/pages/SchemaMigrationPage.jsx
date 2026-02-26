import { useState, useEffect } from 'react';
import { Upload, Play, FileCode, Database, ArrowRight, CheckCircle2, AlertTriangle, XCircle, Eye, Download, GitBranch, GitMerge, Settings, RefreshCw, HelpCircle } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import apiClient from '../services/api';
import ColumnMappingDialog from '../components/ColumnMappingDialog';
import SplitTableDialog from '../components/SplitTableDialog';
import MergeTablesDialog from '../components/MergeTablesDialog';

export default function SchemaMigrationPage() {
  const navigate = useNavigate();
  const [connections, setConnections] = useState([]);
  const [sourceConn, setSourceConn] = useState('');
  const [targetConn, setTargetConn] = useState('');
  const [sourceTables, setSourceTables] = useState([]);
  const [schemaFile, setSchemaFile] = useState(null);
  const [parsedSchema, setParsedSchema] = useState(null);
  const [mappings, setMappings] = useState({});
  const [selectedTable, setSelectedTable] = useState(null);
  const [comparison, setComparison] = useState(null);
  const [loading, setLoading] = useState(false);
  const [step, setStep] = useState(1); // 1: Connect, 2: Load, 3: Map, 4: Execute

  // New dialog states
  const [showColumnMapping, setShowColumnMapping] = useState(false);
  const [showSplitDialog, setShowSplitDialog] = useState(false);
  const [showMergeDialog, setShowMergeDialog] = useState(false);
  
  // Manual confirmation checkboxes (matching reference's 6 checkboxes)
  const [confirmComparedSchemas, setConfirmComparedSchemas] = useState(false);
  const [confirmCheckedTypes, setConfirmCheckedTypes] = useState(false);
  const [confirmVerifiedMappings, setConfirmVerifiedMappings] = useState(false);
  const [confirmDefaultValues, setConfirmDefaultValues] = useState(false);
  const [confirmBackup, setConfirmBackup] = useState(false);
  const [confirmProceed, setConfirmProceed] = useState(false);
  
  // Schema data
  const [sourceSchema, setSourceSchema] = useState(null);
  const [targetSchema, setTargetSchema] = useState(null);
  const [showHelp, setShowHelp] = useState(false);

  useEffect(() => {
    loadConnections();
  }, []);

  const loadConnections = async () => {
    try {
      const response = await apiClient.get('/schema-migration/connections');
      setConnections(response.data.connections || []);
    } catch (error) {
      console.error('Failed to load connections:', error);
    }
  };

  const loadSourceTables = async () => {
    if (!sourceConn) return;
    
    setLoading(true);
    try {
      const response = await apiClient.get(`/schema-migration/connections/${sourceConn}/tables`);
      setSourceTables(response.data.tables || []);
      setStep(2);
    } catch (error) {
      console.error('Failed to load tables:', error);
      alert('Failed to load tables from database');
    } finally {
      setLoading(false);
    }
  };

  // Auto-mapping tables on schema load (matching reference behavior)
  const autoMapTables = () => {
    if (!sourceTables.length || !parsedSchema) return;
    
    let autoMapped = 0;
    sourceTables.forEach(sourceTable => {
      // Skip if already mapped
      if (mappings[sourceTable]) return;
      
      // Try to find matching schema table by name (case-insensitive)
      const matchingSchemaKey = Object.keys(parsedSchema).find(
        key => key.toLowerCase() === sourceTable.toLowerCase()
      );
      
      if (matchingSchemaKey) {
        const targetName = parsedSchema[matchingSchemaKey].name;
        // Create auto-mapping silently
        createSingleMapping(sourceTable, targetName);
        autoMapped++;
      }
    });
    
    if (autoMapped > 0) {
      console.log(`Auto-mapped ${autoMapped} table(s) by name matching`);
    }
  };

  const handleSchemaUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setSchemaFile(file);
    const formData = new FormData();
    formData.append('file', file);

    setLoading(true);
    try {
      const response = await apiClient.post('/schema-migration/schema/parse', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      setParsedSchema(response.data.tables);
      setStep(3);
      
      // Trigger auto-mapping after schema loads (matching reference behavior)
      setTimeout(() => autoMapTables(), 100);
    } catch (error) {
      console.error('Failed to parse schema:', error);
      alert('Failed to parse schema file');
    } finally {
      setLoading(false);
    }
  };

  const createSingleMapping = async (sourceTable, targetTable) => {
    try {
      const response = await apiClient.post('/schema-migration/mappings/single', {
        source_table: sourceTable,
        target_table: targetTable,
        column_mappings: null
      });

      setMappings(prev => ({
        ...prev,
        [sourceTable]: {
          type: 'single',
          target: targetTable,
          mapping_id: response.data.mapping_id
        }
      }));

      alert(`Mapped ${sourceTable} → ${targetTable}`);
    } catch (error) {
      console.error('Failed to create mapping:', error);
      alert('Failed to create mapping');
    }
  };

  const getTableColor = (tableName) => {
    const mapping = mappings[tableName];
    
    if (!mapping) {
      // Not mapped
      if (parsedSchema && parsedSchema[tableName.toLowerCase()]) {
        return 'border-orange-500 bg-orange-50'; // In schema but not mapped
      }
      return 'border-red-500 bg-red-50'; // Not mapped & not in schema
    }
    
    if (mapping.type === 'single') {
      // Check if target exists in schema
      if (!parsedSchema || !parsedSchema[mapping.target.toLowerCase()]) {
        return 'border-purple-500 bg-purple-50'; // Mapped but target not in schema
      }
      return 'border-blue-500 bg-blue-50'; // Mapped, in schema
    }
    
    if (mapping.type === 'split') {
      return 'border-teal-500 bg-teal-50'; // Split mapping
    }
    
    if (mapping.type === 'merge') {
      return 'border-green-500 bg-green-50'; // Merge mapping
    }
    
    return 'border-gray-300 bg-white';
  };

  const viewComparison = async (tableName) => {
    const mapping = mappings[tableName];
    if (!mapping || mapping.type !== 'single') return;

    setLoading(true);
    try {
      const response = await apiClient.post('/schema-migration/schema/compare', {
        conn_id: sourceConn,
        old_table: tableName,
        new_table: mapping.target,
        new_schema_tables: parsedSchema,
        column_mappings: null
      });

      setComparison(response.data.comparison);
      setSelectedTable(tableName);
    } catch (error) {
      console.error('Failed to compare schemas:', error);
      alert('Failed to compare schemas');
    } finally {
      setLoading(false);
    }
  };

  const executeMigration = async (mappingId) => {
    if (!confirm('Are you sure you want to execute this migration? This will create new tables and copy data.')) {
      return;
    }

    setLoading(true);
    try {
      const response = await apiClient.post('/schema-migration/migrate/execute', {
        conn_id: sourceConn,
        mapping_id: mappingId,
        batch_size: 5000,
        lossy_confirmed: false
      });

      if (response.data.success) {
        alert(`Migration completed! ${response.data.rows_copied} rows copied.`);
      } else if (response.data.requires_confirmation) {
        const confirmed = confirm(
          `This migration has lossy type conversions. ${response.data.message}\n\nDo you want to proceed?`
        );

        if (confirmed) {
          // Retry with confirmation
          const retryResponse = await apiClient.post('/schema-migration/migrate/execute', {
            conn_id: sourceConn,
            mapping_id: mappingId,
            batch_size: 5000,
            lossy_confirmed: true
          });

          if (retryResponse.data.success) {
            alert(`Migration completed! ${retryResponse.data.rows_copied} rows copied.`);
          }
        }
      } else {
        alert(`Migration failed: ${response.data.message}`);
      }
    } catch (error) {
      console.error('Migration failed:', error);
      alert('Migration failed: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  // New helper functions
  const getTableSchema = async (tableName) => {
    try {
      const response = await apiClient.get(`/schema-migration/connections/${sourceConn}/tables/${tableName}/schema`);
      return response.data.columns;
    } catch (error) {
      console.error('Failed to load schema:', error);
      return [];
    }
  };

  const openColumnMapping = async () => {
    if (!selectedTable || !mappings[selectedTable]) {
      alert('Please select a mapped table first');
      return;
    }

    const mapping = mappings[selectedTable];
    if (mapping.type !== 'single') {
      alert('Column mapping is only available for single table mappings');
      return;
    }

    // Load schemas
    const srcSchema = await getTableSchema(selectedTable);
    const tgtSchema = parsedSchema[mapping.target.toLowerCase()]?.columns || [];

    setSourceSchema(srcSchema);
    setTargetSchema(tgtSchema);
    setShowColumnMapping(true);
  };

  const saveColumnMappings = async (columnMappings) => {
    if (!selectedTable || !mappings[selectedTable]) return;

    try {
      await apiClient.put(`/schema-migration/mappings/${mappings[selectedTable].mapping_id}`, {
        column_mappings: columnMappings
      });

      alert('Column mappings saved successfully');
    } catch (error) {
      console.error('Failed to save column mappings:', error);
      alert('Failed to save column mappings');
    }
  };

  const handleSplitMapping = async (splitData) => {
    try {
      const response = await apiClient.post('/schema-migration/mappings/split', splitData);
      
      setMappings(prev => ({
        ...prev,
        [splitData.source_table]: {
          type: 'split',
          targets: splitData.target_tables,
          mapping_id: response.data.mapping_id
        }
      }));

      alert(`Split mapping created: ${splitData.source_table} → ${splitData.target_tables.join(', ')}`);
    } catch (error) {
      console.error('Failed to create split mapping:', error);
      alert('Failed to create split mapping: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleMergeMapping = async (mergeData) => {
    try {
      const response = await apiClient.post('/schema-migration/mappings/merge', mergeData);
      
      // Format merge key to match reference: "MERGE: source1, source2 -> target"
      const mergeKey = `MERGE: ${mergeData.source_tables.join(', ')} -> ${mergeData.target_table}`;
      
      setMappings(prev => ({
        ...prev,
        [mergeKey]: {
          type: 'merge',
          sources: mergeData.source_tables,
          target: mergeData.target_table,
          mapping_id: response.data.mapping_id
        }
      }));

      alert(`Merge mapping created: ${mergeKey}`);
    } catch (error) {
      console.error('Failed to create merge mapping:', error);
      alert('Failed to create merge mapping: ' + (error.response?.data?.detail || error.message));
    }
  };

  const generateManualScript = async () => {
    if (!selectedTable || !mappings[selectedTable]) {
      alert('Please select a mapped table first');
      return;
    }

    const mapping = mappings[selectedTable];
    
    try {
      const response = await apiClient.post('/schema-migration/migrate/generate-script', {
        conn_id: sourceConn,
        mapping_id: mapping.mapping_id
      }, {
        responseType: 'blob'
      });

      // Download the generated script
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `manual_migration_${selectedTable}.py`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);

      alert('Manual script downloaded successfully. Review and edit the TODO sections before running.');
    } catch (error) {
      console.error('Failed to generate script:', error);
      alert('Failed to generate manual script');
    }
  };

  const viewTableData = (tableName, isNew = false) => {
    navigate(`/schema/data-viewer?connection=${sourceConn}&table=${tableName}&type=${isNew ? 'new' : 'old'}`);
  };

  // Refresh all: DB + schema + mappings (matching reference)
  const refreshAll = async () => {
    if (!sourceConn) return;
    
    setLoading(true);
    try {
      // Reload tables
      const tablesResponse = await apiClient.get(`/schema-migration/connections/${sourceConn}/tables`);
      setSourceTables(tablesResponse.data.tables || []);
      
      // Re-parse schema if file exists
      if (schemaFile) {
        const formData = new FormData();
        formData.append('file', schemaFile);
        const schemaResponse = await apiClient.post('/schema-migration/schema/parse', formData, {
          headers: { 'Content-Type': 'multipart/form-data' }
        });
        setParsedSchema(schemaResponse.data.tables);
      }
      
      // Trigger auto-mapping
      setTimeout(() => autoMapTables(), 100);
      
      alert('Refreshed: Database tables and schema reloaded');
    } catch (error) {
      console.error('Failed to refresh:', error);
      alert('Failed to refresh data');
    } finally {
      setLoading(false);
    }
  };

  // All 6 confirmation checkboxes must be checked (matching reference)
  const canExecuteMigration = confirmComparedSchemas && confirmCheckedTypes && 
    confirmVerifiedMappings && confirmDefaultValues && confirmBackup && confirmProceed;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Schema Migration Tool</h1>
          <p className="text-gray-600 mt-1">Migrate and transform database schemas</p>
        </div>
        <div className="flex gap-2">
          {step >= 3 && (
            <button
              onClick={refreshAll}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 text-blue-600 border border-blue-600 rounded-lg hover:bg-blue-50 disabled:opacity-50"
              title="Refresh DB & Schema"
            >
              <RefreshCw size={16} />
              Refresh
            </button>
          )}
          <button
            onClick={() => setShowHelp(true)}
            className="flex items-center gap-2 px-4 py-2 text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50"
            title="Help"
          >
            <HelpCircle size={16} />
            Help
          </button>
        </div>
      </div>

      {/* Progress Steps */}
      <div className="flex items-center gap-4">
        {[
          { num: 1, label: 'Connect' },
          { num: 2, label: 'Load Schema' },
          { num: 3, label: 'Map Tables' },
          { num: 4, label: 'Execute' }
        ].map((s, idx) => (
          <div key={s.num} className="flex items-center">
            <div className={`flex items-center gap-2 px-4 py-2 rounded-lg ${
              step >= s.num ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-500'
            }`}>
              <div className={`w-6 h-6 rounded-full flex items-center justify-center text-sm font-bold ${
                step >= s.num ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-600'
              }`}>
                {s.num}
              </div>
              <span className="font-medium">{s.label}</span>
            </div>
            {idx < 3 && <ArrowRight className="mx-2 text-gray-400" size={20} />}
          </div>
        ))}
      </div>

      {/* Step 1: Connection Selection */}
      {step === 1 && (
        <div className="bg-white border rounded-lg p-6">
          <h2 className="text-lg font-semibold mb-4">Step 1: Select Database Connection</h2>
          
          <div className="grid md:grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Source Database
              </label>
              <select
                value={sourceConn}
                onChange={(e) => setSourceConn(e.target.value)}
                className="w-full px-3 py-2 border rounded-lg"
              >
                <option value="">Select connection...</option>
                {connections.map(conn => (
                  <option key={conn.id} value={conn.id}>
                    {conn.name} ({conn.db_type})
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Target Database (same as source for in-place)
              </label>
              <select
                value={targetConn}
                onChange={(e) => setTargetConn(e.target.value)}
                className="w-full px-3 py-2 border rounded-lg"
              >
                <option value="">Select connection...</option>
                {connections.map(conn => (
                  <option key={conn.id} value={conn.id}>
                    {conn.name} ({conn.db_type})
                  </option>
                ))}
              </select>
            </div>
          </div>

          <button
            onClick={loadSourceTables}
            disabled={!sourceConn || loading}
            className="mt-6 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'Loading...' : 'Continue'}
          </button>
        </div>
      )}

      {/* Step 2: Schema Upload */}
      {step === 2 && (
        <div className="bg-white border rounded-lg p-6">
          <h2 className="text-lg font-semibold mb-4">Step 2: Upload Schema File</h2>
          
          <p className="text-gray-600 mb-4">
            Found {sourceTables.length} tables in source database. Upload your new schema definition file.
          </p>

          <div className="border-2 border-dashed rounded-lg p-8 text-center">
            <Upload size={48} className="mx-auto text-gray-400 mb-3" />
            <p className="text-gray-600 mb-4">Upload schema definition file (.txt)</p>
            <input
              type="file"
              accept=".txt,.sql"
              onChange={handleSchemaUpload}
              className="hidden"
              id="schema-upload"
            />
            <label
              htmlFor="schema-upload"
              className="inline-block px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 cursor-pointer"
            >
              Browse Files
            </label>
            {schemaFile && (
              <p className="mt-3 text-sm text-gray-600">Selected: {schemaFile.name}</p>
            )}
          </div>
        </div>
      )}

      {/* Step 3: Table Mapping */}
      {step >= 3 && (
        <div className="grid lg:grid-cols-3 gap-6">
          {/* Source Tables */}
          <div className="bg-white border rounded-lg p-4">
            <h3 className="font-semibold mb-3">Source Tables ({sourceTables.length})</h3>
            <div className="space-y-2 max-h-[500px] overflow-y-auto">
              {sourceTables.map(table => (
                <div
                  key={table}
                  className={`p-3 border-2 rounded-lg cursor-pointer hover:shadow-md transition ${getTableColor(table)}`}
                  onClick={() => setSelectedTable(table)}
                  onDoubleClick={() => viewTableData(table, false)}
                  title="Double-click to view data"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{table}</span>
                    {mappings[table] && (
                      <CheckCircle2 size={16} className="text-green-600" />
                    )}
                  </div>
                  {mappings[table] && (
                    <div className="text-xs text-gray-600 mt-1">
                      {mappings[table].type === 'split' ? (
                        <>→ {mappings[table].targets.join(', ')}</>
                      ) : (
                        <>→ {mappings[table].target}</>
                      )}
                    </div>
                  )}
                </div>
              ))}
              
              {/* Show merge operations */}
              {Object.entries(mappings).filter(([key]) => key.startsWith('MERGE:')).map(([key, mapping]) => (
                <div
                  key={key}
                  className="p-3 border-2 border-emerald-500 bg-emerald-50 rounded-lg cursor-pointer hover:shadow-md transition"
                  onClick={() => setSelectedTable(key)}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-emerald-900">{key}</span>
                    <GitMerge size={16} className="text-emerald-600" />
                  </div>
                  <div className="text-xs text-emerald-700 mt-1">
                    {mapping.sources.join(' + ')} → {mapping.target}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Schema Tables */}
          <div className="bg-white border rounded-lg p-4">
            <h3 className="font-semibold mb-3">
              New Schema Tables ({parsedSchema ? Object.keys(parsedSchema).length : 0})
            </h3>
            <div className="space-y-2 max-h-[500px] overflow-y-auto">
              {parsedSchema && Object.entries(parsedSchema).map(([name, def]) => (
                <div
                  key={name}
                  className="p-3 border rounded-lg hover:bg-gray-50"
                >
                  <div className="font-medium">{def.name}</div>
                  <div className="text-xs text-gray-600 mt-1">
                    {def.columns.length} columns
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Mapping Actions */}
          <div className="bg-white border rounded-lg p-4">
            <h3 className="font-semibold mb-3">Mapping Actions</h3>
            
            {selectedTable && !selectedTable.startsWith('[MERGE]') ? (
              <div className="space-y-3">
                <div className="p-3 bg-blue-50 rounded-lg">
                  <p className="text-sm font-medium">Selected: {selectedTable}</p>
                </div>

                {/* Single Table Mapping */}
                {!mappings[selectedTable] && (
                  <div className="space-y-2">
                    <label className="block text-sm font-medium text-gray-700">
                      Map to New Table:
                    </label>
                    <select
                      className="w-full px-3 py-2 border rounded-lg"
                      onChange={(e) => {
                        if (e.target.value) {
                          createSingleMapping(selectedTable, e.target.value);
                        }
                      }}
                    >
                      <option value="">Select target table...</option>
                      {parsedSchema && Object.keys(parsedSchema).map(name => (
                        <option key={name} value={parsedSchema[name].name}>
                          {parsedSchema[name].name}
                        </option>
                      ))}
                    </select>
                  </div>
                )}

                {/* Actions for mapped tables */}
                {mappings[selectedTable] && mappings[selectedTable].type === 'single' && (
                  <>
                    <button
                      onClick={openColumnMapping}
                      className="w-full flex items-center justify-center gap-2 px-4 py-2 border-2 border-primary-300 text-primary-700 rounded-lg hover:bg-primary-50 transition-colors"
                    >
                      <Settings size={16} />
                      Map Columns
                    </button>

                    <button
                      onClick={() => viewComparison(selectedTable)}
                      className="w-full flex items-center justify-center gap-2 px-4 py-2 border rounded-lg hover:bg-gray-50"
                    >
                      <Eye size={16} />
                      View Schema Comparison
                    </button>

                    <button
                      onClick={() => viewTableData(selectedTable, false)}
                      className="w-full flex items-center justify-center gap-2 px-4 py-2 border rounded-lg hover:bg-gray-50"
                    >
                      <Database size={16} />
                      View Old Table Data
                    </button>

                    <button
                      onClick={generateManualScript}
                      className="w-full flex items-center justify-center gap-2 px-4 py-2 border-2 border-purple-300 text-purple-700 rounded-lg hover:bg-purple-50 transition-colors"
                    >
                      <FileCode size={16} />
                      Generate Manual Script
                    </button>

                    {/* Manual Confirmation Checkboxes - All 6 from reference */}
                    <div className="pt-3 border-t space-y-2">
                      <p className="text-xs font-semibold text-gray-700">Manual Confirmation Gate:</p>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmComparedSchemas}
                          onChange={(e) => setConfirmComparedSchemas(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Compared Schemas?</span>
                      </label>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmCheckedTypes}
                          onChange={(e) => setConfirmCheckedTypes(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Checked Data Types?</span>
                      </label>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmVerifiedMappings}
                          onChange={(e) => setConfirmVerifiedMappings(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Verified Mappings?</span>
                      </label>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmDefaultValues}
                          onChange={(e) => setConfirmDefaultValues(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Checked default values?</span>
                      </label>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmBackup}
                          onChange={(e) => setConfirmBackup(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Database Backed Up?</span>
                      </label>
                      <label className="flex items-start space-x-2 text-xs">
                        <input
                          type="checkbox"
                          checked={confirmProceed}
                          onChange={(e) => setConfirmProceed(e.target.checked)}
                          className="mt-0.5 rounded border-gray-300"
                        />
                        <span>Proceed with Create?</span>
                      </label>
                    </div>

                    <button
                      onClick={() => executeMigration(mappings[selectedTable].mapping_id)}
                      disabled={loading || !canExecuteMigration}
                      className="w-full px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                    >
                      <Play size={16} />
                      {loading ? 'Executing...' : 'CREATE New Table(s) & Copy Data'}
                    </button>
                  </>
                )}

                {/* Actions for split mappings */}
                {mappings[selectedTable] && mappings[selectedTable].type === 'split' && (
                  <>
                    <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                      <p className="text-xs font-medium text-blue-900">Split Mapping</p>
                      <p className="text-xs text-blue-700 mt-1">
                        → {mappings[selectedTable].targets.join(', ')}
                      </p>
                    </div>

                    <button
                      onClick={() => executeMigration(mappings[selectedTable].mapping_id)}
                      disabled={loading || !canExecuteMigration}
                      className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                    >
                      <GitBranch size={16} />
                      Execute Split Migration
                    </button>
                  </>
                )}
              </div>
            ) : selectedTable && selectedTable.startsWith('MERGE:') ? (
              <div className="space-y-3">
                <div className="p-3 bg-emerald-50 rounded-lg">
                  <p className="text-sm font-medium">Merge Operation</p>
                  <p className="text-xs text-gray-600 mt-1">
                    {mappings[selectedTable]?.sources.join(' + ')} → {mappings[selectedTable]?.target}
                  </p>
                </div>

                <button
                  onClick={() => executeMigration(mappings[selectedTable].mapping_id)}
                  disabled={loading || !canExecuteMigration}
                  className="w-full px-4 py-2 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                >
                  <GitMerge size={16} />
                  Execute Merge Migration
                </button>
              </div>
            ) : (
              <div>
                <p className="text-sm text-gray-600 mb-4">
                  Select a source table to start mapping, or create advanced mappings:
                </p>
                
                <div className="space-y-2">
                  <button
                    onClick={async () => {
                      if (selectedTable) {
                        const schema = await getTableSchema(selectedTable);
                        setSourceSchema(schema);
                      }
                      setShowSplitDialog(true);
                    }}
                    disabled={!parsedSchema}
                    className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <GitBranch size={16} />
                    Split Table...
                  </button>

                  <button
                    onClick={() => setShowMergeDialog(true)}
                    disabled={!parsedSchema}
                    className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <GitMerge size={16} />
                    Merge Tables...
                  </button>
                </div>
              </div>
            )}

            {/* Legend */}
            <div className="mt-6 pt-4 border-t">
              <p className="text-xs font-medium text-gray-700 mb-2">Color Legend:</p>
              <div className="space-y-1 text-xs">
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-red-500 bg-red-50 rounded"></div>
                  <span>Not mapped & not in schema</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-orange-500 bg-orange-50 rounded"></div>
                  <span>In schema, not mapped</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-blue-500 bg-blue-50 rounded"></div>
                  <span>Single mapping</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-teal-500 bg-teal-50 rounded"></div>
                  <span>Split operation</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-green-500 bg-green-50 rounded"></div>
                  <span>Merge operation</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Schema Comparison Modal */}
      {comparison && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg p-6 max-w-4xl w-full max-h-[80vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-bold">Schema Comparison</h2>
              <button
                onClick={() => setComparison(null)}
                className="text-gray-500 hover:text-gray-700"
              >
                ✕
              </button>
            </div>

            <div className="space-y-4">
              {comparison.column_comparisons.map((col, idx) => (
                <div
                  key={idx}
                  className={`p-3 rounded-lg ${
                    col.change_type === 'matching' ? 'bg-gray-100' :
                    col.change_type === 'changed' ? 'bg-yellow-50' :
                    col.change_type === 'renamed' ? 'bg-blue-50' :
                    col.change_type === 'added' ? 'bg-green-50' :
                    'bg-pink-50'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <span className="font-medium">
                        {col.old_column?.name || col.new_column?.name}
                      </span>
                      {col.change_type === 'renamed' && (
                        <span className="text-sm text-gray-600 ml-2">
                          → {col.new_column?.name}
                        </span>
                      )}
                    </div>
                    <span className="text-xs font-medium px-2 py-1 rounded-full bg-white">
                      {col.change_type.toUpperCase()}
                    </span>
                  </div>
                  
                  {col.differences.length > 0 && (
                    <div className="mt-2 text-sm text-gray-700">
                      {col.differences.map((diff, i) => (
                        <div key={i} className="flex items-start gap-2">
                          <AlertTriangle size={14} className="mt-0.5 text-yellow-600" />
                          <span>{diff}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Column Mapping Dialog */}
      <ColumnMappingDialog
        isOpen={showColumnMapping}
        onClose={() => setShowColumnMapping(false)}
        sourceTable={selectedTable}
        targetTable={mappings[selectedTable]?.target}
        sourceColumns={sourceSchema || []}
        targetColumns={targetSchema || []}
        onSave={saveColumnMappings}
      />

      {/* Split Table Dialog */}
      <SplitTableDialog
        isOpen={showSplitDialog}
        onClose={() => setShowSplitDialog(false)}
        sourceTable={selectedTable}
        sourceColumns={sourceSchema || []}
        availableTargetTables={parsedSchema ? Object.keys(parsedSchema).map(k => parsedSchema[k].name) : []}
        onSave={handleSplitMapping}
      />

      {/* Merge Tables Dialog */}
      <MergeTablesDialog
        isOpen={showMergeDialog}
        onClose={() => setShowMergeDialog(false)}
        availableSourceTables={sourceTables}
        availableTargetTables={parsedSchema ? Object.keys(parsedSchema).map(k => parsedSchema[k].name) : []}
        getTableSchema={getTableSchema}
        onSave={handleMergeMapping}
      />

      {/* Help Modal */}
      {showHelp && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg p-6 max-w-3xl w-full max-h-[80vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-bold">Schema Migration Tool - Help</h2>
              <button
                onClick={() => setShowHelp(false)}
                className="text-gray-500 hover:text-gray-700"
              >
                ✕
              </button>
            </div>

            <div className="space-y-4 text-sm">
              <section>
                <h3 className="font-semibold text-lg mb-2">Overview</h3>
                <p className="text-gray-700">
                  This tool helps you migrate database schemas with support for single, split, and merge table operations.
                  It compares schemas, maps columns, checks data type conversions, and safely migrates data in batches.
                </p>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Workflow</h3>
                <ol className="list-decimal list-inside space-y-2 text-gray-700">
                  <li><strong>Step 1:</strong> Select a source database connection</li>
                  <li><strong>Step 2:</strong> Upload your new schema definition file (.txt format)</li>
                  <li><strong>Step 3:</strong> Map tables using single, split, or merge operations</li>
                  <li><strong>Step 4:</strong> Review schemas, verify mappings, and execute migration</li>
                </ol>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Mapping Types</h3>
                <ul className="space-y-2 text-gray-700">
                  <li><strong>Single Mapping:</strong> One old table → one new table (1:1 migration)</li>
                  <li><strong>Split Table:</strong> One old table → multiple new tables (1:N split by columns)</li>
                  <li><strong>Merge Tables:</strong> Multiple old tables → one new table (N:1 with JOIN conditions)</li>
                </ul>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Color Legend</h3>
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-red-500 bg-red-50 rounded"></div>
                    <span className="text-gray-700">Not mapped & not in schema</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-orange-500 bg-orange-50 rounded"></div>
                    <span className="text-gray-700">In schema but not mapped yet</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-purple-500 bg-purple-50 rounded"></div>
                    <span className="text-gray-700">Mapped but target not in schema</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-blue-500 bg-blue-50 rounded"></div>
                    <span className="text-gray-700">Single mapping (valid)</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-teal-500 bg-teal-50 rounded"></div>
                    <span className="text-gray-700">Split operation</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-green-500 bg-green-50 rounded"></div>
                    <span className="text-gray-700">Merge operation</span>
                  </div>
                </div>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Safety Features</h3>
                <ul className="list-disc list-inside space-y-1 text-gray-700">
                  <li>Schema comparison highlights differences (matching, changed, renamed, added, removed)</li>
                  <li>Data type conversion safety analysis (safe, lossy, unsafe)</li>
                  <li>6-checkpoint confirmation gate before execution</li>
                  <li>Batch processing (5000 rows per batch) with automatic rollback on errors</li>
                  <li>Creates tables with "_new" suffix to preserve originals</li>
                  <li>Manual script generation for complex scenarios</li>
                </ul>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Tips</h3>
                <ul className="list-disc list-inside space-y-1 text-gray-700">
                  <li><strong>Auto-mapping:</strong> Tables with matching names are automatically mapped on schema load</li>
                  <li><strong>Double-click:</strong> Double-click any source table to quickly view its data</li>
                  <li><strong>Refresh:</strong> Use the Refresh button to reload database and schema after external changes</li>
                  <li><strong>Column mapping:</strong> Configure custom column mappings for advanced transformations</li>
                  <li><strong>Merge JOINs:</strong> Auto-generate JOIN conditions based on primary/foreign key detection</li>
                </ul>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">Schema File Format</h3>
                <pre className="bg-gray-100 p-3 rounded text-xs overflow-x-auto">
{`# Comments start with # or --
Table: users
id INT PRIMARY KEY AUTO_INCREMENT
username VARCHAR(50) NOT NULL
email VARCHAR(100) UNIQUE
created_at DATETIME DEFAULT CURRENT_TIMESTAMP

Table: orders
order_id INT PRIMARY KEY
user_id INT NOT NULL
total DECIMAL(10,2)
status ENUM('pending','completed','cancelled')`}
                </pre>
              </section>

              <section>
                <h3 className="font-semibold text-lg mb-2">⚠️ Important Reminders</h3>
                <div className="bg-yellow-50 border border-yellow-200 rounded p-3 space-y-1 text-gray-700">
                  <p><strong>• Always backup your database before executing migrations</strong></p>
                  <p>• Review schema comparisons and data type conversions carefully</p>
                  <p>• Test migrations on a development database first</p>
                  <p>• Lossy conversions (e.g., VARCHAR → INT) may cause data loss</p>
                  <p>• Use manual script generation for complex or sensitive migrations</p>
                </div>
              </section>
            </div>

            <div className="mt-6 flex justify-end">
              <button
                onClick={() => setShowHelp(false)}
                className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Got it!
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
