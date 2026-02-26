import { useState, useEffect } from 'react';
import { X, Plus, Trash2, AlertCircle, GitMerge, ArrowRight, Sparkles } from 'lucide-react';

export default function MergeTablesDialog({ 
  isOpen, 
  onClose, 
  availableSourceTables,
  availableTargetTables,
  getTableSchema,
  onSave 
}) {
  const [sourceTables, setSourceTables] = useState(['', '']);
  const [targetTable, setTargetTable] = useState('');
  const [joinConditions, setJoinConditions] = useState(['']);
  const [columnMappings, setColumnMappings] = useState([]);
  const [schemas, setSchemas] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen) {
      reset();
    }
  }, [isOpen]);

  const reset = () => {
    setSourceTables(['', '']);
    setTargetTable('');
    setJoinConditions(['']);
    setColumnMappings([]);
    setSchemas({});
    setError(null);
  };

  const addSourceTable = () => {
    setSourceTables([...sourceTables, '']);
  };

  const removeSourceTable = (index) => {
    if (sourceTables.length === 2) {
      setError('At least two source tables are required for merge');
      return;
    }
    const updated = sourceTables.filter((_, idx) => idx !== index);
    setSourceTables(updated);
    
    // Remove related join conditions and mappings
    if (index > 0) {
      setJoinConditions(joinConditions.filter((_, idx) => idx !== index - 1));
    }
  };

  const updateSourceTable = async (index, tableName) => {
    const updated = [...sourceTables];
    updated[index] = tableName;
    setSourceTables(updated);

    // Load schema for this table
    if (tableName) {
      await loadTableSchema(tableName);
    }

    // Adjust join conditions array
    if (updated.filter(t => t).length > 1) {
      const neededJoins = updated.filter(t => t).length - 1;
      setJoinConditions(new Array(neededJoins).fill(''));
    }
  };

  const updateJoinCondition = (index, condition) => {
    const updated = [...joinConditions];
    updated[index] = condition;
    setJoinConditions(updated);
  };

  const loadTableSchema = async (tableName) => {
    if (!tableName || schemas[tableName]) return;

    try {
      const schema = await getTableSchema(tableName);
      setSchemas({ ...schemas, [tableName]: schema });
    } catch (err) {
      console.error(`Failed to load schema for ${tableName}:`, err);
    }
  };

  const autoGenerateMappings = () => {
    if (!targetTable) {
      setError('Please select a target table first');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Load target schema first
      loadTableSchema(targetTable).then(() => {
        const newMappings = [];
        const validSources = sourceTables.filter(t => t && schemas[t]);

        // For each source table, try to map columns to target
        validSources.forEach(sourceTable => {
          const sourceSchema = schemas[sourceTable];
          const targetSchema = schemas[targetTable];

          if (!targetSchema) return;

          sourceSchema.forEach(sourceCol => {
            // Try exact match first
            const exactMatch = targetSchema.find(
              tCol => tCol.name.toLowerCase() === sourceCol.name.toLowerCase()
            );

            if (exactMatch) {
              // Check if not already mapped
              const alreadyMapped = newMappings.some(
                m => m.target_column === exactMatch.name
              );
              if (!alreadyMapped) {
                newMappings.push({
                  source_table: sourceTable,
                  source_column: sourceCol.name,
                  target_column: exactMatch.name,
                  transform: null
                });
              }
            }
          });
        });

        setColumnMappings(newMappings);
        setLoading(false);
      });
    } catch (err) {
      setError('Failed to auto-generate mappings');
      setLoading(false);
    }
  };

  const addColumnMapping = () => {
    setColumnMappings([
      ...columnMappings,
      {
        source_table: sourceTables[0] || '',
        source_column: '',
        target_column: '',
        transform: null
      }
    ]);
  };

  const updateColumnMapping = (index, field, value) => {
    const updated = [...columnMappings];
    updated[index][field] = value;
    setColumnMappings(updated);
  };

  const removeColumnMapping = (index) => {
    setColumnMappings(columnMappings.filter((_, idx) => idx !== index));
  };

  const validate = () => {
    setError(null);

    // Check source tables
    const validSources = sourceTables.filter(t => t);
    if (validSources.length < 2) {
      setError('At least 2 source tables are required');
      return false;
    }

    // Check for duplicate sources
    if (new Set(validSources).size !== validSources.length) {
      setError('Source tables must be unique');
      return false;
    }

    // Check target table
    if (!targetTable) {
      setError('Target table is required');
      return false;
    }

    // Check join conditions
    const neededJoins = validSources.length - 1;
    const validJoins = joinConditions.slice(0, neededJoins).filter(j => j.trim());
    if (validJoins.length !== neededJoins) {
      setError(`${neededJoins} JOIN condition(s) required for ${validSources.length} tables`);
      return false;
    }

    // Check column mappings
    if (columnMappings.length === 0) {
      setError('At least one column mapping is required');
      return false;
    }

    // Validate each mapping
    for (const mapping of columnMappings) {
      if (!mapping.source_table || !mapping.source_column || !mapping.target_column) {
        setError('All column mappings must have source table, source column, and target column');
        return false;
      }
    }

    return true;
  };

  const handleSave = () => {
    if (!validate()) return;

    const validSources = sourceTables.filter(t => t);
    const validJoins = joinConditions.slice(0, validSources.length - 1);

    onSave({
      source_tables: validSources,
      target_table: targetTable,
      join_conditions: validJoins,
      column_mappings: columnMappings
    });

    onClose();
  };

  if (!isOpen) return null;

  const validSources = sourceTables.filter(t => t);
  const targetSchema = schemas[targetTable] || [];

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-strong max-w-7xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-neutral-200">
          <div>
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 bg-emerald-100 rounded-lg flex items-center justify-center">
                <GitMerge className="w-5 h-5 text-emerald-600" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-neutral-900">Merge Tables</h2>
                <p className="text-neutral-600 mt-1">
                  Combine multiple source tables into one target table using JOIN conditions
                </p>
              </div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-neutral-100 rounded-lg transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Info */}
          <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-4">
            <div className="flex items-start space-x-3">
              <AlertCircle className="w-5 h-5 text-emerald-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-emerald-800">
                <p className="font-medium">How Merge Works:</p>
                <ul className="mt-2 space-y-1 list-disc list-inside">
                  <li>Multiple source tables are combined using JOIN conditions</li>
                  <li>Columns from any source table can map to the target</li>
                  <li>Only rows matching JOIN conditions will be included</li>
                  <li>Use for denormalization or combining related data</li>
                </ul>
              </div>
            </div>
          </div>

          {/* Error Display */}
          {error && (
            <div className="bg-error-50 border border-error-200 rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <AlertCircle className="w-5 h-5 text-error-600 flex-shrink-0 mt-0.5" />
                <p className="text-sm text-error-800">{error}</p>
              </div>
            </div>
          )}

          {/* Source Tables & JOIN Conditions */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold text-neutral-900">Source Tables & JOINs</h3>
            
            {sourceTables.map((table, idx) => (
              <div key={idx}>
                <div className="flex items-center space-x-3">
                  <span className="text-sm font-semibold text-neutral-600 w-20">
                    Table {idx + 1}:
                  </span>
                  <select
                    value={table}
                    onChange={(e) => updateSourceTable(idx, e.target.value)}
                    className="flex-1 max-w-sm px-3 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 font-mono"
                  >
                    <option value="">Select source table...</option>
                    {availableSourceTables.map(t => (
                      <option key={t} value={t}>{t}</option>
                    ))}
                  </select>
                  {sourceTables.length > 2 && (
                    <button
                      onClick={() => removeSourceTable(idx)}
                      className="p-2 text-error-600 hover:bg-error-50 rounded-lg transition-colors"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  )}
                </div>

                {/* JOIN condition after first table */}
                {idx > 0 && idx <= joinConditions.length && (
                  <div className="ml-24 mt-2 flex items-center space-x-3">
                    <span className="text-sm font-medium text-neutral-700">JOIN ON:</span>
                    <input
                      type="text"
                      value={joinConditions[idx - 1] || ''}
                      onChange={(e) => updateJoinCondition(idx - 1, e.target.value)}
                      placeholder={`e.g., ${sourceTables[0] || 't1'}.id = ${table || 't2'}.${sourceTables[0] || 't1'}_id`}
                      className="flex-1 px-3 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 font-mono text-sm"
                    />
                  </div>
                )}
              </div>
            ))}

            <button
              onClick={addSourceTable}
              className="flex items-center space-x-2 px-4 py-2 border-2 border-dashed border-neutral-300 rounded-lg text-neutral-600 hover:border-primary-500 hover:text-primary-600 hover:bg-primary-50 transition-colors"
            >
              <Plus className="w-4 h-4" />
              <span>Add Source Table</span>
            </button>
          </div>

          {/* Target Table */}
          <div>
            <h3 className="text-lg font-semibold text-neutral-900 mb-3">Target Table</h3>
            <select
              value={targetTable}
              onChange={(e) => {
                setTargetTable(e.target.value);
                if (e.target.value) loadTableSchema(e.target.value);
              }}
              className="w-full max-w-sm px-3 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 font-mono"
            >
              <option value="">Select target table...</option>
              {availableTargetTables.map(t => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          {/* Column Mappings */}
          {validSources.length >= 2 && targetTable && (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-neutral-900">
                  Column Mappings ({columnMappings.length})
                </h3>
                <button
                  onClick={autoGenerateMappings}
                  disabled={loading}
                  className="flex items-center space-x-2 px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 transition-colors"
                >
                  <Sparkles className="w-4 h-4" />
                  <span>Auto-Generate</span>
                </button>
              </div>

              {columnMappings.length === 0 ? (
                <div className="text-center py-8 border-2 border-dashed border-neutral-300 rounded-lg">
                  <p className="text-neutral-600">No column mappings defined</p>
                  <p className="text-sm text-neutral-500 mt-1">
                    Click "Auto-Generate" or add manually
                  </p>
                </div>
              ) : (
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {columnMappings.map((mapping, idx) => (
                    <div
                      key={idx}
                      className="flex items-center space-x-3 p-3 bg-neutral-50 border border-neutral-200 rounded-lg"
                    >
                      {/* Source Table */}
                      <select
                        value={mapping.source_table}
                        onChange={(e) => updateColumnMapping(idx, 'source_table', e.target.value)}
                        className="w-32 px-2 py-1.5 text-sm border border-neutral-300 rounded font-mono"
                      >
                        <option value="">Table...</option>
                        {validSources.map(t => (
                          <option key={t} value={t}>{t}</option>
                        ))}
                      </select>

                      {/* Source Column */}
                      <select
                        value={mapping.source_column}
                        onChange={(e) => updateColumnMapping(idx, 'source_column', e.target.value)}
                        className="flex-1 px-2 py-1.5 text-sm border border-neutral-300 rounded font-mono"
                      >
                        <option value="">Source column...</option>
                        {mapping.source_table && schemas[mapping.source_table]?.map(col => (
                          <option key={col.name} value={col.name}>
                            {col.name} ({col.data_type})
                          </option>
                        ))}
                      </select>

                      <ArrowRight className="w-4 h-4 text-neutral-400" />

                      {/* Target Column */}
                      <select
                        value={mapping.target_column}
                        onChange={(e) => updateColumnMapping(idx, 'target_column', e.target.value)}
                        className="flex-1 px-2 py-1.5 text-sm border border-neutral-300 rounded font-mono"
                      >
                        <option value="">Target column...</option>
                        {targetSchema.map(col => (
                          <option key={col.name} value={col.name}>
                            {col.name} ({col.data_type})
                          </option>
                        ))}
                      </select>

                      {/* Transform */}
                      <input
                        type="text"
                        value={mapping.transform || ''}
                        onChange={(e) => updateColumnMapping(idx, 'transform', e.target.value)}
                        placeholder="Transform..."
                        className="w-32 px-2 py-1.5 text-sm border border-neutral-300 rounded font-mono"
                      />

                      <button
                        onClick={() => removeColumnMapping(idx)}
                        className="p-1.5 text-error-600 hover:bg-error-50 rounded transition-colors"
                      >
                        <X className="w-4 h-4" />
                      </button>
                    </div>
                  ))}
                </div>
              )}

              <button
                onClick={addColumnMapping}
                className="w-full flex items-center justify-center space-x-2 px-4 py-2 border-2 border-dashed border-neutral-300 rounded-lg text-neutral-600 hover:border-primary-500 hover:text-primary-600 hover:bg-primary-50 transition-colors"
              >
                <Plus className="w-4 h-4" />
                <span>Add Column Mapping</span>
              </button>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end space-x-3 p-6 border-t border-neutral-200 bg-neutral-50">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-neutral-300 rounded-lg hover:bg-neutral-100 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-6 py-2 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 transition-colors"
          >
            Create Merge Mapping
          </button>
        </div>
      </div>
    </div>
  );
}
