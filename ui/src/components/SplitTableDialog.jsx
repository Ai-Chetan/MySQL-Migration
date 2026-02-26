import { useState, useEffect } from 'react';
import { X, Plus, Trash2, AlertCircle, GitBranch } from 'lucide-react';

export default function SplitTableDialog({ 
  isOpen, 
  onClose, 
  sourceTable,
  sourceColumns,
  availableTargetTables,
  onSave 
}) {
  const [targetTables, setTargetTables] = useState([{ name: '', columns: [] }]);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen) {
      setTargetTables([{ name: '', columns: [] }]);
      setError(null);
    }
  }, [isOpen]);

  const addTargetTable = () => {
    setTargetTables([...targetTables, { name: '', columns: [] }]);
  };

  const removeTargetTable = (index) => {
    if (targetTables.length === 1) {
      setError('At least one target table is required for split operation');
      return;
    }
    setTargetTables(targetTables.filter((_, idx) => idx !== index));
  };

  const updateTargetName = (index, name) => {
    const updated = [...targetTables];
    updated[index].name = name;
    setTargetTables(updated);
  };

  const toggleColumn = (targetIndex, columnName) => {
    const updated = [...targetTables];
    const columns = updated[targetIndex].columns;
    
    if (columns.includes(columnName)) {
      updated[targetIndex].columns = columns.filter(c => c !== columnName);
    } else {
      updated[targetIndex].columns = [...columns, columnName];
    }
    
    setTargetTables(updated);
  };

  const selectAllColumns = (targetIndex) => {
    const updated = [...targetTables];
    updated[targetIndex].columns = sourceColumns.map(c => c.name);
    setTargetTables(updated);
  };

  const deselectAllColumns = (targetIndex) => {
    const updated = [...targetTables];
    updated[targetIndex].columns = [];
    setTargetTables(updated);
  };

  const validate = () => {
    setError(null);

    // Check all targets have names
    if (targetTables.some(t => !t.name)) {
      setError('All target tables must have a name');
      return false;
    }

    // Check for duplicate target names
    const names = targetTables.map(t => t.name);
    if (new Set(names).size !== names.length) {
      setError('Target table names must be unique');
      return false;
    }

    // Check all targets have at least one column
    if (targetTables.some(t => t.columns.length === 0)) {
      setError('Each target table must have at least one column selected');
      return false;
    }

    return true;
  };

  const handleSave = () => {
    if (!validate()) return;

    // Create column filters object
    const columnFilters = {};
    targetTables.forEach(target => {
      columnFilters[target.name] = target.columns;
    });

    onSave({
      source_table: sourceTable,
      target_tables: targetTables.map(t => t.name),
      column_filters: columnFilters
    });

    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-strong max-w-6xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-neutral-200">
          <div>
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                <GitBranch className="w-5 h-5 text-blue-600" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-neutral-900">Split Table</h2>
                <p className="text-neutral-600 mt-1">
                  Split <span className="font-mono font-semibold text-primary-600">{sourceTable}</span> into multiple target tables
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
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-start space-x-3">
              <AlertCircle className="w-5 h-5 text-blue-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-blue-800">
                <p className="font-medium">How Split Works:</p>
                <ul className="mt-2 space-y-1 list-disc list-inside">
                  <li>Data from the source table will be copied to each target table</li>
                  <li>Each target gets only the columns you select</li>
                  <li>All rows are copied (no filtering - same data in all targets)</li>
                  <li>Useful for normalizing wide tables or separating concerns</li>
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

          {/* Target Tables */}
          <div className="space-y-4">
            {targetTables.map((target, idx) => (
              <div key={idx} className="border-2 border-neutral-200 rounded-lg p-4 bg-neutral-50">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center space-x-3 flex-1">
                    <span className="text-sm font-semibold text-neutral-600">
                      Target {idx + 1}
                    </span>
                    <select
                      value={target.name}
                      onChange={(e) => updateTargetName(idx, e.target.value)}
                      className="flex-1 max-w-xs px-3 py-2 border border-neutral-300 rounded-lg focus:ring-2 focus:ring-primary-500 font-mono"
                    >
                      <option value="">Select target table...</option>
                      {availableTargetTables.map(table => (
                        <option key={table} value={table}>{table}</option>
                      ))}
                    </select>
                  </div>
                  
                  <button
                    onClick={() => removeTargetTable(idx)}
                    disabled={targetTables.length === 1}
                    className="p-2 text-error-600 hover:bg-error-50 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>

                {target.name && (
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <p className="text-sm font-medium text-neutral-700">
                        Select columns for {target.name}:
                      </p>
                      <div className="flex items-center space-x-2">
                        <button
                          onClick={() => selectAllColumns(idx)}
                          className="text-xs px-2 py-1 text-primary-600 hover:bg-primary-50 rounded transition-colors"
                        >
                          Select All
                        </button>
                        <button
                          onClick={() => deselectAllColumns(idx)}
                          className="text-xs px-2 py-1 text-neutral-600 hover:bg-neutral-100 rounded transition-colors"
                        >
                          Clear
                        </button>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-48 overflow-y-auto bg-white p-3 rounded border border-neutral-200">
                      {sourceColumns.map(col => (
                        <label
                          key={col.name}
                          className="flex items-center space-x-2 p-2 hover:bg-neutral-50 rounded cursor-pointer"
                        >
                          <input
                            type="checkbox"
                            checked={target.columns.includes(col.name)}
                            onChange={() => toggleColumn(idx, col.name)}
                            className="rounded border-neutral-300 text-primary-600 focus:ring-primary-500"
                          />
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-mono font-medium text-neutral-900 truncate">
                              {col.name}
                            </p>
                            <p className="text-xs text-neutral-500">{col.data_type}</p>
                          </div>
                        </label>
                      ))}
                    </div>

                    <p className="text-xs text-neutral-500 mt-2">
                      {target.columns.length} of {sourceColumns.length} columns selected
                    </p>
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Add Target Button */}
          <button
            onClick={addTargetTable}
            className="w-full flex items-center justify-center space-x-2 px-4 py-3 border-2 border-dashed border-neutral-300 rounded-lg text-neutral-600 hover:border-primary-500 hover:text-primary-600 hover:bg-primary-50 transition-colors"
          >
            <Plus className="w-5 h-5" />
            <span className="font-medium">Add Another Target Table</span>
          </button>

          {/* Summary */}
          {targetTables.every(t => t.name && t.columns.length > 0) && (
            <div className="bg-success-50 border border-success-200 rounded-lg p-4">
              <p className="font-medium text-success-900 mb-2">Split Summary:</p>
              <ul className="space-y-1 text-sm text-success-800">
                {targetTables.map((target, idx) => (
                  <li key={idx}>
                    • <span className="font-mono font-semibold">{target.name}</span>: {target.columns.length} columns
                  </li>
                ))}
              </ul>
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
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Create Split Mapping
          </button>
        </div>
      </div>
    </div>
  );
}
