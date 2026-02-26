import { useState, useEffect } from 'react';
import { X, ArrowRight, AlertCircle, Check, Sparkles } from 'lucide-react';

export default function ColumnMappingDialog({ 
  isOpen, 
  onClose, 
  sourceTable,
  targetTable,
  sourceColumns,
  targetColumns,
  existingMappings = [],
  onSave 
}) {
  const [columnMappings, setColumnMappings] = useState([]);
  const [unmappedSource, setUnmappedSource] = useState([]);
  const [unmappedTarget, setUnmappedTarget] = useState([]);

  useEffect(() => {
    if (isOpen) {
      initializeMappings();
    }
  }, [isOpen, sourceColumns, targetColumns, existingMappings]);

  const initializeMappings = () => {
    // Start with existing mappings or create new ones
    if (existingMappings && existingMappings.length > 0) {
      setColumnMappings(existingMappings);
    } else {
      // Auto-map columns with exact name matches
      const autoMapped = [];
      const unmappedSrc = [...sourceColumns];
      const unmappedTgt = [...targetColumns];

      sourceColumns.forEach(srcCol => {
        const match = targetColumns.find(tgtCol => 
          tgtCol.name.toLowerCase() === srcCol.name.toLowerCase()
        );
        if (match) {
          autoMapped.push({
            source_column: srcCol.name,
            target_column: match.name,
            transform: null
          });
          unmappedSrc.splice(unmappedSrc.findIndex(c => c.name === srcCol.name), 1);
          unmappedTgt.splice(unmappedTgt.findIndex(c => c.name === match.name), 1);
        }
      });

      setColumnMappings(autoMapped);
      setUnmappedSource(unmappedSrc);
      setUnmappedTarget(unmappedTgt);
    }
  };

  const addMapping = (sourceCol, targetCol) => {
    setColumnMappings([
      ...columnMappings,
      {
        source_column: sourceCol,
        target_column: targetCol,
        transform: null
      }
    ]);
    setUnmappedSource(unmappedSource.filter(c => c.name !== sourceCol));
    setUnmappedTarget(unmappedTarget.filter(c => c.name !== targetCol));
  };

  const removeMapping = (index) => {
    const mapping = columnMappings[index];
    const srcCol = sourceColumns.find(c => c.name === mapping.source_column);
    const tgtCol = targetColumns.find(c => c.name === mapping.target_column);
    
    setColumnMappings(columnMappings.filter((_, idx) => idx !== index));
    if (srcCol) setUnmappedSource([...unmappedSource, srcCol]);
    if (tgtCol) setUnmappedTarget([...unmappedTarget, tgtCol]);
  };

  const updateTransform = (index, transform) => {
    const updated = [...columnMappings];
    updated[index].transform = transform || null;
    setColumnMappings(updated);
  };

  const handleSave = () => {
    onSave(columnMappings);
    onClose();
  };

  const autoMapSimilar = () => {
    // Try to auto-map similar names (e.g., user_id -> userId, email_address -> email)
    const newMappings = [...columnMappings];
    const newUnmappedSrc = [...unmappedSource];
    const newUnmappedTgt = [...unmappedTarget];

    unmappedSource.forEach(srcCol => {
      const srcName = srcCol.name.toLowerCase().replace(/_/g, '');
      const match = unmappedTarget.find(tgtCol => {
        const tgtName = tgtCol.name.toLowerCase().replace(/_/g, '');
        return tgtName.includes(srcName) || srcName.includes(tgtName);
      });

      if (match) {
        newMappings.push({
          source_column: srcCol.name,
          target_column: match.name,
          transform: null
        });
        newUnmappedSrc.splice(newUnmappedSrc.findIndex(c => c.name === srcCol.name), 1);
        newUnmappedTgt.splice(newUnmappedTgt.findIndex(c => c.name === match.name), 1);
      }
    });

    setColumnMappings(newMappings);
    setUnmappedSource(newUnmappedSrc);
    setUnmappedTarget(newUnmappedTgt);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-strong max-w-6xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-neutral-200">
          <div>
            <h2 className="text-2xl font-bold text-neutral-900">Map Columns</h2>
            <p className="text-neutral-600 mt-1">
              <span className="font-mono font-semibold text-primary-600">{sourceTable}</span>
              <ArrowRight className="inline w-4 h-4 mx-2" />
              <span className="font-mono font-semibold text-accent-600">{targetTable}</span>
            </p>
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
          {/* Auto-map button */}
          <div className="flex items-center justify-between bg-primary-50 border border-primary-200 rounded-lg p-4">
            <div className="flex items-center space-x-3">
              <Sparkles className="w-5 h-5 text-primary-600" />
              <div>
                <p className="font-medium text-primary-900">Smart Auto-Mapping</p>
                <p className="text-sm text-primary-700">
                  {columnMappings.length} of {sourceColumns.length} columns mapped
                </p>
              </div>
            </div>
            <button
              onClick={autoMapSimilar}
              disabled={unmappedSource.length === 0 || unmappedTarget.length === 0}
              className="px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Auto-Map Similar
            </button>
          </div>

          {/* Current Mappings */}
          <div>
            <h3 className="text-lg font-semibold text-neutral-900 mb-3">
              Current Mappings ({columnMappings.length})
            </h3>
            <div className="space-y-2">
              {columnMappings.length === 0 ? (
                <div className="text-center py-8 text-neutral-500">
                  No column mappings defined yet
                </div>
              ) : (
                columnMappings.map((mapping, idx) => (
                  <div
                    key={idx}
                    className="flex items-center space-x-3 p-3 bg-neutral-50 border border-neutral-200 rounded-lg"
                  >
                    <div className="flex-1 grid grid-cols-3 gap-4 items-center">
                      {/* Source */}
                      <div className="flex items-center space-x-2">
                        <div className="w-3 h-3 bg-primary-500 rounded-full"></div>
                        <span className="font-mono text-sm font-medium text-neutral-900">
                          {mapping.source_column}
                        </span>
                        <span className="text-xs text-neutral-500">
                          {sourceColumns.find(c => c.name === mapping.source_column)?.data_type}
                        </span>
                      </div>

                      {/* Transform (optional) */}
                      <div>
                        <input
                          type="text"
                          value={mapping.transform || ''}
                          onChange={(e) => updateTransform(idx, e.target.value)}
                          placeholder="Optional SQL transform..."
                          className="w-full px-3 py-1.5 text-sm border border-neutral-300 rounded focus:ring-2 focus:ring-primary-500"
                        />
                      </div>

                      {/* Target */}
                      <div className="flex items-center space-x-2">
                        <ArrowRight className="w-4 h-4 text-neutral-400" />
                        <div className="w-3 h-3 bg-accent-500 rounded-full"></div>
                        <span className="font-mono text-sm font-medium text-neutral-900">
                          {mapping.target_column}
                        </span>
                        <span className="text-xs text-neutral-500">
                          {targetColumns.find(c => c.name === mapping.target_column)?.data_type}
                        </span>
                      </div>
                    </div>

                    <button
                      onClick={() => removeMapping(idx)}
                      className="p-1.5 text-error-600 hover:bg-error-50 rounded transition-colors"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Unmapped Columns */}
          {(unmappedSource.length > 0 || unmappedTarget.length > 0) && (
            <div className="grid grid-cols-2 gap-6">
              {/* Unmapped Source */}
              <div>
                <h3 className="text-sm font-semibold text-neutral-700 mb-2">
                  Unmapped Source Columns ({unmappedSource.length})
                </h3>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {unmappedSource.map(col => (
                    <div
                      key={col.name}
                      className="p-2 bg-white border border-neutral-200 rounded text-sm"
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <span className="font-mono font-medium text-neutral-900">{col.name}</span>
                          <span className="text-xs text-neutral-500 ml-2">{col.data_type}</span>
                        </div>
                        <select
                          onChange={(e) => {
                            if (e.target.value) {
                              addMapping(col.name, e.target.value);
                              e.target.value = '';
                            }
                          }}
                          className="text-xs px-2 py-1 border border-neutral-300 rounded"
                          defaultValue=""
                        >
                          <option value="">Map to...</option>
                          {unmappedTarget.map(tgtCol => (
                            <option key={tgtCol.name} value={tgtCol.name}>
                              {tgtCol.name} ({tgtCol.data_type})
                            </option>
                          ))}
                        </select>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Unmapped Target */}
              <div>
                <h3 className="text-sm font-semibold text-neutral-700 mb-2">
                  Unmapped Target Columns ({unmappedTarget.length})
                </h3>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {unmappedTarget.map(col => (
                    <div
                      key={col.name}
                      className="p-2 bg-white border border-neutral-200 rounded text-sm"
                    >
                      <span className="font-mono font-medium text-neutral-900">{col.name}</span>
                      <span className="text-xs text-neutral-500 ml-2">{col.data_type}</span>
                      {col.nullable === false && (
                        <span className="ml-2 text-xs px-1.5 py-0.5 bg-error-100 text-error-700 rounded">
                          NOT NULL
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Warnings */}
          {unmappedTarget.filter(c => !c.nullable && !c.default).length > 0 && (
            <div className="bg-warning-50 border border-warning-200 rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <AlertCircle className="w-5 h-5 text-warning-600 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="font-medium text-warning-900">Required Columns Not Mapped</p>
                  <p className="text-sm text-warning-700 mt-1">
                    The following target columns are NOT NULL and have no default value:
                  </p>
                  <ul className="mt-2 space-y-1">
                    {unmappedTarget
                      .filter(c => !c.nullable && !c.default)
                      .map(col => (
                        <li key={col.name} className="text-sm font-mono text-warning-800">
                          • {col.name} ({col.data_type})
                        </li>
                      ))}
                  </ul>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t border-neutral-200 bg-neutral-50">
          <div className="text-sm text-neutral-600">
            <Check className="inline w-4 h-4 text-success-600 mr-1" />
            {columnMappings.length} mappings defined
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={onClose}
              className="px-4 py-2 border border-neutral-300 rounded-lg hover:bg-neutral-100 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              className="px-6 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
            >
              Save Mappings
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
