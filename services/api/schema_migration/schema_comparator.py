"""
Schema Comparator
Compares database schemas and identifies differences.
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
from services.api.schema_migration.schema_parser import TableDefinition, ColumnDefinition, SchemaParser
from shared.utils import setup_logger

logger = setup_logger(__name__)


class ChangeType(str, Enum):
    """Types of schema changes."""
    MATCHING = "matching"  # Same in both
    CHANGED = "changed"  # Definition changed
    RENAMED = "renamed"  # Name changed but mapped
    ADDED = "added"  # Only in new schema
    REMOVED = "removed"  # Only in old schema


@dataclass
class ColumnComparison:
    """Comparison result for a single column."""
    old_column: Optional[ColumnDefinition]
    new_column: Optional[ColumnDefinition]
    change_type: ChangeType
    differences: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'old_column': self.old_column.to_dict() if self.old_column else None,
            'new_column': self.new_column.to_dict() if self.new_column else None,
            'change_type': self.change_type.value,
            'differences': self.differences
        }


@dataclass
class ComparisonResult:
    """Complete comparison result for a table."""
    old_table: Optional[TableDefinition]
    new_table: Optional[TableDefinition]
    column_comparisons: List[ColumnComparison]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'old_table': self.old_table.to_dict() if self.old_table else None,
            'new_table': self.new_table.to_dict() if self.new_table else None,
            'column_comparisons': [cc.to_dict() for cc in self.column_comparisons]
        }


class SchemaComparator:
    """Compares database schemas."""
    
    @staticmethod
    def compare_tables(
        old_table: TableDefinition,
        new_table: TableDefinition,
        column_mappings: Optional[Dict[str, str]] = None
    ) -> ComparisonResult:
        """
        Compare two table definitions.
        
        Args:
            old_table: Old table definition
            new_table: New table definition
            column_mappings: Optional dict of old_col_name -> new_col_name
        
        Returns:
            ComparisonResult with detailed differences
        """
        column_mappings = column_mappings or {}
        reverse_mappings = {v: k for k, v in column_mappings.items()}
        
        comparisons: List[ColumnComparison] = []
        processed_old = set()
        processed_new = set()
        
        # Compare mapped columns
        for old_name, new_name in column_mappings.items():
            old_col = old_table.get_column(old_name)
            new_col = new_table.get_column(new_name)
            
            if old_col and new_col:
                comparison = SchemaComparator._compare_columns(old_col, new_col, is_renamed=True)
                comparisons.append(comparison)
                processed_old.add(old_name.lower())
                processed_new.add(new_name.lower())
        
        # Compare columns with same names
        for old_col in old_table.columns:
            if old_col.name.lower() in processed_old:
                continue
            
            # Skip if already mapped to different name
            if old_col.name in column_mappings:
                continue
            
            new_col = new_table.get_column(old_col.name)
            if new_col and new_col.name.lower() not in processed_new:
                comparison = SchemaComparator._compare_columns(old_col, new_col, is_renamed=False)
                comparisons.append(comparison)
                processed_old.add(old_col.name.lower())
                processed_new.add(new_col.name.lower())
        
        # Add removed columns (in old, not in new)
        for old_col in old_table.columns:
            if old_col.name.lower() not in processed_old:
                comparisons.append(ColumnComparison(
                    old_column=old_col,
                    new_column=None,
                    change_type=ChangeType.REMOVED,
                    differences=["Column removed from new schema"]
                ))
        
        # Add new columns (in new, not in old)
        for new_col in new_table.columns:
            if new_col.name.lower() not in processed_new:
                comparisons.append(ColumnComparison(
                    old_column=None,
                    new_column=new_col,
                    change_type=ChangeType.ADDED,
                    differences=["Column added in new schema"]
                ))
        
        return ComparisonResult(
            old_table=old_table,
            new_table=new_table,
            column_comparisons=comparisons
        )
    
    @staticmethod
    def _compare_columns(
        old_col: ColumnDefinition,
        new_col: ColumnDefinition,
        is_renamed: bool
    ) -> ColumnComparison:
        """Compare two column definitions."""
        differences = []
        
        # Normalize types for comparison
        old_type_norm = SchemaParser.normalize_type(old_col.data_type)
        new_type_norm = SchemaParser.normalize_type(new_col.data_type)
        
        # Check for differences
        if old_type_norm != new_type_norm:
            differences.append(f"Type changed: {old_col.data_type} → {new_col.data_type}")
        
        if old_col.nullable != new_col.nullable:
            null_str = "NULL" if new_col.nullable else "NOT NULL"
            differences.append(f"Nullability changed to {null_str}")
        
        if old_col.primary_key != new_col.primary_key:
            if new_col.primary_key:
                differences.append("Added PRIMARY KEY")
            else:
                differences.append("Removed PRIMARY KEY")
        
        if old_col.auto_increment != new_col.auto_increment:
            if new_col.auto_increment:
                differences.append("Added AUTO_INCREMENT")
            else:
                differences.append("Removed AUTO_INCREMENT")
        
        if old_col.default != new_col.default:
            differences.append(f"Default changed: {old_col.default} → {new_col.default}")
        
        # Determine change type
        if is_renamed:
            change_type = ChangeType.RENAMED
        elif differences:
            change_type = ChangeType.CHANGED
        else:
            change_type = ChangeType.MATCHING
        
        return ColumnComparison(
            old_column=old_col,
            new_column=new_col,
            change_type=change_type,
            differences=differences
        )
    
    @staticmethod
    def get_table_status(
        old_tables: List[str],
        new_schema_tables: List[str],
        mappings: Dict[str, Any],
        generated_tables: List[str]
    ) -> Dict[str, str]:
        """
        Determine color status for each table in the old/source list.
        
        Returns:
            Dict of table_name -> status_color
        """
        statuses = {}
        
        for table in old_tables:
            # Check if mapped
            mapped = table in mappings
            
            if not mapped:
                # Not mapped
                if table in new_schema_tables:
                    statuses[table] = "orange"  # In schema but not mapped
                else:
                    statuses[table] = "red"  # Not mapped & not in schema
            else:
                # Mapped
                mapping_info = mappings[table]
                mapping_type = mapping_info.get('type')
                
                if mapping_type == 'split':
                    # Split mapping
                    targets = mapping_info.get('targets', [])
                    all_in_schema = all(t in new_schema_tables for t in targets)
                    all_generated = all(f"{t}_new" in generated_tables for t in targets)
                    
                    if not all_in_schema:
                        statuses[table] = "purple"  # Mapped but targets not in schema
                    elif all_generated:
                        statuses[table] = "black"  # All done
                    else:
                        statuses[table] = "darkcyan"  # Split, not all generated
                
                elif mapping_type == 'merge':
                    statuses[table] = "darkgreen"  # Merge mapping
                
                else:
                    # Single mapping
                    target = mapping_info.get('target')
                    if target not in new_schema_tables:
                        statuses[table] = "purple"  # Mapped but target not in schema
                    elif f"{target}_new" in generated_tables:
                        statuses[table] = "black"  # Done
                    else:
                        statuses[table] = "blue"  # Mapped, in schema, not generated
        
        return statuses
