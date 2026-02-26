"""
Mapping Engine
Manages table and column mappings for migration (single, split, merge).
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from shared.utils import setup_logger

logger = setup_logger(__name__)


class MappingType(str, Enum):
    """Types of table mappings."""
    SINGLE = "single"  # One old table -> one new table
    SPLIT = "split"  # One old table -> multiple new tables
    MERGE = "merge"  # Multiple old tables -> one new table


@dataclass
class ColumnMapping:
    """Maps columns between old and new schemas."""
    source_column: str
    target_column: str
    source_table: Optional[str] = None  # For merge operations
    transform: Optional[str] = None  # Optional SQL transformation


@dataclass
class TableMapping:
    """Represents a table mapping configuration."""
    mapping_id: str
    mapping_type: MappingType
    source_tables: List[str]
    target_tables: List[str]
    column_mappings: Dict[str, List[ColumnMapping]] = field(default_factory=dict)  # target_table -> mappings
    join_conditions: Optional[List[str]] = None  # For merge operations
    notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'mapping_id': self.mapping_id,
            'mapping_type': self.mapping_type.value,
            'source_tables': self.source_tables,
            'target_tables': self.target_tables,
            'column_mappings': {
                table: [
                    {
                        'source_column': cm.source_column,
                        'target_column': cm.target_column,
                        'source_table': cm.source_table,
                        'transform': cm.transform
                    }
                    for cm in mappings
                ]
                for table, mappings in self.column_mappings.items()
            },
            'join_conditions': self.join_conditions,
            'notes': self.notes
        }


class MappingEngine:
    """Manages all table and column mappings."""
    
    def __init__(self):
        self.mappings: Dict[str, TableMapping] = {}  # mapping_id -> TableMapping
        self._source_to_mapping: Dict[str, str] = {}  # source_table -> mapping_id
    
    def add_single_mapping(
        self,
        mapping_id: str,
        source_table: str,
        target_table: str,
        column_mappings: Optional[Dict[str, str]] = None
    ) -> TableMapping:
        """
        Add a single table mapping (1-to-1).
        
        Args:
            mapping_id: Unique ID for this mapping
            source_table: Source table name
            target_table: Target table name
            column_mappings: Dict of source_col -> target_col
        """
        # Convert column mappings to ColumnMapping objects
        col_map_objects = []
        if column_mappings:
            for source_col, target_col in column_mappings.items():
                col_map_objects.append(ColumnMapping(
                    source_column=source_col,
                    target_column=target_col
                ))
        
        mapping = TableMapping(
            mapping_id=mapping_id,
            mapping_type=MappingType.SINGLE,
            source_tables=[source_table],
            target_tables=[target_table],
            column_mappings={target_table: col_map_objects} if col_map_objects else {}
        )
        
        self.mappings[mapping_id] = mapping
        self._source_to_mapping[source_table] = mapping_id
        
        logger.info(f"Added single mapping: {source_table} -> {target_table}")
        return mapping
    
    def add_split_mapping(
        self,
        mapping_id: str,
        source_table: str,
        target_tables: List[str],
        column_mappings: Dict[str, Dict[str, str]]
    ) -> TableMapping:
        """
        Add a split table mapping (1-to-many).
        
        Args:
            mapping_id: Unique ID for this mapping
            source_table: Source table name
            target_tables: List of target table names
            column_mappings: Dict of target_table -> {source_col -> target_col}
        """
        # Convert column mappings
        col_map_objects = {}
        for target_table, mappings in column_mappings.items():
            col_map_objects[target_table] = [
                ColumnMapping(source_column=src, target_column=tgt)
                for src, tgt in mappings.items()
            ]
        
        mapping = TableMapping(
            mapping_id=mapping_id,
            mapping_type=MappingType.SPLIT,
            source_tables=[source_table],
            target_tables=target_tables,
            column_mappings=col_map_objects
        )
        
        self.mappings[mapping_id] = mapping
        self._source_to_mapping[source_table] = mapping_id
        
        logger.info(f"Added split mapping: {source_table} -> {target_tables}")
        return mapping
    
    def add_merge_mapping(
        self,
        mapping_id: str,
        source_tables: List[str],
        target_table: str,
        column_mappings: List[ColumnMapping],
        join_conditions: List[str]
    ) -> TableMapping:
        """
        Add a merge table mapping (many-to-1).
        
        Args:
            mapping_id: Unique ID for this mapping
            source_tables: List of source table names
            target_table: Target table name
            column_mappings: List of ColumnMapping objects (with source_table specified)
            join_conditions: SQL JOIN conditions (e.g., ["INNER JOIN t2 ON t1.id=t2.t1_id"])
        """
        mapping = TableMapping(
            mapping_id=mapping_id,
            mapping_type=MappingType.MERGE,
            source_tables=source_tables,
            target_tables=[target_table],
            column_mappings={target_table: column_mappings},
            join_conditions=join_conditions
        )
        
        self.mappings[mapping_id] = mapping
        
        # Associate all source tables with this mapping
        for source_table in source_tables:
            self._source_to_mapping[source_table] = mapping_id
        
        logger.info(f"Added merge mapping: {source_tables} -> {target_table}")
        return mapping
    
    def get_mapping(self, mapping_id: str) -> Optional[TableMapping]:
        """Get mapping by ID."""
        return self.mappings.get(mapping_id)
    
    def get_mapping_for_source(self, source_table: str) -> Optional[TableMapping]:
        """Get mapping for a source table."""
        mapping_id = self._source_to_mapping.get(source_table)
        if mapping_id:
            return self.mappings.get(mapping_id)
        return None
    
    def remove_mapping(self, mapping_id: str):
        """Remove a mapping."""
        if mapping_id in self.mappings:
            mapping = self.mappings[mapping_id]
            
            # Remove from source lookup
            for source_table in mapping.source_tables:
                if source_table in self._source_to_mapping:
                    del self._source_to_mapping[source_table]
            
            del self.mappings[mapping_id]
            logger.info(f"Removed mapping: {mapping_id}")
    
    def list_mappings(self) -> List[Dict[str, Any]]:
        """List all mappings."""
        return [m.to_dict() for m in self.mappings.values()]
    
    def update_column_mappings(
        self,
        mapping_id: str,
        target_table: str,
        column_mappings: List[ColumnMapping]
    ):
        """Update column mappings for a specific target table."""
        if mapping_id in self.mappings:
            self.mappings[mapping_id].column_mappings[target_table] = column_mappings
            logger.info(f"Updated column mappings for {mapping_id} -> {target_table}")
    
    def generate_merge_column_mappings(
        self,
        source_schemas: Dict[str, List[Dict[str, Any]]],  # table_name -> columns
        target_schema: List[Dict[str, Any]]
    ) -> List[ColumnMapping]:
        """
        Auto-generate column mappings for merge operation (best effort).
        
        Returns:
            List of ColumnMapping objects (user must verify!)
        """
        mappings = []
        
        # Get all source columns
        all_source_cols = {}
        for table_name, columns in source_schemas.items():
            for col in columns:
                col_name = col['name']
                all_source_cols[col_name.lower()] = (table_name, col_name)
        
        # Try to match target columns
        for target_col in target_schema:
            target_name = target_col['name']
            target_name_lower = target_name.lower()
            
            # Direct name match
            if target_name_lower in all_source_cols:
                source_table, source_col = all_source_cols[target_name_lower]
                mappings.append(ColumnMapping(
                    source_column=source_col,
                    target_column=target_name,
                    source_table=source_table
                ))
            else:
                # No match - user must fill in
                mappings.append(ColumnMapping(
                    source_column="???",  # Placeholder
                    target_column=target_name,
                    source_table="???"
                ))
        
        return mappings
    
    def validate_merge_mapping(self, mapping: TableMapping) -> List[str]:
        """
        Validate a merge mapping configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if not mapping.join_conditions:
            errors.append("Join conditions are required for merge operations")
        
        if not mapping.column_mappings:
            errors.append("Column mappings are required for merge operations")
        
        # Check for missing source tables in column mappings
        for target_table, col_mappings in mapping.column_mappings.items():
            for col_map in col_mappings:
                if not col_map.source_table:
                    errors.append(f"Column {col_map.target_column} missing source table")
                elif col_map.source_table not in mapping.source_tables:
                    errors.append(f"Source table {col_map.source_table} not in mapping")
        
        return errors
    
    def clear_all(self):
        """Clear all mappings."""
        self.mappings.clear()
        self._source_to_mapping.clear()
        logger.info("Cleared all mappings")
