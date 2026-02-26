"""
Schema Parser
Parses schema definition files and database schemas into structured format.
"""
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
import re
from shared.utils import setup_logger

logger = setup_logger(__name__)


@dataclass
class ColumnDefinition:
    """Represents a column definition."""
    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False
    auto_increment: bool = False
    default: Optional[str] = None
    extra: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'data_type': self.data_type,
            'nullable': self.nullable,
            'primary_key': self.primary_key,
            'auto_increment': self.auto_increment,
            'default': self.default,
            'extra': self.extra
        }
    
    def get_full_definition(self) -> str:
        """Get full SQL column definition."""
        parts = [self.name, self.data_type]
        
        if not self.nullable:
            parts.append("NOT NULL")
        
        if self.auto_increment:
            parts.append("AUTO_INCREMENT")
        
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        
        if self.primary_key:
            parts.append("PRIMARY KEY")
        
        if self.extra:
            parts.append(self.extra)
        
        return " ".join(parts)


@dataclass
class TableDefinition:
    """Represents a table definition."""
    name: str
    columns: List[ColumnDefinition] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'indexes': self.indexes,
            'constraints': self.constraints
        }
    
    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None


class SchemaParser:
    """Parses schema definition files."""
    
    @staticmethod
    def parse_schema_file(content: str) -> Dict[str, TableDefinition]:
        """
        Parse a schema file content into table definitions.
        
        Format:
        Table: TableName
          column_name DATA_TYPE [constraints]
          # Comment
          id INT AUTO_INCREMENT PRIMARY KEY
          name VARCHAR(100) NOT NULL
        
        Returns:
            Dictionary of table_name -> TableDefinition
        """
        tables: Dict[str, TableDefinition] = {}
        current_table: Optional[TableDefinition] = None
        
        lines = content.split('\n')
        
        for line in lines:
            # Remove inline comments
            if '#' in line:
                line = line[:line.index('#')]
            if '--' in line:
                line = line[:line.index('--')]
            
            line = line.strip()
            
            if not line:
                continue
            
            # Table definition
            if line.lower().startswith('table:'):
                table_name = line[6:].strip()
                current_table = TableDefinition(name=table_name)
                tables[table_name.lower()] = current_table
                logger.debug(f"Found table: {table_name}")
                continue
            
            # Column definition (indented)
            if current_table and (line.startswith('  ') or line.startswith('\t')):
                column = SchemaParser._parse_column_definition(line.strip())
                if column:
                    current_table.columns.append(column)
                    logger.debug(f"  Column: {column.name} {column.data_type}")
        
        return tables
    
    @staticmethod
    def _parse_column_definition(line: str) -> Optional[ColumnDefinition]:
        """Parse a single column definition line."""
        if not line:
            return None
        
        # Extract column name (first word)
        parts = line.split(None, 1)
        if len(parts) < 2:
            return None
        
        column_name = parts[0]
        rest = parts[1]
        
        # Extract data type (next word or until parenthesis)
        type_match = re.match(r'([A-Za-z]+(?:\([^)]+\))?)', rest)
        if not type_match:
            return None
        
        data_type = type_match.group(1)
        rest = rest[len(data_type):].strip()
        
        # Parse constraints
        nullable = True
        primary_key = False
        auto_increment = False
        default = None
        extra = ""
        
        rest_upper = rest.upper()
        
        if 'NOT NULL' in rest_upper:
            nullable = False
        
        if 'PRIMARY KEY' in rest_upper:
            primary_key = True
            nullable = False
        
        if 'AUTO_INCREMENT' in rest_upper or 'SERIAL' in rest_upper:
            auto_increment = True
        
        # Extract DEFAULT value
        default_match = re.search(r'DEFAULT\s+([^\s,]+)', rest, re.IGNORECASE)
        if default_match:
            default = default_match.group(1)
        
        # Store remaining as extra
        extra_parts = []
        if 'UNIQUE' in rest_upper:
            extra_parts.append('UNIQUE')
        if 'UNSIGNED' in rest_upper:
            extra_parts.append('UNSIGNED')
        
        extra = ' '.join(extra_parts)
        
        return ColumnDefinition(
            name=column_name,
            data_type=data_type,
            nullable=nullable,
            primary_key=primary_key,
            auto_increment=auto_increment,
            default=default,
            extra=extra
        )
    
    @staticmethod
    def from_database_schema(schema_info: List[Dict[str, Any]], table_name: str) -> TableDefinition:
        """
        Convert database schema info to TableDefinition.
        
        Args:
            schema_info: List of column info dicts from database
            table_name: Name of the table
        """
        table = TableDefinition(name=table_name)
        
        for col_info in schema_info:
            column = ColumnDefinition(
                name=col_info['name'],
                data_type=col_info['type'],
                nullable=col_info.get('nullable', True),
                primary_key=col_info.get('key', '') == 'PRI',
                auto_increment='auto_increment' in col_info.get('extra', '').lower(),
                default=col_info.get('default'),
                extra=col_info.get('extra', '')
            )
            table.columns.append(column)
        
        return table
    
    @staticmethod
    def normalize_type(data_type: str) -> str:
        """Normalize data type for comparison (remove size, case)."""
        # Remove size specifications
        data_type = re.sub(r'\([^)]+\)', '', data_type)
        # Remove unsigned, etc.
        data_type = re.sub(r'\s+(unsigned|signed|zerofill)', '', data_type, flags=re.IGNORECASE)
        return data_type.strip().upper()
