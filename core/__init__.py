"""core/__init__.py"""
from core.database import DatabaseManager, DatabaseError, ConnectionLostError, TableSchema
from core.schema_parser import parse_schema_file, generate_create_table_sql, ParsedSchema
from core.type_converter import classify_conversion, ConversionSafety, get_base_type
from core.mapping_store import MappingStore
from core.migrator import MigrationEngine, MigrationResult, MigrationPlan, MigrationError

__all__ = [
    "DatabaseManager",
    "DatabaseError",
    "ConnectionLostError",
    "TableSchema",
    "parse_schema_file",
    "generate_create_table_sql",
    "ParsedSchema",
    "classify_conversion",
    "ConversionSafety",
    "get_base_type",
    "MappingStore",
    "MigrationEngine",
    "MigrationResult",
    "MigrationPlan",
    "MigrationError",
]
