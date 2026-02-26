"""Schema Migration Service - Multi-database schema migration and transformation."""
from services.api.schema_migration.connection_manager import ConnectionManager, DatabaseConnection
from services.api.schema_migration.schema_parser import SchemaParser, TableDefinition, ColumnDefinition
from services.api.schema_migration.schema_comparator import SchemaComparator, ComparisonResult
from services.api.schema_migration.mapping_engine import MappingEngine, TableMapping, MappingType
from services.api.schema_migration.data_type_analyzer import DataTypeAnalyzer, ConversionSafety
from services.api.schema_migration.migration_executor import MigrationExecutor
from services.api.schema_migration.script_generator import ScriptGenerator

__all__ = [
    'ConnectionManager',
    'DatabaseConnection',
    'SchemaParser',
    'TableDefinition',
    'ColumnDefinition',
    'SchemaComparator',
    'ComparisonResult',
    'MappingEngine',
    'TableMapping',
    'MappingType',
    'DataTypeAnalyzer',
    'ConversionSafety',
    'MigrationExecutor',
    'ScriptGenerator',
]
