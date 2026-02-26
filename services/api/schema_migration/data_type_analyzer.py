"""
Data Type Analyzer
Analyzes data type conversions for safety and compatibility.
"""
from typing import Tuple, Optional
from enum import Enum
from services.api.schema_migration.schema_parser import SchemaParser
from shared.utils import setup_logger

logger = setup_logger(__name__)


class ConversionSafety(str, Enum):
    """Safety level of data type conversion."""
    SAFE = "safe"  # Generally safe, no data loss expected
    LOSSY = "lossy"  # May lose precision or truncate
    UNSAFE = "unsafe"  # Likely to fail or cause errors
    UNKNOWN = "unknown"  # Cannot determine


class DataTypeAnalyzer:
    """Analyzes data type conversions between schemas."""
    
    # Type hierarchy for MySQL/PostgreSQL (simplified)
    NUMERIC_TYPES = {
        'TINYINT': 1,
        'SMALLINT': 2,
        'MEDIUMINT': 3,
        'INT': 4,
        'INTEGER': 4,
        'BIGINT': 5,
        'FLOAT': 6,
        'DOUBLE': 7,
        'DECIMAL': 8,
        'NUMERIC': 8
    }
    
    STRING_TYPES = {
        'CHAR': 1,
        'VARCHAR': 2,
        'TINYTEXT': 3,
        'TEXT': 4,
        'MEDIUMTEXT': 5,
        'LONGTEXT': 6
    }
    
    DATE_TYPES = {
        'DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'YEAR'
    }
    
    BLOB_TYPES = {
        'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'BINARY', 'VARBINARY'
    }
    
    @staticmethod
    def analyze_conversion(
        source_type: str,
        target_type: str
    ) -> Tuple[ConversionSafety, str]:
        """
        Analyze the safety of converting from source_type to target_type.
        
        Returns:
            (ConversionSafety, reason/explanation)
        """
        # Normalize types
        source_norm = SchemaParser.normalize_type(source_type).upper()
        target_norm = SchemaParser.normalize_type(target_type).upper()
        
        # Same type -> SAFE
        if source_norm == target_norm:
            return ConversionSafety.SAFE, "Same data type"
        
        # Numeric conversions
        if source_norm in DataTypeAnalyzer.NUMERIC_TYPES and target_norm in DataTypeAnalyzer.NUMERIC_TYPES:
            return DataTypeAnalyzer._analyze_numeric_conversion(source_norm, target_norm)
        
        # String conversions
        if source_norm in DataTypeAnalyzer.STRING_TYPES and target_norm in DataTypeAnalyzer.STRING_TYPES:
            return DataTypeAnalyzer._analyze_string_conversion(source_norm, target_norm, source_type, target_type)
        
        # Date/Time conversions
        if source_norm in DataTypeAnalyzer.DATE_TYPES and target_norm in DataTypeAnalyzer.DATE_TYPES:
            return DataTypeAnalyzer._analyze_datetime_conversion(source_norm, target_norm)
        
        # Blob/Binary conversions
        if source_norm in DataTypeAnalyzer.BLOB_TYPES and target_norm in DataTypeAnalyzer.BLOB_TYPES:
            return ConversionSafety.SAFE, "Compatible binary types"
        
        # Cross-category conversions
        
        # Numeric to String -> Usually SAFE
        if source_norm in DataTypeAnalyzer.NUMERIC_TYPES and target_norm in DataTypeAnalyzer.STRING_TYPES:
            return ConversionSafety.SAFE, "Numeric to string conversion (automatic casting)"
        
        # String to Numeric -> UNSAFE (likely to fail if non-numeric data)
        if source_norm in DataTypeAnalyzer.STRING_TYPES and target_norm in DataTypeAnalyzer.NUMERIC_TYPES:
            return ConversionSafety.UNSAFE, "String to numeric conversion (may fail on non-numeric data)"
        
        # Date to String -> SAFE
        if source_norm in DataTypeAnalyzer.DATE_TYPES and target_norm in DataTypeAnalyzer.STRING_TYPES:
            return ConversionSafety.SAFE, "Date to string conversion (automatic casting)"
        
        # String to Date -> LOSSY (depends on format)
        if source_norm in DataTypeAnalyzer.STRING_TYPES and target_norm in DataTypeAnalyzer.DATE_TYPES:
            return ConversionSafety.LOSSY, "String to date conversion (depends on format compatibility)"
        
        # Boolean/ENUM conversions
        if source_norm in ('BOOLEAN', 'BOOL', 'ENUM', 'SET'):
            if target_norm in DataTypeAnalyzer.STRING_TYPES:
                return ConversionSafety.SAFE, "Boolean/Enum to string (automatic casting)"
            elif target_norm in DataTypeAnalyzer.NUMERIC_TYPES:
                return ConversionSafety.LOSSY, "Boolean/Enum to numeric (may lose meaning)"
        
        # JSON conversions
        if source_norm == 'JSON':
            if target_norm in DataTypeAnalyzer.STRING_TYPES:
                return ConversionSafety.SAFE, "JSON to string conversion"
            else:
                return ConversionSafety.UNSAFE, "JSON to non-string type"
        
        # Unknown conversion
        return ConversionSafety.UNKNOWN, f"Conversion from {source_type} to {target_type} not categorized"
    
    @staticmethod
    def _analyze_numeric_conversion(source_norm: str, target_norm: str) -> Tuple[ConversionSafety, str]:
        """Analyze numeric type conversion."""
        source_rank = DataTypeAnalyzer.NUMERIC_TYPES[source_norm]
        target_rank = DataTypeAnalyzer.NUMERIC_TYPES[target_norm]
        
        if target_rank >= source_rank:
            return ConversionSafety.SAFE, f"Widening conversion ({source_norm} to {target_norm})"
        else:
            # Narrowing conversion
            if 'FLOAT' in source_norm or 'DOUBLE' in source_norm or 'DECIMAL' in source_norm:
                return ConversionSafety.LOSSY, f"Narrowing conversion with precision loss ({source_norm} to {target_norm})"
            else:
                return ConversionSafety.LOSSY, f"Narrowing conversion, may truncate ({source_norm} to {target_norm})"
    
    @staticmethod
    def _analyze_string_conversion(
        source_norm: str,
        target_norm: str,
        source_type: str,
        target_type: str
    ) -> Tuple[ConversionSafety, str]:
        """Analyze string type conversion."""
        # Extract sizes if present
        import re
        source_size = None
        target_size = None
        
        source_match = re.search(r'\((\d+)\)', source_type)
        if source_match:
            source_size = int(source_match.group(1))
        
        target_match = re.search(r'\((\d+)\)', target_type)
        if target_match:
            target_size = int(target_match.group(1))
        
        # If both have sizes, compare
        if source_size and target_size:
            if target_size >= source_size:
                return ConversionSafety.SAFE, f"String size increased ({source_size} to {target_size})"
            else:
                return ConversionSafety.LOSSY, f"String size reduced ({source_size} to {target_size}), may truncate"
        
        # Compare hierarchy
        source_rank = DataTypeAnalyzer.STRING_TYPES.get(source_norm, 0)
        target_rank = DataTypeAnalyzer.STRING_TYPES.get(target_norm, 0)
        
        if target_rank >= source_rank:
            return ConversionSafety.SAFE, f"String type widening ({source_norm} to {target_norm})"
        else:
            return ConversionSafety.LOSSY, f"String type narrowing ({source_norm} to {target_norm}), may truncate"
    
    @staticmethod
    def _analyze_datetime_conversion(source_norm: str, target_norm: str) -> Tuple[ConversionSafety, str]:
        """Analyze date/time type conversion."""
        # DATETIME and TIMESTAMP are similar
        if {source_norm, target_norm} <= {'DATETIME', 'TIMESTAMP'}:
            return ConversionSafety.SAFE, "Compatible datetime types"
        
        # DATE conversions
        if source_norm == 'DATE':
            if target_norm in ('DATETIME', 'TIMESTAMP'):
                return ConversionSafety.SAFE, "Date to datetime (time set to 00:00:00)"
            elif target_norm == 'TIME':
                return ConversionSafety.UNSAFE, "Date to time conversion (incompatible)"
        
        # DATETIME/TIMESTAMP to DATE
        if source_norm in ('DATETIME', 'TIMESTAMP') and target_norm == 'DATE':
            return ConversionSafety.LOSSY, "Datetime to date (time portion discarded)"
        
        # TIME conversions
        if source_norm == 'TIME':
            if target_norm in ('DATETIME', 'TIMESTAMP'):
                return ConversionSafety.UNSAFE, "Time to datetime (no date portion)"
        
        return ConversionSafety.UNKNOWN, f"Date/time conversion {source_norm} to {target_norm}"
    
    @staticmethod
    def requires_cast(source_type: str, target_type: str) -> Tuple[bool, Optional[str]]:
        """
        Determine if a CAST is needed in SQL.
        
        Returns:
            (needs_cast, cast_expression or None)
        """
        safety, _ = DataTypeAnalyzer.analyze_conversion(source_type, target_type)
        
        source_norm = SchemaParser.normalize_type(source_type).upper()
        target_norm = SchemaParser.normalize_type(target_type).upper()
        
        if source_norm == target_norm:
            return False, None
        
        if safety == ConversionSafety.SAFE:
            # Even if safe, some conversions need explicit CAST
            if source_norm in DataTypeAnalyzer.NUMERIC_TYPES and target_norm in DataTypeAnalyzer.STRING_TYPES:
                return True, f"CAST({{col}} AS {target_type})"
            elif source_norm in DataTypeAnalyzer.DATE_TYPES and target_norm in DataTypeAnalyzer.STRING_TYPES:
                return True, f"CAST({{col}} AS {target_type})"
        
        if safety in (ConversionSafety.LOSSY, ConversionSafety.UNKNOWN):
            return True, f"CAST({{col}} AS {target_type})"
        
        return False, None
