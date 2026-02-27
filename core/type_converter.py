"""
core/type_converter.py
----------------------
Data type analysis and conversion safety classification for MySQL migrations.

Classifies any old→new type pairing as:
    SAFE   – Will succeed without data loss (e.g. INT → BIGINT).
    LOSSY  – Will succeed but may truncate or lose precision
             (e.g. FLOAT → INT, VARCHAR(255) → VARCHAR(50)).
    UNSAFE – Likely to fail or produce silently wrong data
             (e.g. TEXT → INT, DATETIME → BINARY).

Design Decision:
    Pure functions with no side effects make this module trivially testable.
    The classification table encodes domain knowledge as data (sets + a
    simple priority model) rather than a deeply nested if/else tree.
"""
from __future__ import annotations

import re
from enum import Enum


class ConversionSafety(str, Enum):
    SAFE = "safe"
    LOSSY = "lossy"
    UNSAFE = "unsafe"


# ---------------------------------------------------------------------------
# Type category sets
# ---------------------------------------------------------------------------
_INTEGER_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)
_APPROX_NUMERIC = frozenset({"float", "double", "real"})
_EXACT_NUMERIC = frozenset({"decimal", "numeric", "fixed"})
_NUMERIC_TYPES = _INTEGER_TYPES | _APPROX_NUMERIC | _EXACT_NUMERIC
_STRING_TYPES = frozenset(
    {"char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set"}
)
_DATETIME_TYPES = frozenset({"date", "datetime", "timestamp", "time", "year"})
_BINARY_TYPES = frozenset(
    {"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bit"}
)
_JSON_TYPE = frozenset({"json"})

_CAT_MAP = (
    ("int",   _INTEGER_TYPES),
    ("approx", _APPROX_NUMERIC),
    ("exact",  _EXACT_NUMERIC),
    ("str",    _STRING_TYPES),
    ("dt",     _DATETIME_TYPES),
    ("bin",    _BINARY_TYPES),
    ("json",   _JSON_TYPE),
)


def get_base_type(dtype_string: str) -> str:
    """
    Extract the base SQL type keyword from a full type definition string.

    Examples::

        get_base_type("VARCHAR(255) NOT NULL")  →  "varchar"
        get_base_type("INT UNSIGNED")           →  "int"
        get_base_type("")                       →  ""
    """
    if not dtype_string:
        return ""
    return dtype_string.split("(")[0].split()[0].lower()


def _category(base_type: str) -> str:
    for cat, types in _CAT_MAP:
        if base_type in types:
            return cat
    return "other"


def classify_conversion(old_type: str, new_type: str) -> ConversionSafety:
    """
    Classify the safety of converting *old_type* data into *new_type*.

    Args:
        old_type: Existing column type (whole definition or base keyword).
        new_type: Target column type (whole definition or base keyword).

    Returns:
        :class:`ConversionSafety` enum value.

    Examples::

        classify_conversion("INT", "BIGINT")          → SAFE
        classify_conversion("FLOAT", "INT")           → LOSSY
        classify_conversion("TEXT", "INT")            → UNSAFE
        classify_conversion("VARCHAR(255)", "TEXT")   → SAFE
    """
    old_base = get_base_type(old_type)
    new_base = get_base_type(new_type)

    if old_base == new_base:
        return ConversionSafety.SAFE

    old_cat = _category(old_base)
    new_cat = _category(new_base)

    # --- Anything → String ---
    if new_cat == "str":
        return ConversionSafety.LOSSY if old_cat == "bin" else ConversionSafety.SAFE

    # --- Numeric → Numeric ---
    if old_cat in ("int", "approx", "exact") and new_cat in ("int", "approx", "exact"):
        if new_cat == "int":
            return ConversionSafety.LOSSY if old_cat in ("approx", "exact") else ConversionSafety.SAFE
        if new_cat == "approx":
            return ConversionSafety.LOSSY  # Precision can be lost
        if new_cat == "exact":
            return ConversionSafety.LOSSY if old_cat == "approx" else ConversionSafety.SAFE

    # --- DateTime → DateTime ---
    if old_cat == "dt" and new_cat == "dt":
        if old_base == "date" and new_base in ("datetime", "timestamp"):
            return ConversionSafety.SAFE
        return ConversionSafety.LOSSY if old_base != new_base else ConversionSafety.SAFE

    # --- Binary → Binary ---
    if old_cat == "bin" and new_cat == "bin":
        return ConversionSafety.SAFE

    # --- String → Binary ---
    if old_cat == "str" and new_cat == "bin":
        return ConversionSafety.LOSSY

    # --- * → JSON ---
    if new_cat == "json":
        return ConversionSafety.SAFE

    # --- JSON → String ---
    if old_cat == "json" and new_cat == "str":
        return ConversionSafety.SAFE

    return ConversionSafety.UNSAFE


def get_cast_expression(source_col_expr: str, target_type: str) -> str:
    """
    Wrap *source_col_expr* in a MySQL ``CAST()`` appropriate for *target_type*.

    Args:
        source_col_expr: A SQL expression referencing the source column,
                         e.g. ``"`col_name`"`` or ``"`t1`.`col`"``.
        target_type:     Full target column definition or just the base type.

    Returns:
        The wrapped ``CAST(… AS …)`` expression, or the original expression
        unchanged if no cast is needed.
    """
    cast_type = _mysql_cast_type(target_type)
    return f"CAST({source_col_expr} AS {cast_type})"


def _mysql_cast_type(type_definition: str) -> str:
    """
    Derive the MySQL ``CAST(… AS <type>)`` type string from a column definition.

    MySQL's CAST only supports a subset of types as cast targets.
    """
    upper = type_definition.upper()
    parts = upper.split("(")[0].split()
    base = parts[0]
    is_unsigned = "UNSIGNED" in parts or "UNSIGNED" in upper

    cast_map: dict[str, str] = {
        "DATE": "DATE",
        "DATETIME": "DATETIME",
        "TIMESTAMP": "DATETIME",
        "TIME": "TIME",
        "YEAR": "SIGNED",
        "JSON": "JSON",
        "FLOAT": "DOUBLE",
        "DOUBLE": "DOUBLE",
        "REAL": "DOUBLE",
    }
    if base in cast_map:
        return cast_map[base]

    int_types = {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"}
    if base in int_types:
        return "UNSIGNED" if is_unsigned else "SIGNED"

    decimal_types = {"DECIMAL", "NUMERIC", "FIXED"}
    if base in decimal_types:
        match = re.search(r"\((\d+)(?:,(\d+))?\)", type_definition)
        precision = match.group(1) if match else "65"
        scale = match.group(2) if match and match.group(2) else "30"
        return f"DECIMAL({precision},{scale})"

    string_types = {
        "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
        "ENUM", "SET",
    }
    if base in string_types:
        return "CHAR CHARACTER SET utf8mb4"

    binary_types = {"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BIT"}
    if base in binary_types:
        return "BINARY"

    return "CHAR CHARACTER SET utf8mb4"


def needs_cast(old_type: str, new_type: str) -> bool:
    """Return True if the conversion requires an explicit MySQL CAST."""
    return get_base_type(old_type) != get_base_type(new_type)
