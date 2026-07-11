"""
models/mapping.py
-----------------
Typed data models for table and column mapping configurations.

Design Decision:
    Using ``@dataclass`` and ``Enum`` instead of plain dicts ensures:
    * Type checking / IDE auto-complete throughout the codebase.
    * A single source of truth for valid mapping types.
    * Easy serialisation / deserialisation with explicit to_dict / from_dict
      methods that include backward-compatibility upgrades for old JSON formats.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class MappingType(str, Enum):
    """Discriminator for the three supported migration strategies."""
    SINGLE = "single"
    SPLIT = "split"
    MERGE = "merge"


@dataclass
class SingleMapping:
    """
    Maps one source DB table → one target schema table.

    Attributes:
        source_table:        Name of the existing database table.
        target_schema_name:  Table name as defined in the schema file.
        column_mappings:     Dict mapping old_column_name → new_column_name
                             for columns whose names differ.
    """
    source_table: str
    target_schema_name: str
    column_mappings: dict[str, str] = field(default_factory=dict)
    mapping_type: MappingType = MappingType.SINGLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.mapping_type.value,
            "new_table_name_schema": self.target_schema_name,
            "column_mappings": self.column_mappings,
        }

    @staticmethod
    def from_dict(source_table: str, data: dict[str, Any]) -> "SingleMapping":
        return SingleMapping(
            source_table=source_table,
            target_schema_name=data.get("new_table_name_schema", source_table),
            column_mappings=data.get("column_mappings", {}),
        )


@dataclass
class SplitTarget:
    """One branch of a split mapping (one new target table)."""
    schema_name: str
    column_mappings: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"schema_name": self.schema_name, "column_mappings": self.column_mappings}

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SplitTarget":
        return SplitTarget(
            schema_name=data.get("schema_name", ""),
            column_mappings=data.get("column_mappings", {}),
        )


@dataclass
class SplitMapping:
    """
    Maps one source DB table → multiple target schema tables.

    Attributes:
        source_table:  Name of the existing database table.
        targets:       Ordered list of target table definitions.
    """
    source_table: str
    targets: list[SplitTarget] = field(default_factory=list)
    mapping_type: MappingType = MappingType.SPLIT

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.mapping_type.value,
            "new_tables": [t.to_dict() for t in self.targets],
        }

    @staticmethod
    def from_dict(source_table: str, data: dict[str, Any]) -> "SplitMapping":
        targets = [
            SplitTarget.from_dict(t) for t in data.get("new_tables", [])
        ]
        return SplitMapping(source_table=source_table, targets=targets)

    @property
    def target_schema_names(self) -> list[str]:
        return [t.schema_name for t in self.targets]

    def column_mappings_for(self, schema_name: str) -> dict[str, str]:
        for t in self.targets:
            if t.schema_name == schema_name:
                return t.column_mappings
        return {}


@dataclass
class MergeMapping:
    """
    Maps multiple source DB tables → one target schema table via JOIN.

    Attributes:
        merge_key:           The dict key used in JSON storage (usually
                             ``"merge_<target>_<timestamp>"``).
        source_tables:       Ordered list of source table names. The first
                             table is the primary (FROM) table; subsequent
                             tables are JOINed.
        target_schema_name:  Target table name in the schema file.
        join_conditions:     Raw SQL snippet appended after the FROM clause,
                             e.g. ``"INNER JOIN t2 ON t1.id = t2.t1_id"``.
        column_mappings:     Dict mapping ``"source_table.column"`` →
                             ``"target_column"``.
    """
    merge_key: str
    source_tables: list[str]
    target_schema_name: str
    join_conditions: str = ""
    column_mappings: dict[str, str] = field(default_factory=dict)
    mapping_type: MappingType = MappingType.MERGE

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.mapping_type.value,
            "source_tables": self.source_tables,
            "new_table_name_schema": self.target_schema_name,
            "join_conditions": self.join_conditions,
            "column_mappings": self.column_mappings,
        }

    @staticmethod
    def from_dict(merge_key: str, data: dict[str, Any]) -> "MergeMapping":
        return MergeMapping(
            merge_key=merge_key,
            source_tables=data.get("source_tables", []),
            target_schema_name=data.get("new_table_name_schema", ""),
            join_conditions=data.get("join_conditions", ""),
            column_mappings=data.get("column_mappings", {}),
        )

    @property
    def display_name(self) -> str:
        return f"MERGE: {', '.join(self.source_tables)} -> {self.target_schema_name}"


# Union type alias
AnyMapping = SingleMapping | SplitMapping | MergeMapping


def mapping_from_dict(key: str, data: Any) -> AnyMapping | None:
    """
    Deserialise a single mapping entry from its JSON dict representation.

    Handles backward-compatible upgrade from old format where values were
    plain strings (single mappings before the 'type' field was added).

    Args:
        key:  The JSON key (source table name or merge key).
        data: The JSON value (string or dict).

    Returns:
        A typed mapping object, or None if the format is unrecognised.
    """
    if isinstance(data, str):
        # Legacy: {"old_table": "new_table"}
        return SingleMapping(source_table=key, target_schema_name=data)

    if not isinstance(data, dict):
        return None

    raw_type = data.get("type", "")
    try:
        mtype = MappingType(raw_type)
    except ValueError:
        # Old dict format without 'type' field
        if "new_table_name_schema" in data or "column_mappings" in data:
            return SingleMapping.from_dict(key, data)
        return None

    if mtype == MappingType.SINGLE:
        return SingleMapping.from_dict(key, data)
    if mtype == MappingType.SPLIT:
        return SplitMapping.from_dict(key, data)
    if mtype == MappingType.MERGE:
        return MergeMapping.from_dict(key, data)
    return None


def load_mappings_from_file(path: Path) -> dict[str, AnyMapping]:
    """
    Load and deserialise all mappings from a JSON file.

    Args:
        path: Path to the JSON mapping file.

    Returns:
        A dict of ``{key: mapping_object}``.  Empty dict if file is absent.

    Raises:
        ValueError: If the file contains invalid JSON.
    """
    if not path.exists():
        return {}
    try:
        raw: dict = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in mapping file '{path}': {exc}") from exc

    result: dict[str, AnyMapping] = {}
    for k, v in raw.items():
        m = mapping_from_dict(k, v)
        if m is not None:
            result[k] = m
    return result


def save_mappings_to_file(path: Path, mappings: dict[str, AnyMapping]) -> None:
    """
    Serialise all mappings to JSON and write atomically (write-then-rename).

    Args:
        path:     Destination file path.
        mappings: Current mapping state keyed by source table / merge key.
    """
    raw = {k: m.to_dict() for k, m in mappings.items()}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(raw, indent=4), encoding="utf-8")
    tmp.replace(path)   # Atomic rename avoids partial writes corrupting the file
