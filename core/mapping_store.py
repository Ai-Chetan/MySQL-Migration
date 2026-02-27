"""
core/mapping_store.py
---------------------
In-memory mapping state with load/save to JSON and auto-mapping logic.

Design Decision:
    Keeping mapping I/O in a dedicated class separates it from the UI and
    allows the mapping state to be unit-tested without touching the file
    system (mock the persistence methods or use a temp file).
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from core.database import DatabaseManager
from core.schema_parser import ParsedSchema
from logger import get_logger
from models.mapping import (
    AnyMapping,
    SingleMapping,
    SplitMapping,
    MergeMapping,
    MappingType,
    load_mappings_from_file,
    save_mappings_to_file,
)

log = get_logger(__name__)


class MappingStore:
    """
    Thread-unsafe but serialisation-safe mapping registry.

    Attributes:
        _data: Dict of {key: AnyMapping}.  Key is always the source table
               name (for single/split) or the merge key string.
    """

    def __init__(self, file_path: Path | str) -> None:
        self._path = Path(file_path)
        self._data: dict[str, AnyMapping] = {}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self) -> None:
        """
        Load mappings from the JSON file.

        Does not raise on missing file (returns empty store).
        Raises ValueError on corrupt JSON.
        """
        try:
            self._data = load_mappings_from_file(self._path)
            log.info("Loaded %d mapping(s) from '%s'.", len(self._data), self._path)
        except ValueError as exc:
            log.error("Failed to load mappings: %s", exc)
            self._data = {}

    def save(self) -> None:
        """
        Persist the current mapping state to the JSON file.

        Uses an atomic write-then-rename pattern to prevent corruption.
        """
        try:
            save_mappings_to_file(self._path, self._data)
            log.debug("Saved %d mapping(s) to '%s'.", len(self._data), self._path)
        except OSError as exc:
            log.error("Failed to save mappings to '%s': %s", self._path, exc)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get(self, key: str) -> AnyMapping | None:
        return self._data.get(key)

    def all(self) -> dict[str, AnyMapping]:
        return dict(self._data)

    def all_single(self) -> dict[str, SingleMapping]:
        return {k: v for k, v in self._data.items() if isinstance(v, SingleMapping)}

    def all_splits(self) -> dict[str, SplitMapping]:
        return {k: v for k, v in self._data.items() if isinstance(v, SplitMapping)}

    def all_merges(self) -> dict[str, MergeMapping]:
        return {k: v for k, v in self._data.items() if isinstance(v, MergeMapping)}

    def tables_in_merges(self) -> set[str]:
        """Return all source table names that participate in a merge."""
        result: set[str] = set()
        for m in self._data.values():
            if isinstance(m, MergeMapping):
                result.update(m.source_tables)
        return result

    def all_mapped_targets(self, exclude_key: str | None = None) -> set[str]:
        """Return all target schema names currently used across all mappings."""
        targets: set[str] = set()
        for key, m in self._data.items():
            if key == exclude_key:
                continue
            if isinstance(m, SingleMapping):
                targets.add(m.target_schema_name)
            elif isinstance(m, SplitMapping):
                targets.update(m.target_schema_names)
            elif isinstance(m, MergeMapping):
                targets.add(m.target_schema_name)
        return targets

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def set_single(self, source_table: str, target_schema: str) -> None:
        existing = self._data.get(source_table)
        col_maps = {}
        if isinstance(existing, SingleMapping) and existing.target_schema_name == target_schema:
            col_maps = existing.column_mappings
        self._data[source_table] = SingleMapping(
            source_table=source_table,
            target_schema_name=target_schema,
            column_mappings=col_maps,
        )
        log.debug("Set single mapping: %s → %s", source_table, target_schema)
        self.save()

    def remove(self, key: str) -> bool:
        """Remove a mapping by key. Returns True if a mapping was removed."""
        if key in self._data:
            del self._data[key]
            log.debug("Removed mapping for key '%s'.", key)
            self.save()
            return True
        return False

    def set_mapping(self, key: str, mapping: AnyMapping) -> None:
        self._data[key] = mapping
        log.debug("Updated mapping for key '%s'.", key)
        self.save()

    def set_column_mapping(
        self,
        source_table: str,
        old_col: str,
        new_col: str,
        split_target: str | None = None,
    ) -> None:
        """
        Add or update a column mapping within an existing table mapping.

        Args:
            source_table:  Source DB table name (mapping key).
            old_col:       Column name in the source table.
            new_col:       Column name in the target schema.
            split_target:  For split mappings, the name of the schema target
                           to update. Required if the mapping is a split.
        """
        m = self._data.get(source_table)
        if isinstance(m, SingleMapping):
            m.column_mappings[old_col] = new_col
        elif isinstance(m, SplitMapping):
            if split_target is None:
                raise ValueError("split_target required for SplitMapping column mapping.")
            for t in m.targets:
                if t.schema_name == split_target:
                    t.column_mappings[old_col] = new_col
                    break
        else:
            raise ValueError(f"No single/split mapping found for '{source_table}'.")
        self.save()

    def remove_column_mapping(
        self,
        source_table: str,
        new_col: str,
        split_target: str | None = None,
    ) -> None:
        """Remove an explicit column mapping by target column name."""
        m = self._data.get(source_table)
        if isinstance(m, SingleMapping):
            m.column_mappings = {
                k: v for k, v in m.column_mappings.items() if v != new_col
            }
        elif isinstance(m, SplitMapping):
            for t in m.targets:
                if t.schema_name == split_target:
                    t.column_mappings = {
                        k: v for k, v in t.column_mappings.items() if v != new_col
                    }
        self.save()

    # ------------------------------------------------------------------
    # Auto-mapping
    # ------------------------------------------------------------------

    def auto_map(self, db: DatabaseManager, schema: ParsedSchema) -> int:
        """
        Create single mappings for DB tables whose name matches exactly a
        table name in the schema file.  Also auto-map columns with identical
        names (no explicit column map needed, but creates the mapping entry).

        Existing mappings are not overwritten.

        Args:
            db:     Connected DatabaseManager to list tables from.
            schema: Parsed schema dict.

        Returns:
            Number of new mappings created.
        """
        tables_db = set(db.list_tables())
        tables_schema = set(schema.keys())
        new_count = 0

        for table in sorted(tables_db & tables_schema):
            if table in self._data:
                continue  # Do not override existing mapping
            if table.endswith("_new"):
                continue
            if table in self.tables_in_merges():
                continue

            # Auto-map: same name → single mapping with no column overrides
            self._data[table] = SingleMapping(
                source_table=table,
                target_schema_name=table,
                column_mappings={},
            )
            new_count += 1
            log.debug("Auto-mapped table '%s'.", table)

        if new_count:
            self.save()
            log.info("Auto-mapped %d table(s).", new_count)
        return new_count
