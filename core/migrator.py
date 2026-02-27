"""
core/migrator.py
----------------
Migration engine: orchestrates table creation and data copying.

Design Decisions:
    * The engine is a plain class with injected dependencies
      (DatabaseManager, config, schema dict, mappings).  No global state.
    * Progress is reported via a callback (``progress_cb``) so both CLI
      and GUI callers can display updates without coupling this module to Tkinter.
    * Batched LIMIT/OFFSET pattern is used for safety on large tables.
      Cursors are not used for server-side streaming to keep the MySQL
      connection state simple.
    * All DDL/DML is executed inside explicit transaction boundaries.
    * Lossy conversion warnings are surfaced as structured data; the caller
      decides whether to ask the user for confirmation.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from core.database import DatabaseManager, DatabaseError, TableSchema
from core.schema_parser import ParsedSchema, generate_create_table_sql
from core.type_converter import (
    classify_conversion,
    ConversionSafety,
    get_cast_expression,
    get_base_type,
    needs_cast,
)
from config import CONFIG
from logger import get_logger
from models.mapping import (
    AnyMapping,
    SingleMapping,
    SplitMapping,
    MergeMapping,
)

log = get_logger(__name__)

ProgressCallback = Callable[[str, int, int], None]  # message, current, total


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class ColumnPair:
    """Represents one column to be copied from source → target."""
    select_expression: str     # SQL expression for SELECT clause
    target_column: str         # Column name in the new table
    source_type: str
    target_type: str
    safety: ConversionSafety

    @property
    def is_lossy(self) -> bool:
        return self.safety == ConversionSafety.LOSSY

    @property
    def is_unsafe(self) -> bool:
        return self.safety == ConversionSafety.UNSAFE


@dataclass
class MigrationResult:
    """Outcome of a single table migration operation."""
    table_name: str
    success: bool
    rows_copied: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    def __str__(self) -> str:
        status = "OK" if self.success else "FAILED"
        parts = [f"[{status}] {self.table_name}: {self.rows_copied} rows"]
        if self.warnings:
            parts.append(f"  Warnings: {'; '.join(self.warnings)}")
        if self.errors:
            parts.append(f"  Errors: {'; '.join(self.errors)}")
        return "\n".join(parts)


@dataclass
class MigrationPlan:
    """Pre-flight analysis result before any DB changes are made."""
    mapping_key: str
    column_pairs: list[ColumnPair]
    lossy_columns: list[ColumnPair] = field(default_factory=list)
    unsafe_columns: list[ColumnPair] = field(default_factory=list)

    @property
    def is_executable(self) -> bool:
        return len(self.unsafe_columns) == 0


class MigrationError(Exception):
    """Raised when a migration cannot proceed."""


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class MigrationEngine:
    """
    Orchestrates migration of one or more tables.

    Args:
        db:          Connected :class:`DatabaseManager` instance.
        schema:      Parsed schema dict (output of ``parse_schema_file``).
        mappings:    Current mapping state.
        batch_size:  Rows per INSERT…SELECT batch.
        progress_cb: Optional callback ``(message, current, total)`` for
                     progress reporting.

    Example::

        engine = MigrationEngine(db=dm, schema=schema, mappings=mappings)
        result = engine.migrate_single(mapping)
    """

    def __init__(
        self,
        db: DatabaseManager,
        schema: ParsedSchema,
        mappings: dict[str, AnyMapping],
        batch_size: int | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> None:
        self._db = db
        self._schema = schema
        self._mappings = mappings
        self._batch_size = batch_size or CONFIG.migration.batch_size
        self._progress_cb = progress_cb or self._default_progress

    @staticmethod
    def _default_progress(msg: str, current: int, total: int) -> None:
        log.info("%s (%d/%s)", msg, current, total if total else "?")

    def _progress(self, msg: str, current: int = 0, total: int = 0) -> None:
        self._progress_cb(msg, current, total)

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def analyse_single(
        self, mapping: SingleMapping, target_schema_name: str | None = None
    ) -> MigrationPlan:
        """
        Analyse a single-table migration and return a plan (no DB changes).

        Args:
            mapping:            The :class:`SingleMapping` to analyse.
            target_schema_name: Override target name (used internally for
                                split targets); defaults to mapping value.

        Returns:
            :class:`MigrationPlan` with classified column pairs.

        Raises:
            MigrationError: If source or target schema is unavailable.
        """
        target_name = target_schema_name or mapping.target_schema_name
        db_schema = self._db.describe_table(mapping.source_table)
        if not db_schema:
            raise MigrationError(
                f"Cannot read schema for source table '{mapping.source_table}'."
            )
        if target_name not in self._schema:
            raise MigrationError(
                f"Target '{target_name}' not found in schema file."
            )

        col_mappings = mapping.column_mappings if isinstance(mapping, SingleMapping) else {}
        pairs = self._build_column_pairs(
            source_table=mapping.source_table,
            db_schema=db_schema,
            new_schema=self._schema[target_name],
            column_mappings=col_mappings,
        )
        return MigrationPlan(
            mapping_key=mapping.source_table,
            column_pairs=pairs,
            lossy_columns=[p for p in pairs if p.is_lossy],
            unsafe_columns=[p for p in pairs if p.is_unsafe],
        )

    def migrate_single(
        self,
        mapping: SingleMapping,
        confirm_lossy: bool = False,
    ) -> MigrationResult:
        """
        Create the target table and copy data for a single-table mapping.

        Args:
            mapping:       The mapping to execute.
            confirm_lossy: If True, proceed even when lossy conversions are
                           detected (caller has already obtained user consent).

        Returns:
            :class:`MigrationResult`.

        Raises:
            MigrationError: On pre-flight failures.
        """
        plan = self.analyse_single(mapping)
        return self._execute_plan(
            source_table=mapping.source_table,
            target_schema_name=mapping.target_schema_name,
            plan=plan,
            confirm_lossy=confirm_lossy,
        )

    def migrate_split(
        self,
        mapping: SplitMapping,
        confirm_lossy: bool = False,
    ) -> list[MigrationResult]:
        """
        Execute a split mapping — one source table → multiple target tables.

        Returns:
            List of :class:`MigrationResult` (one per target table).
        """
        results: list[MigrationResult] = []
        db_schema = self._db.describe_table(mapping.source_table)
        if not db_schema:
            raise MigrationError(
                f"Cannot read schema for source table '{mapping.source_table}'."
            )

        for target in mapping.targets:
            if not target.schema_name or target.schema_name not in self._schema:
                log.error("Split target '%s' not in schema — skipping.", target.schema_name)
                results.append(
                    MigrationResult(
                        table_name=f"{target.schema_name}_new",
                        success=False,
                        errors=[f"Schema definition missing for '{target.schema_name}'"],
                    )
                )
                continue

            pairs = self._build_column_pairs(
                source_table=mapping.source_table,
                db_schema=db_schema,
                new_schema=self._schema[target.schema_name],
                column_mappings=target.column_mappings,
            )
            plan = MigrationPlan(
                mapping_key=f"{mapping.source_table}->{target.schema_name}",
                column_pairs=pairs,
                lossy_columns=[p for p in pairs if p.is_lossy],
                unsafe_columns=[p for p in pairs if p.is_unsafe],
            )
            result = self._execute_plan(
                source_table=mapping.source_table,
                target_schema_name=target.schema_name,
                plan=plan,
                confirm_lossy=confirm_lossy,
            )
            results.append(result)

        return results

    def migrate_merge(
        self,
        mapping: MergeMapping,
        confirm_lossy: bool = False,
    ) -> MigrationResult:
        """
        Execute a merge mapping — multiple source tables → one target table.

        Returns:
            :class:`MigrationResult`.

        Raises:
            MigrationError: On fatal pre-flight failures.
        """
        target_name = mapping.target_schema_name
        if not target_name or target_name not in self._schema:
            raise MigrationError(
                f"Merge target '{target_name}' not found in schema file."
            )

        new_schema = self._schema[target_name]
        all_source_schemas: dict[str, TableSchema] = {}
        for src in mapping.source_tables:
            schema = self._db.describe_table(src)
            if not schema:
                raise MigrationError(
                    f"Cannot read schema for merge source table '{src}'."
                )
            all_source_schemas[src] = schema

        select_parts: list[str] = []
        insert_cols: list[str] = []
        lossy_warnings: list[str] = []
        fatal: str | None = None

        for new_col, new_def in new_schema.items():
            # Resolve source specifier from column_mappings
            source_spec = next(
                (src for src, tgt in mapping.column_mappings.items() if tgt == new_col),
                None,
            )
            if not source_spec or "." not in source_spec:
                log.warning(
                    "No 'table.column' mapping for target column '%s' in merge '%s'. Skipping.",
                    new_col, target_name,
                )
                continue

            src_table, src_col = source_spec.split(".", 1)
            if src_table not in all_source_schemas or src_col not in all_source_schemas[src_table]:
                fatal = f"Mapped source '{source_spec}' not found in source table schemas."
                break

            old_type = all_source_schemas[src_table][src_col][1]
            new_type = new_def.split()[0]
            safety = classify_conversion(old_type, new_type)

            if safety == ConversionSafety.UNSAFE:
                fatal = (
                    f"Unsafe type conversion: `{src_table}`.`{src_col}` ({old_type})"
                    f" → `{target_name}`.`{new_col}` ({new_type})"
                )
                break

            expr = f"`{src_table}`.`{src_col}`"
            if needs_cast(old_type, new_type):
                expr = get_cast_expression(expr, new_type)
                if safety == ConversionSafety.LOSSY:
                    lossy_warnings.append(f"`{new_col}` ({old_type} → {new_type})")

            select_parts.append(expr)
            insert_cols.append(f"`{new_col}`")

        if fatal:
            raise MigrationError(fatal)

        if not confirm_lossy and lossy_warnings:
            raise MigrationError(
                "Lossy conversions detected (confirm_lossy=False):\n"
                + "\n".join(f"  - {w}" for w in lossy_warnings)
            )

        target_db_name = f"{target_name}_new"
        if self._db.table_exists(target_db_name):
            raise MigrationError(
                f"Target table '{target_db_name}' already exists. Drop it first."
            )

        create_sql = generate_create_table_sql(target_db_name, new_schema)
        from_clause = f"`{mapping.source_tables[0]}`"
        if mapping.join_conditions:
            from_clause += f" {mapping.join_conditions}"

        return self._create_and_copy(
            create_sql=create_sql,
            target_db_name=target_db_name,
            insert_cols=insert_cols,
            select_clause=", ".join(select_parts),
            from_clause=from_clause,
            source_ref=mapping.merge_key,
            warnings=lossy_warnings,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_column_pairs(
        self,
        source_table: str,
        db_schema: TableSchema,
        new_schema: dict[str, str],
        column_mappings: dict[str, str],
    ) -> list[ColumnPair]:
        """
        Build :class:`ColumnPair` objects for all target columns.

        Column resolution order:
        1. Explicit mapping (new → old via reverse lookup).
        2. Same name in both schemas.
        3. Column silently omitted from copy if not in source (new column).
        """
        reverse = {v: k for k, v in column_mappings.items()}
        pairs: list[ColumnPair] = []

        for new_col, new_def in new_schema.items():
            old_col = reverse.get(new_col, new_col)
            if old_col not in db_schema:
                log.debug(
                    "Target column '%s' has no matching source column in '%s'. "
                    "It will be omitted from the SELECT (new column).",
                    new_col, source_table,
                )
                continue

            old_type = db_schema[old_col][1]
            new_type = new_def.split()[0]
            safety = classify_conversion(old_type, new_type)

            expr = f"`{old_col}`"
            if needs_cast(old_type, new_type) and safety != ConversionSafety.UNSAFE:
                expr = get_cast_expression(expr, new_type)

            pairs.append(
                ColumnPair(
                    select_expression=expr,
                    target_column=new_col,
                    source_type=old_type,
                    target_type=new_type,
                    safety=safety,
                )
            )
        return pairs

    def _execute_plan(
        self,
        source_table: str,
        target_schema_name: str,
        plan: MigrationPlan,
        confirm_lossy: bool,
    ) -> MigrationResult:
        """Validate a plan's safety then create → copy."""
        if plan.unsafe_columns:
            details = "; ".join(
                f"`{p.target_column}` ({p.source_type} → {p.target_type})"
                for p in plan.unsafe_columns
            )
            raise MigrationError(f"Unsafe type conversions detected: {details}")

        if not confirm_lossy and plan.lossy_columns:
            details = "; ".join(
                f"`{p.target_column}` ({p.source_type} → {p.target_type})"
                for p in plan.lossy_columns
            )
            raise MigrationError(
                f"Lossy conversions detected (confirm_lossy=False): {details}"
            )

        target_db_name = f"{target_schema_name}_new"
        if self._db.table_exists(target_db_name):
            raise MigrationError(
                f"Target table '{target_db_name}' already exists. Drop it first."
            )

        create_sql = generate_create_table_sql(
            target_db_name, self._schema[target_schema_name]
        )
        insert_cols = [f"`{p.target_column}`" for p in plan.column_pairs]
        select_clause = ", ".join(p.select_expression for p in plan.column_pairs)
        from_clause = f"`{source_table}`"
        lossy_warnings = [
            f"`{p.target_column}` ({p.source_type} → {p.target_type})"
            for p in plan.lossy_columns
        ]

        return self._create_and_copy(
            create_sql=create_sql,
            target_db_name=target_db_name,
            insert_cols=insert_cols,
            select_clause=select_clause,
            from_clause=from_clause,
            source_ref=source_table,
            warnings=lossy_warnings,
        )

    def _create_and_copy(
        self,
        create_sql: str,
        target_db_name: str,
        insert_cols: list[str],
        select_clause: str,
        from_clause: str,
        source_ref: str,
        warnings: list[str],
    ) -> MigrationResult:
        """Execute CREATE TABLE then copy data in batches."""
        start = time.monotonic()
        result = MigrationResult(
            table_name=target_db_name,
            success=False,
            warnings=warnings,
        )

        # --- CREATE TABLE ---
        try:
            log.info("Creating table '%s'...", target_db_name)
            self._db.execute(create_sql)
            self._db.commit()
            log.info("Table '%s' created successfully.", target_db_name)
        except DatabaseError as exc:
            result.errors.append(f"CREATE TABLE failed: {exc}")
            log.error("Failed to create '%s': %s\nSQL:\n%s", target_db_name, exc, create_sql)
            return result

        # --- COPY DATA ---
        rows_copied = self._copy_batched(
            source_ref=source_ref,
            target_db_name=target_db_name,
            insert_cols=insert_cols,
            select_clause=select_clause,
            from_clause=from_clause,
            result=result,
        )

        result.rows_copied = rows_copied
        result.elapsed_seconds = time.monotonic() - start
        result.success = len(result.errors) == 0
        log.info(
            "Migration of '%s' finished: %d rows, %.2fs",
            target_db_name, rows_copied, result.elapsed_seconds,
        )
        return result

    def _copy_batched(
        self,
        source_ref: str,
        target_db_name: str,
        insert_cols: list[str],
        select_clause: str,
        from_clause: str,
        result: MigrationResult,
    ) -> int:
        """
        Perform batched INSERT … SELECT with LIMIT/OFFSET and return total rows copied.
        """
        if not insert_cols or not select_clause or not from_clause:
            log.warning("Empty copy parameters for '%s'. Skipping data copy.", target_db_name)
            return 0

        # Estimate total rows (best-effort; may not be exact for JOINs)
        primary_source = source_ref if not source_ref.startswith("MERGE:") else None
        if primary_source:
            total_rows = self._db.count_rows(primary_source)
        else:
            total_rows = 0

        self._progress(f"Copying data → {target_db_name}", 0, total_rows)

        # Try to find an ORDER BY column for deterministic paging
        order_col = self._determine_order_column(source_ref, from_clause)

        insert_cols_str = ", ".join(insert_cols)
        offset = 0
        rows_copied = 0
        batch_num = 0

        while True:
            if not self._db.is_connected:
                result.errors.append("Connection lost during data copy.")
                break

            limited = f"{from_clause} ORDER BY {order_col} LIMIT {self._batch_size} OFFSET {offset}"
            query = (
                f"INSERT INTO `{target_db_name}` ({insert_cols_str}) "
                f"SELECT {select_clause} FROM {limited};"
            )

            try:
                self._db.execute(query)
                batch_count = self._db.rowcount
                self._db.commit()

                # Surface any MySQL warnings
                for w in self._db.warnings():
                    result.warnings.append(f"[Batch {batch_num}] {w[2]}")

                rows_copied += batch_count
                batch_num += 1
                self._progress(
                    f"Copying → {target_db_name}: {rows_copied} rows",
                    rows_copied,
                    total_rows,
                )
                log.debug(
                    "Batch %d done: %d rows (offset %d).",
                    batch_num, batch_count, offset,
                )

                if batch_count < self._batch_size:
                    break  # Last batch
                offset += self._batch_size

            except DatabaseError as exc:
                self._db.rollback()
                error_msg = (
                    f"Batch copy failed at offset {offset} for '{target_db_name}': {exc}"
                )
                result.errors.append(error_msg)
                log.error(error_msg)
                log.debug("Failed query:\n%s", query)
                break

        return rows_copied

    def _determine_order_column(self, source_ref: str, from_clause: str) -> str:
        """
        Best-effort ORDER BY expression for deterministic LIMIT/OFFSET.

        Prefers the primary key of the primary source table; falls back to 1.
        """
        if source_ref.startswith("MERGE:"):
            # Try to extract the first table name from the FROM clause
            match = __import__("re").match(r"`(\w+)`", from_clause)
            if match:
                pk = self._db.primary_key_column(match.group(1))
                if pk:
                    return f"`{match.group(1)}`.`{pk}`"
            return "1"

        pk = self._db.primary_key_column(source_ref)
        return f"`{source_ref}`.`{pk}`" if pk else "1"
