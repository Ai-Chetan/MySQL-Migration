"""
ui/app.py
---------
Application controller — the single object that holds all shared state
and mediates between the UI windows and the core business logic.

Design Decisions:
    * All mutable application state lives here (db, store, schema, etc.).
    * Windows are created and destroyed through the controller, ensuring
      the lifecycle order is always: Login → Main.
    * The controller listens for events from child windows and delegates
      to the appropriate core module. This avoids circular imports between
      window modules.
    * No Tkinter imports at module level; they are deferred to the methods
      that need them so the core layer remains importable in non-GUI contexts
      (e.g., unit tests).
"""
from __future__ import annotations

import tkinter as tk

from config import CONFIG
from core.database import DatabaseManager, DatabaseError
from core.mapping_store import MappingStore
from core.schema_parser import parse_schema_file, ParsedSchema
from core.migrator import MigrationEngine, MigrationError, MigrationResult
from core.script_generator import generate_script
from logger import get_logger
from models.mapping import (
    AnyMapping,
    SingleMapping,
    SplitMapping,
    MergeMapping,
)

log = get_logger(__name__)


class AppController:
    """
    Central coordinator for the MySQL Migration Tool.

    Lifecycle::

        app = AppController()
        app.run()          # blocks until the user closes all windows

    Internal state attributes (set after successful login):
        db           Connected DatabaseManager.
        store        MappingStore (loaded from file).
        schema       ParsedSchema dict (loaded from schema file).
        schema_path  Path to the currently loaded schema file.
    """

    def __init__(self) -> None:
        self.db: DatabaseManager | None = None
        self.store: MappingStore = MappingStore(CONFIG.migration.mapping_file)
        self.schema: ParsedSchema = {}
        self.schema_path: str = ""
        self._root: tk.Tk | None = None
        self._main_win = None  # MainWindow instance (set after login)

    # ------------------------------------------------------------------
    # Application entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the application (shows login window, then main window)."""
        from ui.login_window import LoginWindow
        login = LoginWindow(on_connect=self._on_login_success)
        login.run()

    # ------------------------------------------------------------------
    # Callbacks invoked by child windows
    # ------------------------------------------------------------------

    def _on_login_success(self, db: DatabaseManager) -> None:
        """Called by LoginWindow when the user successfully connects."""
        self.db = db
        self.store.load()
        from ui.main_window import MainWindow
        self._main_win = MainWindow(controller=self)
        self._main_win.run()

    def on_db_schema_loaded(
        self, database_name: str, schema_path: str
    ) -> tuple[bool, str]:
        """
        Load a database + schema file combination.

        Returns:
            (success, error_message) pair.
        """
        assert self.db is not None
        try:
            self.db.select_database(database_name)
        except DatabaseError as exc:
            return False, str(exc)

        schema = parse_schema_file(schema_path)
        if not schema:
            return False, (
                f"Schema file '{schema_path}' was loaded but contained no table definitions."
            )

        self.schema = schema
        self.schema_path = schema_path
        self.store.load()  # Reload mappings on fresh DB/schema load
        self.store.auto_map(self.db, self.schema)
        log.info(
            "Loaded DB='%s', schema='%s' (%d tables).",
            database_name, schema_path, len(schema),
        )
        return True, ""

    def migrate_mapping(
        self,
        mapping_key: str,
        confirm_lossy: bool,
        progress_cb=None,
    ) -> list[MigrationResult]:
        """
        Execute a migration for the given mapping key.

        Args:
            mapping_key:   Key in the MappingStore (source table or merge key).
            confirm_lossy: If True, proceed despite lossy type conversions.
            progress_cb:   Optional ``(msg, current, total)`` progress callback.

        Returns:
            List of MigrationResult (one per target table created).

        Raises:
            MigrationError: On pre-flight or execution failures.
        """
        assert self.db is not None

        engine = MigrationEngine(
            db=self.db,
            schema=self.schema,
            mappings=self.store.all(),
            progress_cb=progress_cb,
        )

        mapping = self.store.get(mapping_key)
        results: list[MigrationResult] = []

        if isinstance(mapping, SingleMapping):
            results.append(engine.migrate_single(mapping, confirm_lossy=confirm_lossy))
        elif isinstance(mapping, SplitMapping):
            results = engine.migrate_split(mapping, confirm_lossy=confirm_lossy)
        elif isinstance(mapping, MergeMapping):
            results.append(engine.migrate_merge(mapping, confirm_lossy=confirm_lossy))
        else:
            # Unmapped table whose name matches schema — attempt direct migration
            if mapping_key in self.schema:
                temp = SingleMapping(
                    source_table=mapping_key,
                    target_schema_name=mapping_key,
                )
                results.append(engine.migrate_single(temp, confirm_lossy=confirm_lossy))
            else:
                raise MigrationError(
                    f"No mapping found for '{mapping_key}' and name not in schema."
                )

        return results

    def analyse_mapping(self, mapping_key: str):
        """
        Run pre-flight analysis for a mapping (no DB changes).

        Returns a list of MigrationPlan objects (one per target table).
        """
        assert self.db is not None
        engine = MigrationEngine(
            db=self.db,
            schema=self.schema,
            mappings=self.store.all(),
        )
        mapping = self.store.get(mapping_key)
        if isinstance(mapping, SingleMapping):
            return [engine.analyse_single(mapping)]
        if isinstance(mapping, SplitMapping):
            plans = []
            for target in mapping.targets:
                try:
                    plans.append(engine.analyse_single(mapping, target_schema_name=target.schema_name))
                except MigrationError:
                    pass
            return plans
        return []

    def generate_manual_script(self, source_table: str) -> str:
        """
        Generate a migration script template for *source_table*.

        Returns the path to the generated file as a string.
        Raises MigrationError on failure.
        """
        assert self.db is not None
        mapping = self.store.get(source_table)
        if isinstance(mapping, MergeMapping):
            raise MigrationError("Manual scripts are not supported for merge mappings.")

        if isinstance(mapping, SingleMapping):
            target = mapping.target_schema_name
        elif isinstance(mapping, SplitMapping):
            if not mapping.targets:
                raise MigrationError("Split mapping has no targets defined.")
            target = mapping.targets[0].schema_name  # Use first target
        elif source_table in self.schema:
            target = source_table
        else:
            raise MigrationError(
                f"'{source_table}' is not mapped and not found in the schema file."
            )

        if target not in self.schema:
            raise MigrationError(f"Target schema '{target}' not found in schema file.")

        old_schema = self.db.describe_table(source_table)
        new_schema = self.schema[target]

        # Fetch sample rows
        sample_rows: list[tuple] = []
        col_names: list[str] = []
        try:
            self.db.execute(f"SELECT * FROM `{source_table}` LIMIT 2")
            sample_rows = self.db.fetchall()
            col_names = [d[0] for d in self.db.description]
        except Exception:
            pass

        path = generate_script(
            source_table=source_table,
            target_schema_name=target,
            database_name=self.db.current_database or "",
            old_schema=old_schema,
            new_schema=new_schema,
            sample_rows=sample_rows,
            column_names=col_names,
            output_dir=CONFIG.migration.scripts_dir,
        )
        return str(path)

    def cleanup(self) -> None:
        """Close database connection gracefully."""
        if self.db:
            self.db.close()
            self.db = None
            log.info("Application shutdown — database connection closed.")
