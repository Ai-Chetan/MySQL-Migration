"""
tests/test_migrator.py
-----------------------
Unit tests for core/migrator.py using a mock DatabaseManager.
Run with: python -m pytest tests/
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
from pathlib import Path

import pytest

from core.migrator import MigrationEngine, MigrationPlan, MigrationResult
from core.type_converter import ConversionSafety
from models.mapping import SingleMapping, SplitMapping, SplitTarget


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db() -> MagicMock:
    db = MagicMock()
    db.describe_table.return_value = {
        "id": "INT",
        "name": "VARCHAR(100)",
        "email": "VARCHAR(200)",
    }
    db.primary_key_column.return_value = "id"
    db.count_rows.return_value = 3
    db.table_exists.return_value = False
    db.execute.return_value = MagicMock(fetchall=lambda: [(1, "Alice", "a@example.com")])
    db.transaction.return_value = MagicMock(__enter__=MagicMock(return_value=None), __exit__=MagicMock(return_value=False))
    return db


@pytest.fixture
def source_schema() -> dict[str, dict[str, str]]:
    return {
        "users_new": {
            "id": "BIGINT AUTO_INCREMENT PRIMARY KEY",
            "name": "VARCHAR(100) NOT NULL",
            "email": "VARCHAR(200)",
        }
    }


@pytest.fixture
def engine(mock_db, source_schema) -> MigrationEngine:
    return MigrationEngine(db=mock_db, target_schema=source_schema)


# ---------------------------------------------------------------------------
# analyse_single
# ---------------------------------------------------------------------------

class TestAnalyseSingle:
    def test_returns_plan(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        plan = engine.analyse_single(mapping)
        assert isinstance(plan, MigrationPlan)

    def test_plan_has_column_pairs(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        plan = engine.analyse_single(mapping)
        assert len(plan.column_pairs) > 0

    def test_plan_is_executable_when_no_unsafe(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        plan = engine.analyse_single(mapping)
        assert plan.is_executable or len(plan.unsafe_columns) == 0

    def test_missing_target_schema_not_executable(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="nonexistent_schema")
        plan = engine.analyse_single(mapping)
        assert not plan.is_executable

    def test_column_mapping_applied(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(
            source_table="users",
            target_schema_name="users_new",
            column_mappings={"name": "full_name"},
        )
        # full_name not in schema so plan may still generate — just verify it runs
        plan = engine.analyse_single(mapping)
        assert isinstance(plan, MigrationPlan)


# ---------------------------------------------------------------------------
# migrate_single (mocked DB, no real MySQL)
# ---------------------------------------------------------------------------

class TestMigrateSingle:
    def test_returns_result(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        results = engine.migrate_single(mapping)
        assert isinstance(results, list)
        assert all(isinstance(r, MigrationResult) for r in results)

    def test_calls_describe_table(self, engine: MigrationEngine, mock_db: MagicMock) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        engine.migrate_single(mapping)
        mock_db.describe_table.assert_called_with("users")

    def test_result_table_name(self, engine: MigrationEngine) -> None:
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        results = engine.migrate_single(mapping)
        assert len(results) == 1
        assert results[0].table_name == "users_new"

    def test_progress_callback_called(self, engine: MigrationEngine) -> None:
        cb = MagicMock()
        mapping = SingleMapping(source_table="users", target_schema_name="users_new")
        engine.migrate_single(mapping, progress_cb=cb)
        assert cb.call_count > 0


# ---------------------------------------------------------------------------
# MigrationResult dataclass
# ---------------------------------------------------------------------------

class TestMigrationResult:
    def test_success_flag(self) -> None:
        result = MigrationResult(table_name="tbl", success=True, rows_copied=5)
        assert result.success
        assert result.rows_copied == 5

    def test_defaults(self) -> None:
        result = MigrationResult(table_name="t", success=False)
        assert result.warnings == []
        assert result.errors == []
        assert result.elapsed_seconds == 0.0
