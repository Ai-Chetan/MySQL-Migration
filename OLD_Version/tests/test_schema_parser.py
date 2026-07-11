"""
tests/test_schema_parser.py
----------------------------
Unit tests for core/schema_parser.py.
Run with: python -m pytest tests/
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from core.schema_parser import (
    ColumnDefinition,
    generate_create_table_sql,
    parse_schema_file,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_schema(tmp_path: Path, content: str, filename: str = "schema.txt") -> Path:
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# parse_schema_file
# ---------------------------------------------------------------------------

class TestParseSchemaFile:
    def test_single_table(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, """\
            Table: users
              id   INT AUTO_INCREMENT PRIMARY KEY
              name VARCHAR(100) NOT NULL
        """)
        result = parse_schema_file(f)
        assert "users" in result
        assert "id" in result["users"]
        assert "name" in result["users"]

    def test_multiple_tables(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, """\
            Table: users
              id INT PRIMARY KEY

            Table: orders
              id        INT PRIMARY KEY
              user_id   INT NOT NULL
        """)
        result = parse_schema_file(f)
        assert set(result.keys()) == {"users", "orders"}

    def test_comments_ignored(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, """\
            # This is a comment
            Table: products
              id INT PRIMARY KEY
              -- inline comment
              name VARCHAR(50)
        """)
        result = parse_schema_file(f)
        assert "products" in result
        cols = result["products"]
        assert "id" in cols
        assert "name" in cols
        # comment lines must NOT appear as columns
        assert "#" not in " ".join(cols.keys())
        assert "--" not in " ".join(cols.keys())

    def test_empty_file(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, "")
        result = parse_schema_file(f)
        assert result == {}

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            parse_schema_file(tmp_path / "nonexistent.txt")

    def test_column_type_preserved(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, """\
            Table: t
              price DECIMAL(10,2) NOT NULL DEFAULT 0.00
        """)
        result = parse_schema_file(f)
        assert "DECIMAL" in result["t"]["price"].upper()

    def test_case_insensitive_table_marker(self, tmp_path: Path) -> None:
        f = _write_schema(tmp_path, """\
            TABLE: Things
              x INT
        """)
        result = parse_schema_file(f)
        assert "Things" in result


# ---------------------------------------------------------------------------
# generate_create_table_sql
# ---------------------------------------------------------------------------

class TestGenerateCreateTableSql:
    def _schema(self) -> dict[str, str]:
        return {
            "id": "INT AUTO_INCREMENT PRIMARY KEY",
            "name": "VARCHAR(100) NOT NULL",
            "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        }

    def test_contains_create(self) -> None:
        sql = generate_create_table_sql("users_new", self._schema())
        assert "CREATE TABLE" in sql.upper()
        assert "users_new" in sql

    def test_all_columns_present(self) -> None:
        schema = self._schema()
        sql = generate_create_table_sql("test_tbl", schema)
        for col in schema:
            assert col in sql

    def test_if_not_exists(self) -> None:
        sql = generate_create_table_sql("tbl", self._schema(), if_not_exists=True)
        assert "IF NOT EXISTS" in sql.upper()

    def test_no_extra_primary_key(self) -> None:
        """If a column already defines PRIMARY KEY inline, no extra constraint row."""
        schema = {"id": "INT PRIMARY KEY", "val": "VARCHAR(20)"}
        sql = generate_create_table_sql("tbl", schema)
        # Should not try to add PRIMARY KEY(...) as a separate constraint
        # when it is already inline (implementation-specific; verify no duplicate)
        assert sql.upper().count("PRIMARY KEY") == 1
