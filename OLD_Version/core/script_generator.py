"""
core/script_generator.py
------------------------
Generates runnable Python migration script templates for complex cases.

Design Decisions:
    * Script output is a self-contained, well-commented Python file that
      runs standalone (``python <script>.py``) without requiring this tool.
    * Placeholders are clearly marked with ``# TODO:`` comments at each
      decision point.
    * The generated script includes retry/rollback logic so users have a
      safe starting point regardless of their Python skill level.
"""
from __future__ import annotations

import datetime
import textwrap
from pathlib import Path
from typing import Any

from core.database import TableSchema
from core.schema_parser import ParsedSchema, generate_create_table_sql
from logger import get_logger
from models.mapping import SingleMapping, SplitMapping

log = get_logger(__name__)

_SCRIPT_TEMPLATE = """\
# -*- coding: utf-8 -*-
# Manual Migration Script
# Source Table : {source_table}
# Target Table : {target_db_name}
# Schema Name  : {target_schema_name}
# Generated    : {timestamp}
# Tool Version : MySQL Migration Tool v2
#
# ═══════════════════════════════════════════════════════════════════
# IMPORTANT — READ BEFORE RUNNING
# ═══════════════════════════════════════════════════════════════════
# 1. Update DB_CONFIG with your real credentials.
# 2. Search for every "# TODO:" comment and fill in your logic.
# 3. Run a test against a non-production database first.
# 4. ALWAYS backup the database before running against production.
# ═══════════════════════════════════════════════════════════════════

import sys
import datetime
import decimal
import mysql.connector

# ---------------------------------------------------------------------------
# Configuration — update before running
# ---------------------------------------------------------------------------
DB_CONFIG = {{
    "host":     "localhost",
    "port":     3306,
    "user":     "YOUR_USERNAME",     # TODO: replace
    "password": "YOUR_PASSWORD",     # TODO: replace
    "database": "{database}",
    "charset":  "utf8mb4",
    "raise_on_warnings": False,
}}

BATCH_SIZE = 5000   # Rows per INSERT batch
OLD_TABLE  = "{source_table}"
NEW_TABLE  = "{target_db_name}"

# ---------------------------------------------------------------------------
# Old schema (from database at generation time)
# ---------------------------------------------------------------------------
#
{old_schema_comment}
#
# ---------------------------------------------------------------------------
# New schema (from schema file)
# ---------------------------------------------------------------------------
#
{new_schema_comment}
#
# ---------------------------------------------------------------------------
# Sample data (first 2 rows at generation time)
# ---------------------------------------------------------------------------
#
{sample_data_comment}
#

# ---------------------------------------------------------------------------
# Generated CREATE TABLE statement
# ---------------------------------------------------------------------------
CREATE_SQL = \"\"\"
{create_sql}
\"\"\"

# ---------------------------------------------------------------------------
# Migration logic
# ---------------------------------------------------------------------------

def transform_row(row: dict) -> tuple | None:
    \"\"\"
    Transform one source row into a tuple of values for INSERT.

    Args:
        row: Dict {{column_name: value}} for the current source row.

    Returns:
        Tuple of values matching NEW_TABLE column order, or None to skip row.

    TODO: Implement your transformation logic below.
    \"\"\"
    try:
        new_values = (
{insert_value_lines}
        )
        return new_values
    except Exception as exc:
        # Log and skip malformed rows
        print(f"  Skipping row due to transform error: {{exc}}", file=sys.stderr)
        return None


INSERT_SQL = (
    f"INSERT INTO `{{NEW_TABLE}}` ({insert_cols_str}) "
    f"VALUES ({placeholders_str})"
)


def migrate_data() -> None:
    \"\"\"Connect to MySQL, create the target table, and migrate data.\"\"\"
    conn = None
    cursor = None
    rows_processed = rows_inserted = rows_failed = 0

    try:
        print(f"Connecting to {{DB_CONFIG['host']}}…")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        print("Connected.")

        # --- Create target table ---
        print(f"Creating table '{{NEW_TABLE}}'…")
        cursor.execute(CREATE_SQL)
        conn.commit()
        print("Table created.")

        # --- Count source rows for progress tracking ---
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{{OLD_TABLE}}`")
        total = cursor.fetchone()["cnt"]
        print(f"Source rows: {{total}}")

        # --- Fetch and insert in batches ---
        offset = 0
        while True:
            cursor.execute(
                f"SELECT * FROM `{{OLD_TABLE}}` LIMIT {{BATCH_SIZE}} OFFSET {{offset}}"
            )
            batch = cursor.fetchall()
            if not batch:
                break

            batch_inserts = 0
            for row in batch:
                rows_processed += 1
                values = transform_row(row)
                if values is None:
                    rows_failed += 1
                    continue
                try:
                    cursor.execute(INSERT_SQL, values)
                    rows_inserted += 1
                    batch_inserts += 1
                except mysql.connector.Error as exc:
                    rows_failed += 1
                    print(
                        f"  Insert error (row {{rows_processed}}): {{exc}}",
                        file=sys.stderr,
                    )

            conn.commit()
            offset += BATCH_SIZE
            pct = rows_processed / total * 100 if total else 0
            print(f"  Progress: {{rows_processed}}/{{total}} ({{{pct:.1f}}}%)")

        print("\\nFinal commit…")
        conn.commit()

    except mysql.connector.Error as exc:
        print(f"Database error: {{exc}}", file=sys.stderr)
        if conn:
            conn.rollback()
    except Exception as exc:
        print(f"Unexpected error: {{exc}}", file=sys.stderr)
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print(
            f"\\nDone. Processed={{rows_processed}}, "
            f"Inserted={{rows_inserted}}, Failed={{rows_failed}}"
        )


if __name__ == "__main__":
    confirm = input(
        f"Migrate '{{OLD_TABLE}}' → '{{NEW_TABLE}}' in database "
        f"'{{DB_CONFIG['database']}}'.\\n"
        "!!! Ensure the database is backed up. Type 'yes' to proceed: "
    )
    if confirm.strip().lower() == "yes":
        migrate_data()
    else:
        print("Migration aborted.")
"""


def generate_script(
    source_table: str,
    target_schema_name: str,
    database_name: str,
    old_schema: TableSchema,
    new_schema: dict[str, str],
    sample_rows: list[tuple],
    column_names: list[str],
    output_dir: Path | str = ".",
) -> Path:
    """
    Generate a standalone Python migration script template.

    Args:
        source_table:        Name of the existing DB table.
        target_schema_name:  Target table name from the schema file.
        database_name:       Name of the MySQL database.
        old_schema:          DESCRIBE output for the source table.
        new_schema:          {col: definition} from the schema file.
        sample_rows:         Up to 2 sample data rows from the source.
        column_names:        Column names matching *sample_rows* tuples.
        output_dir:          Directory to write the .py file to.

    Returns:
        Path to the generated script file.
    """
    target_db_name = f"{target_schema_name}_new"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_dir = Path(output_dir)

    # --- Build inline comments ---
    old_schema_lines = [
        f"#   {col:<25} {details[1]:<20} NULL={details[2]} Key={details[3]}"
        for col, details in old_schema.items()
    ]
    new_schema_lines = [
        f"#   {col:<25} {defn}"
        for col, defn in new_schema.items()
    ]

    sample_lines: list[str] = []
    for i, row in enumerate(sample_rows[:2], start=1):
        row_dict = dict(zip(column_names, row))
        sample_lines.append(f"#  Row {i}: {row_dict}")

    # --- Create SQL ---
    create_sql = generate_create_table_sql(target_db_name, new_schema)

    # --- INSERT template ---
    new_cols = list(new_schema.keys())
    insert_cols_str = ", ".join(f"`{c}`" for c in new_cols)
    placeholders_str = ", ".join(["%s"] * len(new_cols))

    # Best-effort value lines: use same-named column from source if available
    insert_lines: list[str] = []
    for col in new_cols:
        if col in old_schema:
            insert_lines.append(
                f"            row.get({col!r}),  # {old_schema[col][1]} → {new_schema[col].split()[0]}"
            )
        else:
            insert_lines.append(
                f"            None,  # TODO: provide value for new column '{col}'"
            )
    insert_value_lines = "\n".join(insert_lines)

    script = _SCRIPT_TEMPLATE.format(
        source_table=source_table,
        target_db_name=target_db_name,
        target_schema_name=target_schema_name,
        database=database_name,
        timestamp=timestamp,
        old_schema_comment="\n".join(old_schema_lines) or "#  (unavailable)",
        new_schema_comment="\n".join(new_schema_lines) or "#  (unavailable)",
        sample_data_comment="\n".join(sample_lines) or "#  (no rows found)",
        create_sql=textwrap.indent(create_sql, ""),
        insert_cols_str=insert_cols_str,
        placeholders_str=placeholders_str,
        insert_value_lines=insert_value_lines,
    )

    filename = f"manual_migration_{source_table}_to_{target_schema_name}.py"
    out_path = out_dir / filename

    out_path.write_text(script, encoding="utf-8")
    log.info("Generated manual migration script: %s", out_path)
    return out_path
