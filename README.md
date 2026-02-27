# MySQL Migration Tool

A production-ready desktop application for migrating MySQL database schemas with full column-level control, type-safety analysis, and data export.

---

## Features

- **Modular architecture** — strict layered packages (`config`, `logger`, `models`, `core`, `ui`).
- **Three mapping modes** — Single (1→1), Split (1→many), Merge (many→1 via JOIN).
- **Column mapping** — explicit source → target column name overrides per table.
- **Type-safety analysis** — classifies every column pair as SAFE / LOSSY / UNSAFE before any write.
- **Batch migration** — configurable batch size (default 5 000 rows) with progress reporting.
- **Atomic file writes** — mapping JSON is never half-written.
- **Retry on connect** — exponential back-off up to `max_retries` attempts.
- **Structured logging** — Python `logging` module; optional file output.
- **Script generation** — produce standalone Python scripts for complex manual transforms.
- **Data viewer** — browse table contents and download as CSV or JSON.

---

## Requirements

- Python 3.10 or newer
- MySQL Server 5.7+ or MySQL 8.x

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## Quick Start

```bash
# Optional: copy environment template and fill in credentials
cp .env.example .env

python main.py
```

1. **Login** — Enter your MySQL host, port, user, and password.
2. **Load DB & Schema** — Select a database from the dropdown and a schema `.txt` file, then click *Load*.
3. **Map tables** — Use the right-panel buttons to create Single, Split, or Merge mappings.
4. **Tick all checklist items** and click *CREATE Table(s) & Copy Data*.

---

## Schema File Format

```
Table: users
  id         INT AUTO_INCREMENT PRIMARY KEY
  first_name VARCHAR(100) NOT NULL
  last_name  VARCHAR(100)
  email      VARCHAR(200) UNIQUE

# Blank lines and comment lines (# or --) are ignored.

Table: orders
  id         INT AUTO_INCREMENT PRIMARY KEY
  user_id    INT NOT NULL
  total      DECIMAL(10,2)
```

---

## Environment Variables

All variables are optional; they override the built-in defaults.

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | MySQL server hostname. |
| `DB_PORT` | `3306` | MySQL server port. |
| `DB_USER` | *(empty)* | MySQL user (can be entered at login). |
| `DB_PASSWORD` | *(empty)* | MySQL password (can be entered at login). |
| `DB_CHARSET` | `utf8mb4` | Connection charset. |
| `MIGRATION_BATCH_SIZE` | `5000` | Rows per INSERT batch. |
| `MAPPING_FILE` | `table_mappings.json` | Path to the mapping persistence file. |
| `SCRIPTS_DIR` | `.` | Directory for generated migration scripts. |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `LOG_FILE` | *(empty)* | If set, log output is also written to this file path. |

---

## Project Structure

```
.
├── main.py                  # Entry point
├── config.py                # Immutable app configuration (dataclasses)
├── logger.py                # Logging setup
├── requirements.txt
├── .env.example
│
├── models/
│   ├── __init__.py
│   └── mapping.py           # SingleMapping, SplitMapping, MergeMapping dataclasses
│
├── core/
│   ├── __init__.py
│   ├── database.py          # DatabaseManager — connection, retry, transactions
│   ├── schema_parser.py     # Parse .txt schema files → ParsedSchema dict
│   ├── type_converter.py    # MySQL type safety classification
│   ├── mapping_store.py     # In-memory mapping registry + JSON persistence
│   ├── migrator.py          # MigrationEngine — analyse, migrate, progress callbacks
│   └── script_generator.py  # Generate standalone Python migration scripts
│
├── ui/
│   ├── __init__.py
│   ├── app.py               # AppController — top-level state and orchestration
│   ├── utils.py             # Shared UI helpers (ToolTip, ProgressDialog, etc.)
│   ├── login_window.py      # Login dialog
│   ├── main_window.py       # Main application window
│   └── dialogs/
│       ├── __init__.py
│       ├── map_table.py     # Single mapping dialog
│       ├── map_split.py     # Split mapping dialog
│       ├── map_merge.py     # Merge mapping dialog
│       ├── map_columns.py   # Column mapping dialog
│       ├── view_data.py     # Data viewer + CSV/JSON export
│       └── help_dialog.py   # Help window
│
└── tests/
    ├── __init__.py
    ├── test_schema_parser.py
    ├── test_type_converter.py
    ├── test_mapping_store.py
    └── test_migrator.py
```

---

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

---

## Mapping Persistence

Mappings are saved to `table_mappings.json` (or the path set by `MAPPING_FILE`). The file is written atomically — a crash during save will never corrupt existing data.

---

## Manual Script Generation

Select any source table in the main window and click **Generate Manual Script…**. A standalone Python file is created in `SCRIPTS_DIR`; open it and fill in the `# TODO:` sections in `transform_row()`, then run it directly:

```bash
python migration_<table>.py
```

---

## Production Notes

- Always **back up your database** before running any migration.
- Tables created by this tool are named `<target>_new` until you rename them.
- LOSSY column conversions (e.g. BIGINT → INT) require explicit confirmation.
- UNSAFE conversions (e.g. TEXT → INT) are blocked unless you add an explicit column mapping with a valid CAST.
