"""
core/schema_parser.py
---------------------
Parses plain-text schema definition files into structured Python dicts.

File Format (supported)::

    Table: users
      id          INT AUTO_INCREMENT PRIMARY KEY
      username    VARCHAR(100) NOT NULL
      email       VARCHAR(255) NOT NULL UNIQUE
      created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
      # comments are ignored (lines starting with # or --)

    Table: orders
      order_id  INT AUTO_INCREMENT PRIMARY KEY
      user_id   INT NOT NULL
      total     DECIMAL(10,2)

Design Decisions:
    * The parser is a pure function (no side effects) to simplify testing.
    * Regex is kept minimal; full SQL parsing is out of scope.
    * Duplicate table definitions: last wins (matching original behaviour).
    * Duplicate column names: last wins.
    * Returns plain dicts to keep the interface simple; callers that need
      richer types can wrap the result.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

from logger import get_logger

log = get_logger(__name__)

# {table_name: {col_name: "TYPE CONSTRAINTS …"}}
ParsedSchema = dict[str, dict[str, str]]

_TABLE_RE = re.compile(r"^\s*Table\s*:\s*(\w+)\s*$", re.IGNORECASE)
_COL_RE = re.compile(r"^\s*[`'\"]?([\w_]+)[`'\"]?\s+(.+)")
_COMMENT_RE = re.compile(r"^\s*(#|--)")


class SchemaParseError(Exception):
    """Raised when a schema file cannot be read or is fundamentally invalid."""


class ColumnDefinition(NamedTuple):
    """Structured breakdown of a column definition string."""
    name: str
    raw_definition: str
    base_type: str
    is_nullable: bool
    is_primary_key: bool
    is_unique: bool
    has_auto_increment: bool
    default_value: str | None


def parse_schema_file(file_path: str | Path) -> ParsedSchema:
    """
    Parse a schema definition file and return a nested dict.

    Args:
        file_path: Path to the ``.txt`` / ``.sql`` schema file.

    Returns:
        A dict ``{table_name: {col_name: definition_string}}``.
        Returns an empty dict if the file does not exist.

    Raises:
        SchemaParseError: If the file cannot be read.

    Example::

        schema = parse_schema_file("schema.txt")
        # schema["users"]["email"] == "VARCHAR(255) NOT NULL UNIQUE"
    """
    path = Path(file_path)
    if not path.exists():
        log.warning("Schema file not found: %s", path)
        return {}

    schema: ParsedSchema = {}
    current_table: str | None = None
    errors: list[str] = []

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SchemaParseError(f"Cannot read schema file '{path}': {exc}") from exc

    for line_num, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or _COMMENT_RE.match(stripped):
            continue

        table_match = _TABLE_RE.match(stripped)
        if table_match:
            current_table = table_match.group(1)
            if current_table in schema:
                log.debug(
                    "Duplicate table definition '%s' at line %d — overwriting.",
                    current_table, line_num,
                )
            schema[current_table] = {}
            continue

        if current_table:
            col_match = _COL_RE.match(stripped)
            if col_match:
                col_name = col_match.group(1)
                definition = col_match.group(2).strip()
                schema[current_table][col_name] = definition
            else:
                errors.append(f"Line {line_num}: unrecognised column syntax → {stripped!r}")
        else:
            if stripped:
                log.debug("Line %d is outside any Table block — skipped: %s", line_num, stripped)

    if errors:
        log.warning(
            "Schema parse warnings in '%s':\n  %s",
            path, "\n  ".join(errors)
        )

    log.info(
        "Parsed schema file '%s': %d table(s), %d column(s) total.",
        path.name,
        len(schema),
        sum(len(cols) for cols in schema.values()),
    )
    return schema


def parse_column_definition(col_name: str, definition: str) -> ColumnDefinition:
    """
    Extract structured attributes from a raw column definition string.

    This is a best-effort extraction used for schema comparison; it does
    *not* attempt full SQL parsing.

    Args:
        col_name:   Column name.
        definition: The raw definition string, e.g. ``"VARCHAR(255) NOT NULL"``.

    Returns:
        A :class:`ColumnDefinition` named tuple.
    """
    defn_upper = definition.upper()
    parts = definition.split()
    base_type = parts[0].split("(")[0].lower() if parts else ""

    is_nullable = "NOT NULL" not in defn_upper
    is_pk = "PRIMARY KEY" in defn_upper
    is_unique = "UNIQUE" in defn_upper
    has_auto_increment = "AUTO_INCREMENT" in defn_upper

    default_match = re.search(
        r"DEFAULT\s+((?:'(?:[^']|\\')*'|\"(?:[^\"]|\\\")*\"|[\w.\-]+)|NULL)",
        definition,
        re.IGNORECASE,
    )
    default_value: str | None = None
    if default_match:
        raw = default_match.group(1)
        default_value = None if raw.upper() == "NULL" else raw.strip("'\"")

    return ColumnDefinition(
        name=col_name,
        raw_definition=definition,
        base_type=base_type,
        is_nullable=is_nullable,
        is_primary_key=is_pk,
        is_unique=is_unique,
        has_auto_increment=has_auto_increment,
        default_value=default_value,
    )


def generate_create_table_sql(
    table_name: str,
    column_defs: dict[str, str],
    engine: str = "InnoDB",
    charset: str = "utf8mb4",
    collate: str = "utf8mb4_unicode_ci",
) -> str:
    """
    Generate a ``CREATE TABLE`` statement from a column definition dict.

    Avoids duplicate PRIMARY KEY declarations by detecting inline ``PRIMARY KEY``
    and only adding a separate constraint when none is declared inline.

    Args:
        table_name:   Target table name (unquoted).
        column_defs:  Ordered dict ``{col_name: definition_string}``.
        engine:       MySQL storage engine (default ``InnoDB``).
        charset:      Character set (default ``utf8mb4``).
        collate:      Collation (default ``utf8mb4_unicode_ci``).

    Returns:
        A complete ``CREATE TABLE …`` SQL statement string.
    """
    col_lines: list[str] = []
    constraints: list[str] = []
    inline_pk = False

    for col_name, definition in column_defs.items():
        col_lines.append(f"  `{col_name}` {definition}")
        if "PRIMARY KEY" in definition.upper():
            inline_pk = True

    if not inline_pk:
        # Heuristic: look for a column that has 'PRIMARY KEY' among all defs
        pk_cols = [c for c, d in column_defs.items() if "PRIMARY KEY" in d.upper()]
        if not pk_cols and "id" in column_defs:
            pk_cols = ["id"]
        if pk_cols:
            constraints.append(f"  PRIMARY KEY (`{pk_cols[0]}`)")

    body = ",\n".join(col_lines)
    if constraints:
        body += ",\n" + ",\n".join(constraints)

    return (
        f"CREATE TABLE `{table_name}` (\n{body}\n"
        f") ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={collate};"
    )
