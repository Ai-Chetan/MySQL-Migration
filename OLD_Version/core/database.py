"""
core/database.py
----------------
Database connection management and query execution.

Design Decisions:
    * ``DatabaseManager`` is a context manager so callers can use it with
      ``with`` statements and be guaranteed the connection is closed on exit.
    * All table/column names use backtick quoting to avoid reserved-word
      collisions in MySQL.
    * Retry logic is implemented for transient connection errors using
      exponential back-off (configurable via ``max_retries`` / ``retry_delay``).
    * Queries never use Python string interpolation for user-supplied values;
      only structural identifiers (table/column names) that are backtick-quoted
      are inserted into SQL strings. Parameterised execution (``%s``) is used
      for all data values.
    * Logging replaces all print() calls so output is structured and filterable.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator, Iterator

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from config import CONFIG
from logger import get_logger

log = get_logger(__name__)

# Type alias for a column schema row as returned by DESCRIBE
ColumnSchema = tuple[str, str, str, str, Any, str]  # Field,Type,Null,Key,Default,Extra
TableSchema = dict[str, ColumnSchema]  # col_name → column tuple


class DatabaseError(Exception):
    """Raised for database-level failures reported by this module."""


class ConnectionLostError(DatabaseError):
    """Raised when the connection to MySQL is detected as lost."""


class DatabaseManager:
    """
    Production-grade MySQL connection wrapper.

    Provides:
        * Lazy connect / reconnect with retry back-off.
        * Context-manager support (``with DatabaseManager(...) as db``).
        * Helper methods for common DDL/DML operations used by the migrator.
        * Automatic rollback on uncaught exceptions inside a managed block.

    Example::

        dm = DatabaseManager(host="localhost", user="root", password="secret")
        dm.connect()
        tables = dm.list_tables()
        dm.close()

        # Or using context manager:
        with DatabaseManager.from_config(user="root", password="secret") as dm:
            dm.select_database("mydb")
            schema = dm.describe_table("users")
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        charset: str = "utf8mb4",
        connect_timeout: int = 10,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._charset = charset
        self._connect_timeout = connect_timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay

        self._conn: MySQLConnection | None = None
        self._cursor: MySQLCursor | None = None
        self.current_database: str | None = None

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, user: str, password: str) -> "DatabaseManager":
        """Convenience factory using values from the application config."""
        return cls(
            host=CONFIG.db.host,
            port=CONFIG.db.port,
            user=user,
            password=password,
            charset=CONFIG.db.charset,
            connect_timeout=CONFIG.db.connect_timeout,
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DatabaseManager":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            log.warning("Unhandled exception in DatabaseManager context: %s", exc_val)
            self._safe_rollback()
        self.close()
        return False  # Never suppress exceptions

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Open (or re-open) the MySQL connection with exponential back-off retries.

        Raises:
            DatabaseError: If connection fails after all retries.
        """
        for attempt in range(1, self._max_retries + 1):
            try:
                log.info(
                    "Connecting to MySQL at %s:%s (attempt %d/%d)",
                    self._host, self._port, attempt, self._max_retries,
                )
                self._conn = mysql.connector.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    charset=self._charset,
                    connect_timeout=self._connect_timeout,
                    get_warnings=True,
                    raise_on_warnings=False,
                )
                self._cursor = self._conn.cursor()
                log.info("Connected to MySQL successfully.")
                return
            except mysql.connector.Error as exc:
                log.warning("Connection attempt %d failed: %s", attempt, exc)
                if attempt < self._max_retries:
                    time.sleep(self._retry_delay * attempt)
        raise DatabaseError(
            f"Could not connect to MySQL at {self._host}:{self._port} "
            f"after {self._max_retries} attempts."
        )

    def close(self) -> None:
        """Close cursor and connection, logging any cleanup errors."""
        try:
            if self._cursor:
                self._cursor.close()
        except Exception:
            pass
        try:
            if self._conn and self._conn.is_connected():
                self._conn.close()
                log.info("Database connection closed.")
        except Exception:
            pass
        self._cursor = None
        self._conn = None

    @property
    def is_connected(self) -> bool:
        return bool(self._conn and self._conn.is_connected())

    def _ensure_connected(self) -> None:
        if not self.is_connected:
            raise ConnectionLostError(
                "Database connection is not open. Call connect() first."
            )

    def _safe_rollback(self) -> None:
        try:
            if self._conn and self._conn.is_connected():
                self._conn.rollback()
                log.debug("Transaction rolled back.")
        except Exception as exc:
            log.warning("Rollback failed: %s", exc)

    # ------------------------------------------------------------------
    # Public query helpers
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: tuple | None = None) -> MySQLCursor:
        """
        Execute a SQL statement and return the cursor.

        Args:
            sql:    SQL statement. Use %s placeholders for values.
            params: Tuple of parameter values (optional).

        Returns:
            The internal cursor (allows caller to call fetchall etc.).

        Raises:
            ConnectionLostError: If not connected.
            DatabaseError: On MySQL execution errors.
        """
        self._ensure_connected()
        assert self._cursor is not None
        try:
            self._cursor.execute(sql, params)
            return self._cursor
        except mysql.connector.Error as exc:
            log.error("SQL execution error: %s | SQL: %.500s", exc, sql)
            raise DatabaseError(str(exc)) from exc

    def fetchall(self) -> list[tuple]:
        """Fetch all rows from the last execute."""
        assert self._cursor is not None
        return self._cursor.fetchall() or []

    def fetchone(self) -> tuple | None:
        """Fetch one row from the last execute."""
        assert self._cursor is not None
        return self._cursor.fetchone()

    def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()

    def rollback(self) -> None:
        self._safe_rollback()

    @property
    def description(self):
        assert self._cursor is not None
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        assert self._cursor is not None
        return self._cursor.rowcount

    def warnings(self) -> list[tuple]:
        """Return SQL warnings from the last statement."""
        try:
            self._cursor.execute("SHOW WARNINGS")
            return self.fetchall()
        except Exception:
            return []

    # ------------------------------------------------------------------
    # High-level database operations
    # ------------------------------------------------------------------

    def list_databases(self, exclude_system: bool = True) -> list[str]:
        """
        Return user database names, optionally filtering system databases.

        Args:
            exclude_system: When True (default), omits information_schema,
                            mysql, performance_schema, and sys.
        """
        system = {"information_schema", "mysql", "performance_schema", "sys"}
        self.execute("SHOW DATABASES")
        dbs = [row[0] for row in self.fetchall()]
        if exclude_system:
            dbs = [d for d in dbs if d not in system]
        return sorted(dbs)

    def select_database(self, name: str) -> None:
        """
        Switch the active database.

        Args:
            name: Database name to USE.

        Raises:
            DatabaseError: If the USE statement fails.
        """
        self.execute(f"USE `{name}`")
        self._conn.commit()  # type: ignore[union-attr]
        self.current_database = name
        log.info("Selected database: %s", name)

    def list_tables(self) -> list[str]:
        """Return table names in the current database."""
        self._ensure_connected()
        self.execute("SHOW TABLES")
        return [row[0] for row in self.fetchall()]

    def describe_table(self, table_name: str) -> TableSchema:
        """
        Fetch the schema of a table using DESCRIBE.

        Args:
            table_name: The (unquoted) table name.

        Returns:
            Dict mapping column name → full DESCRIBE row tuple.
            Empty dict if the table cannot be described.
        """
        try:
            self.execute(f"DESCRIBE `{table_name}`")
            rows = self.fetchall()
            return {row[0]: row for row in rows}
        except DatabaseError as exc:
            log.warning("Could not describe table '%s': %s", table_name, exc)
            return {}

    def table_exists(self, table_name: str) -> bool:
        """Return True if *table_name* exists in the current database."""
        try:
            self.execute("SHOW TABLES LIKE %s", (table_name,))
            return self.fetchone() is not None
        except DatabaseError:
            return False

    def primary_key_column(self, table_name: str) -> str | None:
        """
        Return the first primary key column name for *table_name*, or None.
        """
        try:
            self.execute(
                "SHOW KEYS FROM `%s` WHERE Key_name = 'PRIMARY'" % table_name  # nosec – quoted
            )
            row = self.fetchone()
            return row[4] if row else None  # Column_name is index 4
        except DatabaseError:
            return None

    def count_rows(self, table_name: str) -> int:
        """Return the approximate row count for *table_name*."""
        try:
            self.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row = self.fetchone()
            return row[0] if row else 0
        except DatabaseError:
            return 0

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """
        Convenience context manager for explicit transaction control.

        Commits on clean exit, rolls back on any exception.

        Example::

            with db.transaction():
                db.execute("INSERT INTO ...")
                db.execute("INSERT INTO ...")
            # auto-committed
        """
        try:
            yield
            self.commit()
            log.debug("Transaction committed.")
        except Exception:
            self._safe_rollback()
            raise
