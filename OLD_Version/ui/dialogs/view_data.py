"""
ui/dialogs/view_data.py
-----------------------
General-purpose data viewer for MySQL table contents.

Features:
    * Treeview display (capped at 5,000 displayed rows for performance).
    * Download full data as CSV or JSON.
    * Handles bytes, Decimal, and datetime objects in display and export.
"""
from __future__ import annotations

import csv
import datetime
import decimal
import json
import re
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any

from config import CONFIG
from ui.utils import center_window

_MAX_ROWS_DISPLAY = 5_000


def _to_str(value: Any) -> str:
    """Convert any value to a safe display string."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return f"<{len(value)} bytes>"
    return str(value)


def _safe_json(obj: Any) -> Any:
    """JSON-serialise types not natively supported by json.dumps."""
    if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return obj.hex()
    raise TypeError(f"Unserializable type: {type(obj)}")


class ViewDataDialog:
    """
    Modal window for browsing and downloading table data.

    Args:
        parent:     Parent window.
        table_name: Name of the table (used in dialog title and filename).
        columns:    Column header names.
        data:       Full data as a list of row tuples.
    """

    def __init__(
        self,
        parent: tk.Widget,
        table_name: str,
        columns: list[str],
        data: list[tuple],
    ) -> None:
        self._table = table_name
        self._columns = columns
        self._data = data

        win = tk.Toplevel(parent)
        win.title(f"Data: {table_name}  ({len(data)} rows)")
        win.transient(parent)
        center_window(win, 900, 550)

        self._build_ui(win)
        win.wait_window()

    def _build_ui(self, win: tk.Toplevel) -> None:
        # --- Treeview ---
        tree_frame = ttk.Frame(win)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(8, 4))
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        tree = ttk.Treeview(tree_frame, columns=self._columns, show="headings")
        sy = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        sx = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
        tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        sx.grid(row=1, column=0, sticky="ew")

        col_width = max(80, min(160, (870 - 20) // max(len(self._columns), 1)))
        for col in self._columns:
            tree.heading(col, text=col, anchor="w")
            tree.column(col, width=col_width, anchor="w", stretch=tk.YES)

        display = self._data[:_MAX_ROWS_DISPLAY]
        for row in display:
            tree.insert("", tk.END, values=[_to_str(v) for v in row])

        if len(self._data) > _MAX_ROWS_DISPLAY:
            tree.insert(
                "", tk.END,
                values=(f"… displaying first {_MAX_ROWS_DISPLAY:,} of {len(self._data):,} rows …",)
            )

        # --- Download bar ---
        dl_frame = ttk.Frame(win)
        dl_frame.pack(fill=tk.X, padx=8, pady=(4, 8))
        ttk.Label(
            dl_frame,
            text=f"{len(self._data):,} rows total  |  Download full data:",
            font=(CONFIG.ui.font_family, CONFIG.ui.font_size),
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(dl_frame, text="CSV", command=self._download_csv, width=8).pack(side=tk.LEFT, padx=4)
        ttk.Button(dl_frame, text="JSON", command=self._download_json, width=8).pack(side=tk.LEFT, padx=4)

    def _safe_filename(self) -> str:
        return re.sub(r"[^\w\-]+", "_", self._table)

    def _download_csv(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save as CSV",
            initialfile=f"{self._safe_filename()}_data.csv",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(self._columns)
                for row in self._data:
                    writer.writerow([
                        "" if v is None else (
                            v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v)
                        )
                        for v in row
                    ])
            messagebox.showinfo("Exported", f"Saved {len(self._data):,} rows to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error", f"CSV export failed:\n{exc}")

    def _download_json(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save as JSON",
            initialfile=f"{self._safe_filename()}_data.json",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            payload = [dict(zip(self._columns, row)) for row in self._data]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=_safe_json)
            messagebox.showinfo("Exported", f"Saved {len(self._data):,} rows to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export Error", f"JSON export failed:\n{exc}")
