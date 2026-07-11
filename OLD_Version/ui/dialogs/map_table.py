"""
ui/dialogs/map_table.py
-----------------------
Dialog for mapping a single source DB table to one schema target.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from ui.utils import center_window, ToolTip


class MapTableDialog:
    """
    Modal dialog to map *source_table* → one schema table name.

    Args:
        parent:       Parent window.
        source_table: The source DB table name.
        controller:   AppController instance.
        on_done:      Callback invoked (no args) after any mapping change.
    """

    def __init__(
        self,
        parent: tk.Widget,
        source_table: str,
        controller,
        on_done: Callable[[], None],
    ) -> None:
        self._ctrl = controller
        self._source = source_table
        self._on_done = on_done

        store = controller.store
        schema = controller.schema

        # Gather available targets
        all_schema_names = sorted(schema.keys())
        current_mapping = store.get(source_table)
        current_target = (
            current_mapping.target_schema_name
            if hasattr(current_mapping, "target_schema_name")
            else None
        )
        taken = store.all_mapped_targets(exclude_key=source_table)
        # Always include current target even if "taken" by this key
        available = [
            n for n in all_schema_names
            if n not in taken or n == current_target
        ]

        if not available:
            messagebox.showinfo(
                "No Targets Available",
                "All schema tables are already mapped to other sources.",
                parent=parent,
            )
            return

        self._win = win = tk.Toplevel(parent)
        win.title(f"Map '{source_table}' (Single)")
        win.transient(parent)
        win.grab_set()
        win.resizable(False, False)
        center_window(win, 420, 200)

        ttk.Label(win, text=f"Map source table '{source_table}' to schema table:").pack(pady=(14, 6), padx=16)

        self._target_var = tk.StringVar(win)
        combo = ttk.Combobox(win, textvariable=self._target_var, values=available, state="readonly", width=34)
        combo.pack(padx=16, pady=4)
        if current_target and current_target in available:
            self._target_var.set(current_target)
        elif available:
            combo.current(0)
        ToolTip(combo, "Select the target table name from the schema file.")

        btn_frame = ttk.Frame(win)
        btn_frame.pack(pady=12)

        ttk.Button(btn_frame, text="Confirm Map", command=self._confirm, width=14).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Unmap", command=self._unmap, width=10).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Cancel", command=win.destroy, width=10).pack(side=tk.LEFT, padx=6)

        win.wait_window()

    def _confirm(self) -> None:
        target = self._target_var.get().strip()
        if not target:
            messagebox.showwarning("No Selection", "Please select a target table.", parent=self._win)
            return
        self._ctrl.store.set_single(self._source, target)
        self._on_done()
        self._win.destroy()

    def _unmap(self) -> None:
        removed = self._ctrl.store.remove(self._source)
        if removed:
            messagebox.showinfo(
                "Unmapped",
                f"Mapping for '{self._source}' removed.",
                parent=self._win,
            )
        self._on_done()
        self._win.destroy()
