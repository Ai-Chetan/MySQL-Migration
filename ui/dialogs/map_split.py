"""
ui/dialogs/map_split.py
-----------------------
Dialog for configuring a split mapping: one source → multiple targets.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from models.mapping import SplitMapping, SplitTarget
from ui.utils import center_window, ToolTip


class MapSplitDialog:
    """
    Dialog to define which schema tables a source table should be split into.

    Args:
        parent:       Parent window.
        source_table: Source DB table name.
        controller:   AppController instance.
        on_done:      Callback invoked after saving.
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

        # Current targets (mutable list of schema name strings)
        current_m = controller.store.get(source_table)
        self._targets: list[str] = (
            current_m.target_schema_names
            if isinstance(current_m, SplitMapping)
            else []
        )
        current_set = set(self._targets)

        all_schema = sorted(controller.schema.keys())
        taken = controller.store.all_mapped_targets(exclude_key=source_table)
        self._available_all = all_schema  # full list (for repopulating combo)
        self._taken = taken

        self._win = win = tk.Toplevel(parent)
        win.title(f"Split '{source_table}' into Multiple Targets")
        win.transient(parent)
        win.grab_set()
        center_window(win, 520, 420)

        ttk.Label(win, text=f"Define target tables for source '{source_table}':").pack(pady=(12, 4), padx=12)

        # --- Target listbox ---
        list_frame = ttk.LabelFrame(win, text="Assigned Targets", padding=6)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=4)
        list_frame.rowconfigure(0, weight=1)
        list_frame.columnconfigure(0, weight=1)
        self._listbox = tk.Listbox(list_frame, selectmode=tk.SINGLE, exportselection=False)
        sb = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self._listbox.yview)
        self._listbox.config(yscrollcommand=sb.set)
        self._listbox.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")
        for t in self._targets:
            self._listbox.insert(tk.END, t)

        # --- Add / remove ---
        ctrl_frame = ttk.Frame(win)
        ctrl_frame.pack(fill=tk.X, padx=12, pady=4)
        self._add_var = tk.StringVar(win)
        available = [n for n in all_schema if n not in current_set and n not in taken]
        self._add_combo = ttk.Combobox(
            ctrl_frame, textvariable=self._add_var, values=available, state="readonly", width=26
        )
        self._add_combo.grid(row=0, column=0, padx=(0, 4))
        if available:
            self._add_combo.current(0)
        ttk.Button(ctrl_frame, text="Add Target", command=self._add_target).grid(row=0, column=1, padx=4)
        ttk.Button(ctrl_frame, text="Remove Selected", command=self._remove_target).grid(row=0, column=2, padx=4)
        ToolTip(self._add_combo, "Choose a schema table to add as a split target.")

        # --- Buttons ---
        btn_frame = ttk.Frame(win)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Save Split", command=self._confirm, width=14).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Cancel", command=win.destroy, width=10).pack(side=tk.LEFT, padx=6)

        win.wait_window()

    def _add_target(self) -> None:
        name = self._add_var.get().strip()
        if not name or name in self._targets:
            return
        self._targets.append(name)
        self._listbox.insert(tk.END, name)
        self._refresh_combo()

    def _remove_target(self) -> None:
        sel = self._listbox.curselection()
        if not sel:
            return
        name = self._listbox.get(sel[0])
        self._targets.remove(name)
        self._listbox.delete(sel[0])
        self._refresh_combo()

    def _refresh_combo(self) -> None:
        current_set = set(self._targets)
        available = [n for n in self._available_all if n not in current_set and n not in self._taken]
        self._add_combo["values"] = available
        if available:
            self._add_combo.current(0)

    def _confirm(self) -> None:
        if len(self._targets) < 2:
            messagebox.showwarning(
                "Too Few Targets",
                "A split mapping requires at least 2 target tables.",
                parent=self._win,
            )
            return

        existing = self._ctrl.store.get(self._source)
        # Preserve existing per-target column mappings if target is still present
        existing_col_maps: dict[str, dict] = {}
        if isinstance(existing, SplitMapping):
            for t in existing.targets:
                existing_col_maps[t.schema_name] = t.column_mappings

        new_targets = [
            SplitTarget(
                schema_name=name,
                column_mappings=existing_col_maps.get(name, {}),
            )
            for name in self._targets
        ]
        mapping = SplitMapping(source_table=self._source, targets=new_targets)
        self._ctrl.store.set_mapping(self._source, mapping)
        self._on_done()
        self._win.destroy()
