"""
ui/dialogs/map_columns.py
-------------------------
Dialog for setting explicit column name mappings between source and target.
Supports both single mappings and split mappings (target selection via Combobox).
"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from models.mapping import SingleMapping, SplitMapping
from ui.utils import center_window, ToolTip


class MapColumnsDialog:
    """
    Column mapping dialog.

    Args:
        parent:       Parent window.
        source_table: Source DB table name.
        controller:   AppController.
        on_done:      Callback after any save.
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

        mapping = controller.store.get(source_table)
        if mapping is None or isinstance(mapping, type(None)):
            messagebox.showinfo("Info", f"'{source_table}' is not mapped yet. Map it first.", parent=parent)
            return

        if isinstance(mapping, SingleMapping):
            self._target_names = [mapping.target_schema_name]
        elif isinstance(mapping, SplitMapping):
            self._target_names = mapping.target_schema_names
        else:
            messagebox.showinfo("Info", "Column mapping is not available for merge mappings.", parent=parent)
            return

        self._mapping = mapping

        # Source columns
        db_schema = controller.db.describe_table(source_table)
        if not db_schema:
            messagebox.showerror("Error", f"Cannot read schema for '{source_table}'.", parent=parent)
            return
        self._db_cols: list[str] = sorted(db_schema.keys())

        self._win = win = tk.Toplevel(parent)
        win.title(f"Map Columns: {source_table}")
        win.transient(parent)
        win.grab_set()
        center_window(win, 680, 480)

        self._build_ui()
        win.wait_window()

    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        win = self._win

        # Target selector
        top = ttk.Frame(win, padding=(10, 8, 10, 4))
        top.pack(fill=tk.X)
        ttk.Label(top, text="Target Table:").pack(side=tk.LEFT, padx=(0, 6))
        self._tgt_var = tk.StringVar(win)
        self._tgt_combo = ttk.Combobox(
            top, textvariable=self._tgt_var, values=self._target_names, state="readonly", width=30
        )
        self._tgt_combo.pack(side=tk.LEFT)
        if self._target_names:
            self._tgt_combo.current(0)
        self._tgt_var.trace_add("write", lambda *_: self._refresh_display())

        # Mapping area
        main = ttk.Frame(win, padding=(10, 0, 10, 0))
        main.pack(fill=tk.BOTH, expand=True)
        main.columnconfigure(0, weight=1)
        main.columnconfigure(2, weight=1)
        main.rowconfigure(1, weight=1)

        # Source column selector
        ttk.Label(main, text=f"Source ({self._source}):").grid(row=0, column=0, sticky="w")
        self._src_var = tk.StringVar(win)
        self._src_combo = ttk.Combobox(main, textvariable=self._src_var, state="readonly", width=26)
        self._src_combo.grid(row=1, column=0, sticky="ew", pady=4)

        ttk.Label(main, text="→").grid(row=1, column=1, padx=8)

        # Target column selector
        ttk.Label(main, text="Target:").grid(row=0, column=2, sticky="w")
        self._tgt_col_var = tk.StringVar(win)
        self._tgt_col_combo = ttk.Combobox(main, textvariable=self._tgt_col_var, state="readonly", width=26)
        self._tgt_col_combo.grid(row=1, column=2, sticky="ew", pady=4)

        # Existing mappings display
        exist_lf = ttk.LabelFrame(main, text="Existing Mappings (source_col → target_col)", padding=6)
        exist_lf.grid(row=2, column=0, columnspan=3, sticky="nsew", pady=6)
        exist_lf.rowconfigure(0, weight=1)
        exist_lf.columnconfigure(0, weight=1)
        self._map_list = tk.Listbox(exist_lf, font=("Courier New", 9), exportselection=False)
        sb = ttk.Scrollbar(exist_lf, orient=tk.VERTICAL, command=self._map_list.yview)
        self._map_list.config(yscrollcommand=sb.set)
        self._map_list.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")

        # Buttons
        btn_frame = ttk.Frame(win, padding=(10, 6))
        btn_frame.pack(fill=tk.X)
        self._add_btn = ttk.Button(btn_frame, text="Add Mapping", command=self._add_mapping, width=14)
        self._add_btn.pack(side=tk.LEFT, padx=(0, 6))
        self._rm_btn = ttk.Button(btn_frame, text="Remove Selected", command=self._remove_mapping, width=16)
        self._rm_btn.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_frame, text="Close", command=self._close, width=10).pack(side=tk.LEFT)

        ToolTip(self._add_btn, "Add the selected source → target column pair.")
        ToolTip(self._rm_btn, "Remove the selected column mapping.")

        self._refresh_display()

    def _current_target(self) -> str:
        return self._tgt_var.get()

    def _current_col_maps(self) -> dict[str, str]:
        tgt = self._current_target()
        m = self._mapping
        if isinstance(m, SingleMapping):
            return dict(m.column_mappings)
        if isinstance(m, SplitMapping):
            return dict(m.column_mappings_for(tgt))
        return {}

    def _refresh_display(self) -> None:
        tgt = self._current_target()
        if not tgt:
            return

        tgt_schema = self._ctrl.schema.get(tgt, {})
        col_maps = self._current_col_maps()
        mapped_src = set(col_maps.keys())
        mapped_tgt = set(col_maps.values())

        unmapped_src = [c for c in self._db_cols if c not in mapped_src]
        unmapped_tgt = [c for c in tgt_schema if c not in mapped_tgt]

        self._src_combo["values"] = unmapped_src
        self._tgt_col_combo["values"] = unmapped_tgt
        if unmapped_src:
            self._src_combo.current(0)
        if unmapped_tgt:
            self._tgt_col_combo.current(0)

        self._map_list.delete(0, tk.END)
        for src_c, tgt_c in sorted(col_maps.items()):
            self._map_list.insert(tk.END, f"{src_c}  →  {tgt_c}")

    def _add_mapping(self) -> None:
        src_col = self._src_var.get().strip()
        tgt_col = self._tgt_col_var.get().strip()
        if not src_col or not tgt_col:
            messagebox.showwarning("Incomplete", "Select both a source column and a target column.", parent=self._win)
            return
        split_target = self._current_target() if isinstance(self._mapping, SplitMapping) else None
        self._ctrl.store.set_column_mapping(
            source_table=self._source,
            old_col=src_col,
            new_col=tgt_col,
            split_target=split_target,
        )
        # Reload mapping from store
        self._mapping = self._ctrl.store.get(self._source)
        self._refresh_display()
        self._on_done()

    def _remove_mapping(self) -> None:
        sel = self._map_list.curselection()
        if not sel:
            return
        entry = self._map_list.get(sel[0])
        _, tgt_col = [p.strip() for p in entry.split("→", 1)]
        split_target = self._current_target() if isinstance(self._mapping, SplitMapping) else None
        self._ctrl.store.remove_column_mapping(
            source_table=self._source,
            new_col=tgt_col,
            split_target=split_target,
        )
        self._mapping = self._ctrl.store.get(self._source)
        self._refresh_display()
        self._on_done()

    def _close(self) -> None:
        self._win.destroy()
