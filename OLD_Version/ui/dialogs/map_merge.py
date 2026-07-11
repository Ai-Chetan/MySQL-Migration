"""
ui/dialogs/map_merge.py
-----------------------
Dialog for configuring a merge mapping: multiple sources → one target.
Includes an "Auto-Generate" button that attempts to produce JOIN conditions
and column mappings based on foreign key / name matching heuristics.
"""
from __future__ import annotations

import datetime
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from models.mapping import MergeMapping
from ui.utils import center_window, ToolTip, scrolled_text


class MapMergeDialog:
    """
    Dialog for configuring a merge mapping.

    Args:
        parent:     Parent window.
        controller: AppController instance.
        on_done:    Callback after saving.
    """

    def __init__(
        self,
        parent: tk.Widget,
        controller,
        on_done: Callable[[], None],
    ) -> None:
        self._ctrl = controller
        self._on_done = on_done

        try:
            all_db_tables = sorted(controller.db.list_tables())
        except Exception:
            all_db_tables = []
        all_schema_names = sorted(controller.schema.keys())

        self._win = win = tk.Toplevel(parent)
        win.title("Merge Multiple Tables into One")
        win.transient(parent)
        win.grab_set()
        center_window(win, 760, 680)

        main = ttk.Frame(win, padding=10)
        main.pack(fill=tk.BOTH, expand=True)
        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(3, weight=1)
        main.rowconfigure(4, weight=1)

        # --- Source tables ---
        src_lf = ttk.LabelFrame(main, text="1. Source Tables (DB)", padding=8)
        src_lf.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 6))
        src_lf.rowconfigure(0, weight=1)
        src_lf.columnconfigure(0, weight=1)
        self._src_list = tk.Listbox(src_lf, selectmode=tk.MULTIPLE, exportselection=False, height=6)
        sb_src = ttk.Scrollbar(src_lf, orient=tk.VERTICAL, command=self._src_list.yview)
        self._src_list.config(yscrollcommand=sb_src.set)
        for t in all_db_tables:
            self._src_list.insert(tk.END, t)
        self._src_list.grid(row=0, column=0, sticky="nsew")
        sb_src.grid(row=0, column=1, sticky="ns")
        ToolTip(self._src_list, "Hold Ctrl/Cmd to select multiple tables. The FIRST selected table is the primary (FROM table).")

        # --- Target table ---
        tgt_lf = ttk.LabelFrame(main, text="2. Target Schema Table", padding=8)
        tgt_lf.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 6))
        self._tgt_var = tk.StringVar(win)
        tgt_combo = ttk.Combobox(tgt_lf, textvariable=self._tgt_var, values=all_schema_names, state="readonly", width=32)
        tgt_combo.pack(pady=8)
        if all_schema_names:
            tgt_combo.current(0)
        ToolTip(tgt_combo, "Select the target table name from the schema file.")

        # --- Auto-generate ---
        auto_btn = ttk.Button(main, text="3. Auto-Generate JOIN & Column Mappings", command=self._auto_generate)
        auto_btn.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ToolTip(auto_btn, "Attempt to produce JOIN conditions and column mappings from table schemas.")

        # --- JOIN conditions ---
        join_lf = ttk.LabelFrame(main, text="4. JOIN Conditions (SQL snippet)", padding=6)
        join_lf.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(0, 6))
        join_lf.rowconfigure(0, weight=1)
        join_lf.columnconfigure(0, weight=1)
        self._join_text, _ = scrolled_text(join_lf, height=4, wrap=tk.WORD)
        _.pack(fill=tk.BOTH, expand=True)
        self._join_text.insert(
            "1.0",
            "# Example: INNER JOIN table2 ON table1.id = table2.t1_id\n"
            "# Click Auto-Generate or enter manually.",
        )

        # --- Column mappings ---
        map_lf = ttk.LabelFrame(main, text="5. Column Mappings  (format: source_table.column -> target_column)", padding=6)
        map_lf.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(0, 6))
        map_lf.rowconfigure(0, weight=1)
        map_lf.columnconfigure(0, weight=1)
        self._map_text, _ = scrolled_text(map_lf, height=10, wrap=tk.NONE, font=("Courier New", 9))
        _.pack(fill=tk.BOTH, expand=True)
        self._map_text.insert("1.0", "# Click Auto-Generate or enter manually.")

        # --- Buttons ---
        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=(6, 0))
        ttk.Button(btn_frame, text="Save Merge", command=self._confirm, width=16).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Cancel", command=win.destroy, width=10).pack(side=tk.LEFT, padx=6)

        win.wait_window()

    # ------------------------------------------------------------------

    def _get_selected_sources(self) -> list[str]:
        return [self._src_list.get(i) for i in self._src_list.curselection()]

    def _auto_generate(self) -> None:
        sources = self._get_selected_sources()
        target = self._tgt_var.get().strip()
        if not sources or not target:
            messagebox.showwarning("Incomplete", "Select source tables and a target schema table first.", parent=self._win)
            return

        target_schema = self._ctrl.schema.get(target, {})
        db = self._ctrl.db

        # Fetch source schemas
        src_schemas: dict[str, dict] = {}
        for s in sources:
            src_schemas[s] = db.describe_table(s)

        # --- Attempt JOIN generation ---
        join_lines: list[str] = []
        for i in range(1, len(sources)):
            prev = sources[i - 1]
            curr = sources[i]
            prev_schema = src_schemas.get(prev, {})
            curr_schema = src_schemas.get(curr, {})
            # Heuristic: look for a column in curr that references prev's PK
            prev_pk = db.primary_key_column(prev)
            join_col = None
            for col in curr_schema:
                if col.lower() in (f"{prev}_id", f"{prev.rstrip('s')}_id", f"fk_{prev}_id"):
                    join_col = col
                    break
            if prev_pk and join_col:
                join_lines.append(f"INNER JOIN `{curr}` ON `{prev}`.`{prev_pk}` = `{curr}`.`{join_col}`")
            else:
                join_lines.append(f"-- TODO: INNER JOIN `{curr}` ON `{prev}`.id = `{curr}`.fk_id")

        self._join_text.delete("1.0", tk.END)
        self._join_text.insert("1.0", "\n".join(join_lines) if join_lines else "-- No JOIN generated. Enter manually.")

        # --- Attempt column mapping generation ---
        map_lines: list[str] = []
        for target_col in target_schema:
            # Look for same-named col across source tables
            found = False
            for src_tbl, src_sch in src_schemas.items():
                if target_col in src_sch:
                    map_lines.append(f"{src_tbl}.{target_col} -> {target_col}")
                    found = True
                    break
            if not found:
                map_lines.append(f"# TODO: ?.{target_col} -> {target_col}")

        self._map_text.delete("1.0", tk.END)
        self._map_text.insert("1.0", "\n".join(map_lines))

    def _confirm(self) -> None:
        sources = self._get_selected_sources()
        target = self._tgt_var.get().strip()
        if not sources:
            messagebox.showerror("Error", "Select at least 2 source tables.", parent=self._win)
            return
        if len(sources) < 2:
            messagebox.showerror("Error", "A merge requires at least 2 source tables.", parent=self._win)
            return
        if not target:
            messagebox.showerror("Error", "Select a target schema table.", parent=self._win)
            return

        join_raw = self._join_text.get("1.0", tk.END).strip()
        join_conditions = "\n".join(
            line for line in join_raw.splitlines() if not line.strip().startswith("#")
        ).strip()

        map_raw = self._map_text.get("1.0", tk.END).strip()
        col_mappings: dict[str, str] = {}
        parse_errors: list[str] = []
        for line in map_raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "->" not in line:
                parse_errors.append(line)
                continue
            left, right = line.split("->", 1)
            col_mappings[left.strip()] = right.strip()

        if parse_errors:
            messagebox.showwarning(
                "Parse Warnings",
                f"Skipped malformed mapping lines:\n" + "\n".join(parse_errors),
                parent=self._win,
            )

        if not col_mappings:
            messagebox.showerror("Error", "No valid column mappings defined.", parent=self._win)
            return

        merge_key = f"merge_{target}_{int(datetime.datetime.now().timestamp())}"
        mapping = MergeMapping(
            merge_key=merge_key,
            source_tables=sources,
            target_schema_name=target,
            join_conditions=join_conditions,
            column_mappings=col_mappings,
        )
        self._ctrl.store.set_mapping(merge_key, mapping)
        self._on_done()
        self._win.destroy()
