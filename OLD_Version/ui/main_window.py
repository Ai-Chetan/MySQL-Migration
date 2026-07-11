"""
ui/main_window.py
-----------------
Main application window: table lists, schema comparison trees, action
buttons, and the confirmation-gate + migration trigger.

Design Decisions:
    * The window only manages layout and user interaction. All business
      logic is delegated to AppController or the dialog classes.
    * Table list colouring uses a dedicated method ``_get_table_color``
      that encodes the decision rules clearly and is easy to extend.
    * The schema comparison treeviews are populated by the controller's
      ``analyse_mapping`` result rather than by direct DB calls, so the
      UI never constructs SQL.
    * The status bar at the bottom provides rolling feedback without modal
      dialogs for non-critical events (selection changed, refresh started, etc.).
"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk, filedialog

from config import CONFIG
from core.database import DatabaseError
from core.migrator import MigrationError
from core.schema_parser import parse_column_definition
from logger import get_logger
from models.mapping import SingleMapping, SplitMapping, MergeMapping
from ui.utils import (
    center_window,
    configure_style,
    set_window_icon,
    ToolTip,
    ProgressDialog,
)

log = get_logger(__name__)

# Colour codes for the "Old/Source" listbox
_COLOUR_UNMAP_NO_SCHEMA = "red"
_COLOUR_UNMAP_IN_SCHEMA = "orange"
_COLOUR_MAP_NO_SCHEMA = "purple"
_COLOUR_MAP_READY = "blue"
_COLOUR_SPLIT_READY = "darkcyan"
_COLOUR_SPLIT_DONE = "black"
_COLOUR_MERGE = "darkgreen"
_COLOUR_DONE = "black"
_COLOUR_ORPHAN = "grey"

# Schema diff tag names → background colours
_DIFF_TAGS: dict[str, str] = {
    "matching": "#E0E0E0",
    "changed": "#FFFACD",
    "renamed": "#ADD8E6",
    "removed": "#FFB6C1",
    "added": "#90EE90",
}


class MainWindow:
    """
    Primary application window shown after successful login.

    Args:
        controller: The :class:`~ui.app.AppController` instance.
    """

    def __init__(self, controller) -> None:
        self._ctrl = controller
        self._root = tk.Tk()
        self._root.title(CONFIG.app_name)
        configure_style()
        set_window_icon(self._root)
        center_window(self._root, CONFIG.ui.main_window_width, CONFIG.ui.main_window_height)
        self._root.minsize(CONFIG.ui.min_window_width, CONFIG.ui.min_window_height)

        self._constraint_vars: list[tk.BooleanVar] = []
        self._create_btn: ttk.Button | None = None
        self._status_var = tk.StringVar(value="Ready.")

        self._build_ui()
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._build_top_bar()
        self._build_status_bar()
        self._content_frame = ttk.Frame(self._root)
        self._content_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 6))
        self._content_frame.columnconfigure(0, weight=1, minsize=260)
        self._content_frame.columnconfigure(1, weight=4, minsize=440)
        self._content_frame.columnconfigure(2, weight=1, minsize=260)
        self._content_frame.rowconfigure(0, weight=1)

        self._build_left_panel()
        self._build_middle_panel()
        self._build_right_panel()

    # ---------- Top bar ----------

    def _build_top_bar(self) -> None:
        bar = ttk.Frame(self._root, padding=(8, 8, 8, 4))
        bar.pack(fill=tk.X)
        bar.columnconfigure(1, weight=1)

        ttk.Label(bar, text="Database:").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._db_combo = ttk.Combobox(bar, width=36, state="readonly")
        self._db_combo.grid(row=0, column=1, sticky="ew", padx=4)
        ToolTip(self._db_combo, "Select the MySQL database to migrate from.")

        ttk.Label(bar, text="Schema File:").grid(row=1, column=0, sticky="w", padx=(0, 4), pady=(4, 0))
        self._schema_entry = ttk.Entry(bar, width=60)
        self._schema_entry.grid(row=1, column=1, sticky="ew", padx=4, pady=(4, 0))
        ToolTip(self._schema_entry, "Path to the .txt schema definition file.")

        browse_btn = ttk.Button(bar, text="Browse…", command=self._browse_schema, width=9)
        browse_btn.grid(row=1, column=2, padx=(4, 0), pady=(4, 0))

        load_btn = ttk.Button(
            bar,
            text="Load DB & Schema",
            command=self._on_load,
            style="Action.TButton",
            width=18,
        )
        load_btn.grid(row=2, column=1, columnspan=2, sticky="e", pady=(6, 0))
        ToolTip(load_btn, "Connect to the selected database and parse the schema file.")

        # Populate databases
        try:
            dbs = self._ctrl.db.list_databases()
            self._db_combo["values"] = dbs
            if dbs:
                self._db_combo.current(0)
        except DatabaseError:
            pass

    # ---------- Status bar ----------

    def _build_status_bar(self) -> None:
        sb = ttk.Label(
            self._root,
            textvariable=self._status_var,
            style="Status.TLabel",
            anchor="w",
        )
        sb.pack(fill=tk.X, padx=8, pady=(0, 4))

    # ---------- Left panel (table lists) ----------

    def _build_left_panel(self) -> None:
        left = ttk.Frame(self._content_frame)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=4)
        left.rowconfigure(1, weight=1)
        left.rowconfigure(3, weight=1)
        left.columnconfigure(0, weight=1)

        ttk.Label(left, text="Source Tables (Old DB):").grid(row=0, column=0, sticky="w")
        old_frame = ttk.Frame(left)
        old_frame.grid(row=1, column=0, sticky="nsew")
        old_frame.rowconfigure(0, weight=1)
        old_frame.columnconfigure(0, weight=1)

        self._list_old = tk.Listbox(
            old_frame, exportselection=False,
            font=(CONFIG.ui.mono_font, 9), activestyle="dotbox",
        )
        sb_old = ttk.Scrollbar(old_frame, orient=tk.VERTICAL, command=self._list_old.yview)
        self._list_old.config(yscrollcommand=sb_old.set)
        self._list_old.grid(row=0, column=0, sticky="nsew")
        sb_old.grid(row=0, column=1, sticky="ns")
        self._list_old.bind("<<ListboxSelect>>", self._on_old_select)
        self._list_old.bind("<Double-Button-1>", lambda _: self._view_old_data())
        ToolTip(self._list_old, "Select a table or mapping. Double-click to view data.")

        ttk.Label(left, text="Generated Tables (_new):").grid(row=2, column=0, sticky="w", pady=(6, 0))
        new_frame = ttk.Frame(left)
        new_frame.grid(row=3, column=0, sticky="nsew")
        new_frame.rowconfigure(0, weight=1)
        new_frame.columnconfigure(0, weight=1)

        self._list_new = tk.Listbox(
            new_frame, exportselection=False,
            font=(CONFIG.ui.mono_font, 9), activestyle="dotbox",
        )
        sb_new = ttk.Scrollbar(new_frame, orient=tk.VERTICAL, command=self._list_new.yview)
        self._list_new.config(yscrollcommand=sb_new.set)
        self._list_new.grid(row=0, column=0, sticky="nsew")
        sb_new.grid(row=0, column=1, sticky="ns")
        self._list_new.bind("<Double-Button-1>", lambda _: self._view_new_data())
        ToolTip(self._list_new, "Double-click to view data in a generated table.")

    # ---------- Middle panel (schema trees) ----------

    def _build_middle_panel(self) -> None:
        mid = ttk.Frame(self._content_frame)
        mid.grid(row=0, column=1, sticky="nsew", padx=4, pady=4)
        mid.rowconfigure(0, weight=1)
        mid.rowconfigure(2, weight=1)
        mid.columnconfigure(0, weight=1)

        cols = ("Field", "Type", "Null", "Key", "Default", "Extra")
        col_widths = (110, 140, 50, 45, 90, 100)

        def make_tree(parent: tk.Widget, label_text: str, row: int):
            lf = ttk.LabelFrame(parent, text=label_text, padding=4)
            lf.grid(row=row, column=0, sticky="nsew", pady=(0, 4))
            lf.rowconfigure(0, weight=1)
            lf.columnconfigure(0, weight=1)
            tree = ttk.Treeview(lf, columns=cols, show="headings", height=8)
            sy = ttk.Scrollbar(lf, orient=tk.VERTICAL, command=tree.yview)
            sx = ttk.Scrollbar(lf, orient=tk.HORIZONTAL, command=tree.xview)
            tree.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
            tree.grid(row=0, column=0, sticky="nsew")
            sy.grid(row=0, column=1, sticky="ns")
            sx.grid(row=1, column=0, sticky="ew")
            for col, w in zip(cols, col_widths):
                tree.heading(col, text=col, anchor="w")
                tree.column(col, width=w, anchor="w", stretch=True)
            for tag, colour in _DIFF_TAGS.items():
                tree.tag_configure(tag, background=colour)
            return tree, lf

        self._tree_old, self._frame_old = make_tree(mid, "Original Schema (DB)", 0)

        # Legend
        legend_frame = ttk.Frame(mid)
        legend_frame.grid(row=1, column=0, sticky="w", pady=(0, 2))
        legend_items = [
            ("Matching", "#E0E0E0"), ("Changed", "#FFFACD"),
            ("Renamed", "#ADD8E6"), ("Removed", "#FFB6C1"), ("Added", "#90EE90"),
        ]
        for i, (label, colour) in enumerate(legend_items):
            box = ttk.Label(legend_frame, width=10, text=label, background=colour,
                            relief=tk.RIDGE, font=(CONFIG.ui.font_family, 8))
            box.grid(row=0, column=i, padx=2)

        self._tree_new, self._frame_new = make_tree(mid, "Target Schema (File)", 2)

    # ---------- Right panel (actions + confirmation gate) ----------

    def _build_right_panel(self) -> None:
        right = ttk.Frame(self._content_frame)
        right.grid(row=0, column=2, sticky="nsew", padx=(4, 0), pady=4)
        right.columnconfigure(0, weight=1)

        # --- Mapping actions ---
        act = ttk.LabelFrame(right, text="Table Mapping", padding=8)
        act.pack(fill=tk.X, pady=(0, 6))
        act.columnconfigure(0, weight=1)

        buttons = [
            ("Map Table (Single) …", self._map_single, "Map one source table to one target schema table."),
            ("Split Table …", self._map_split, "Map one source table to multiple target tables."),
            ("Merge Tables …", self._map_merge, "Merge multiple source tables into one target table."),
            ("Map Columns …", self._map_columns, "Explicitly map column names between source and target."),
        ]
        for text, cmd, tip in buttons:
            btn = ttk.Button(act, text=text, command=cmd)
            btn.pack(fill=tk.X, pady=2)
            ToolTip(btn, tip)

        # --- View / Utility actions ---
        view = ttk.LabelFrame(right, text="View & Utilities", padding=8)
        view.pack(fill=tk.X, pady=(0, 6))
        view.columnconfigure(0, weight=1)

        util_buttons = [
            ("View Source Data", self._view_old_data, "Browse rows from the selected source table."),
            ("View Target Data", self._view_new_data, "Browse rows from the selected _new table."),
            ("Refresh", self._refresh, "Reload tables and schema from disk/database."),
            ("Generate Manual Script …", self._generate_script, "Create a Python migration script template."),
            ("Help", self._show_help, "Open the help reference."),
        ]
        for text, cmd, tip in util_buttons:
            btn = ttk.Button(view, text=text, command=cmd)
            btn.pack(fill=tk.X, pady=2)
            ToolTip(btn, tip)

        # --- Confirmation gate ---
        gate = ttk.LabelFrame(right, text="Safety Checklist", padding=8)
        gate.pack(fill=tk.X, pady=(0, 6))
        checks = [
            "Schemas compared?",
            "Data types reviewed?",
            "Mappings verified?",
            "Default values checked?",
            "Database backed up?",
            "Ready to proceed?",
        ]
        self._constraint_vars = []
        for check in checks:
            var = tk.BooleanVar()
            cb = ttk.Checkbutton(gate, text=check, variable=var,
                                 command=self._update_create_btn)
            cb.pack(anchor="w", pady=1)
            self._constraint_vars.append(var)
            ToolTip(cb, "Tick when you have confirmed this item.")

        ttk.Separator(right, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=6)

        self._create_btn = ttk.Button(
            right,
            text="CREATE Table(s) & Copy Data",
            command=self._on_create,
            state=tk.DISABLED,
            style="Action.TButton",
        )
        self._create_btn.pack(fill=tk.X, ipady=6)
        ToolTip(
            self._create_btn,
            "Creates the target table(s) and copies data based on the selected mapping.\n"
            "All safety checklist items must be ticked first.",
        )

    # ------------------------------------------------------------------
    # Event handlers — top bar
    # ------------------------------------------------------------------

    def _browse_schema(self) -> None:
        path = filedialog.askopenfilename(
            title="Select Schema Definition File",
            filetypes=[("Schema files", "*.txt;*.sql"), ("All files", "*.*")],
        )
        if path:
            self._schema_entry.delete(0, tk.END)
            self._schema_entry.insert(0, path)

    def _on_load(self) -> None:
        db_name = self._db_combo.get().strip()
        schema_path = self._schema_entry.get().strip()
        if not db_name:
            messagebox.showerror("Error", "Please select a database.", parent=self._root)
            return
        if not schema_path:
            messagebox.showerror("Error", "Please provide the schema file path.", parent=self._root)
            return

        self._set_status("Loading…")
        self._root.update_idletasks()
        ok, err = self._ctrl.on_db_schema_loaded(db_name, schema_path)
        if not ok:
            messagebox.showerror("Load Error", err, parent=self._root)
            self._set_status(f"Error: {err}")
            return

        self._set_status(
            f"Connected to '{db_name}' | Schema: {schema_path.split('/')[-1].split(chr(92))[-1]}"
        )
        self._refresh_table_lists()
        self._reset_checklist()

    def _refresh(self) -> None:
        if not self._ctrl.db or not self._ctrl.db.is_connected:
            return
        self._refresh_table_lists()
        self._set_status("Refreshed.")

    # ------------------------------------------------------------------
    # Event handlers — list selection
    # ------------------------------------------------------------------

    def _on_old_select(self, _event: tk.Event | None = None) -> None:
        sel = self._list_old.curselection()
        if not sel:
            return
        item = self._list_old.get(sel[0])
        self._show_schema_for(item)

    # ------------------------------------------------------------------
    # Event handlers — actions
    # ------------------------------------------------------------------

    def _map_single(self) -> None:
        sel = self._get_selected_old()
        if not sel:
            return
        if sel.startswith("MERGE:"):
            messagebox.showinfo("Info", "Cannot remap a merge entry.", parent=self._root)
            return
        from ui.dialogs.map_table import MapTableDialog
        MapTableDialog(
            parent=self._root,
            source_table=sel,
            controller=self._ctrl,
            on_done=self._refresh_table_lists,
        )

    def _map_split(self) -> None:
        sel = self._get_selected_old()
        if not sel or sel.startswith("MERGE:"):
            messagebox.showinfo("Info", "Select a regular source table.", parent=self._root)
            return
        from ui.dialogs.map_split import MapSplitDialog
        MapSplitDialog(
            parent=self._root,
            source_table=sel,
            controller=self._ctrl,
            on_done=self._refresh_table_lists,
        )

    def _map_merge(self) -> None:
        if not self._ctrl.schema:
            messagebox.showinfo("Info", "Load a database and schema first.", parent=self._root)
            return
        from ui.dialogs.map_merge import MapMergeDialog
        MapMergeDialog(
            parent=self._root,
            controller=self._ctrl,
            on_done=self._refresh_table_lists,
        )

    def _map_columns(self) -> None:
        sel = self._get_selected_old()
        if not sel or sel.startswith("MERGE:"):
            messagebox.showinfo("Info", "Select a single or split-mapped source table.", parent=self._root)
            return
        from ui.dialogs.map_columns import MapColumnsDialog
        MapColumnsDialog(
            parent=self._root,
            source_table=sel,
            controller=self._ctrl,
            on_done=lambda: (self._refresh_table_lists(), self._show_schema_for(sel)),
        )

    def _view_old_data(self) -> None:
        sel = self._get_selected_old()
        if not sel:
            messagebox.showinfo("Info", "Select a source table first.", parent=self._root)
            return
        if sel.startswith("MERGE:"):
            messagebox.showinfo("Info", "Select an individual source table to view data.", parent=self._root)
            return
        self._open_data_viewer(sel)

    def _view_new_data(self) -> None:
        # Prefer selection from _new list; fall back to deriving from old selection
        sel_new = self._list_new.curselection()
        if sel_new:
            self._open_data_viewer(self._list_new.get(sel_new[0]))
            return
        sel_old = self._get_selected_old()
        if sel_old and not sel_old.startswith("MERGE:"):
            m = self._ctrl.store.get(sel_old)
            if isinstance(m, SingleMapping):
                self._open_data_viewer(f"{m.target_schema_name}_new")
                return
        messagebox.showinfo("Info", "Select a table from the '_new' list or a mapped source.", parent=self._root)

    def _open_data_viewer(self, table_name: str) -> None:
        try:
            self._ctrl.db.execute(f"SELECT * FROM `{table_name}` LIMIT 10000")
            rows = self._ctrl.db.fetchall()
            cols = [d[0] for d in self._ctrl.db.description]
        except Exception as exc:
            messagebox.showerror("Error", f"Could not read table '{table_name}':\n{exc}", parent=self._root)
            return
        from ui.dialogs.view_data import ViewDataDialog
        ViewDataDialog(
            parent=self._root,
            table_name=table_name,
            columns=cols,
            data=rows,
        )

    def _generate_script(self) -> None:
        sel = self._get_selected_old()
        if not sel or sel.startswith("MERGE:"):
            messagebox.showinfo("Info", "Select a single or split-mapped source table.", parent=self._root)
            return
        try:
            path = self._ctrl.generate_manual_script(sel)
            messagebox.showinfo(
                "Script Generated",
                f"Migration script saved to:\n{path}\n\n"
                "Review and edit all # TODO: sections before running.",
                parent=self._root,
            )
        except MigrationError as exc:
            messagebox.showerror("Error", str(exc), parent=self._root)

    def _show_help(self) -> None:
        from ui.dialogs.help_dialog import HelpDialog
        HelpDialog(parent=self._root)

    def _on_create(self) -> None:
        if not all(v.get() for v in self._constraint_vars):
            messagebox.showwarning("Checklist Incomplete", "Please tick all safety checklist items.", parent=self._root)
            return
        sel = self._get_selected_old()
        if not sel:
            messagebox.showinfo("Info", "Select a table or mapping to migrate.", parent=self._root)
            return

        # Pre-flight lossy check
        plans = self._ctrl.analyse_mapping(sel)
        lossy = [
            f"  `{p.target_column}` ({p.source_type} → {p.target_type})"
            for plan in plans
            for p in plan.lossy_columns
        ]
        if lossy:
            msg = "Potential data loss detected:\n\n" + "\n".join(lossy) + "\n\nProceed anyway?"
            if not messagebox.askyesno("Data Loss Warning", msg, parent=self._root):
                return

        pdlg = ProgressDialog(self._root, "Migrating…")
        pdlg.show()
        try:
            results = self._ctrl.migrate_mapping(
                mapping_key=sel,
                confirm_lossy=True,
                progress_cb=pdlg.update,
            )
        except MigrationError as exc:
            pdlg.close()
            messagebox.showerror("Migration Error", str(exc), parent=self._root)
            return
        except Exception as exc:
            pdlg.close()
            messagebox.showerror("Unexpected Error", str(exc), parent=self._root)
            log.exception("Unexpected error during migration of '%s'", sel)
            return
        finally:
            pdlg.close()

        # Show summary
        success_count = sum(1 for r in results if r.success)
        lines = [str(r) for r in results]
        title = "Migration Complete" if success_count == len(results) else "Partial Failure"
        messagebox.showinfo(title, "\n\n".join(lines), parent=self._root)

        self._refresh_table_lists()
        self._reset_checklist()

    # ------------------------------------------------------------------
    # Schema display
    # ------------------------------------------------------------------

    def _show_schema_for(self, item: str) -> None:
        self._clear_trees()
        if not self._ctrl.schema:
            return

        m = self._ctrl.store.get(item)
        if isinstance(m, (SplitMapping, MergeMapping)):
            self._tree_old.insert("", tk.END, values=("Schema comparison", "N/A for Split/Merge", "", "", "", ""))
            self._frame_old.config(text="Original Schema (DB)")
            self._frame_new.config(text="Target Schema (File)")
            return

        # Single table
        db_schema = self._ctrl.db.describe_table(item) if self._ctrl.db else {}
        target_name = m.target_schema_name if isinstance(m, SingleMapping) else (
            item if item in self._ctrl.schema else None
        )

        for col, details in db_schema.items():
            row = list(details) + [""] * (6 - len(details))
            if row[4] is None:
                row[4] = "NULL"
            self._tree_old.insert("", tk.END, values=row, iid=f"old_{col}")

        self._frame_old.config(text=f"Original Schema: {item}")
        new_schema = self._ctrl.schema.get(target_name, {}) if target_name else {}
        self._frame_new.config(text=f"Target Schema: {target_name or '(not mapped)'}")

        for col, defn in new_schema.items():
            cd = parse_column_definition(col, defn)
            null_str = "YES" if cd.is_nullable else "NO"
            key_str = "PRI" if cd.is_primary_key else ("UNI" if cd.is_unique else "")
            extra = "auto_increment" if cd.has_auto_increment else ""
            default = str(cd.default_value) if cd.default_value is not None else "NULL"
            self._tree_new.insert("", tk.END, values=(col, cd.base_type, null_str, key_str, default, extra), iid=f"new_{col}")

        if isinstance(m, SingleMapping):
            self._apply_diff_highlights(db_schema, new_schema, m.column_mappings)

    def _apply_diff_highlights(
        self,
        db_schema: dict,
        new_schema: dict[str, str],
        col_maps: dict[str, str],
    ) -> None:
        from core.type_converter import get_base_type
        reverse = {v: k for k, v in col_maps.items()}
        all_old = set(db_schema.keys())
        processed_old: set[str] = set()

        for new_col, new_def in new_schema.items():
            old_col = reverse.get(new_col, new_col)
            if old_col not in db_schema:
                self._tag_item(self._tree_new, f"new_{new_col}", "added")
                continue

            processed_old.add(old_col)
            old_details = db_schema[old_col]
            old_type_base = get_base_type(str(old_details[1]))
            new_type_base = get_base_type(new_def.split()[0])
            is_different = old_type_base != new_type_base

            tag = "renamed" if old_col != new_col else ("changed" if is_different else "matching")
            self._tag_item(self._tree_old, f"old_{old_col}", tag)
            self._tag_item(self._tree_new, f"new_{new_col}", tag)

        for old_col in all_old - processed_old:
            self._tag_item(self._tree_old, f"old_{old_col}", "removed")

    def _tag_item(self, tree: ttk.Treeview, iid: str, tag: str) -> None:
        try:
            tree.item(iid, tags=(tag,))
        except tk.TclError:
            pass

    def _clear_trees(self) -> None:
        for row in self._tree_old.get_children():
            self._tree_old.delete(row)
        for row in self._tree_new.get_children():
            self._tree_new.delete(row)

    # ------------------------------------------------------------------
    # Table list population
    # ------------------------------------------------------------------

    def _refresh_table_lists(self) -> None:
        if not self._ctrl.db or not self._ctrl.db.is_connected:
            return
        self._list_old.delete(0, tk.END)
        self._list_new.delete(0, tk.END)
        self._clear_trees()

        try:
            all_tables = set(self._ctrl.db.list_tables())
        except DatabaseError:
            return

        schema = self._ctrl.schema
        store = self._ctrl.store
        tables_in_merges = store.tables_in_merges()

        # --- Merge entries first ---
        for key, m in sorted(store.all_merges().items()):
            self._list_old.insert(tk.END, m.display_name)
            idx = self._list_old.size() - 1
            self._list_old.itemconfig(idx, {"fg": _COLOUR_MERGE})

        # --- Individual source tables ---
        for table in sorted(all_tables):
            if table.endswith("_new") or table in tables_in_merges:
                continue

            m = store.get(table)
            colour = self._get_table_color(table, m, all_tables, schema)
            self._list_old.insert(tk.END, table)
            idx = self._list_old.size() - 1
            self._list_old.itemconfig(idx, {"fg": colour})

        # --- _new tables ---
        for table in sorted(all_tables):
            if not table.endswith("_new"):
                continue
            base = table[:-4]
            is_orphan = base not in schema and not any(
                (isinstance(m, SingleMapping) and m.target_schema_name == base) or
                (isinstance(m, SplitMapping) and base in m.target_schema_names) or
                (isinstance(m, MergeMapping) and m.target_schema_name == base)
                for m in store.all().values()
            )
            self._list_new.insert(tk.END, table)
            idx = self._list_new.size() - 1
            self._list_new.itemconfig(idx, {"fg": _COLOUR_ORPHAN if is_orphan else "black"})

    def _get_table_color(self, table: str, mapping, all_tables: set, schema: dict) -> str:
        if mapping is None:
            return _COLOUR_UNMAP_IN_SCHEMA if table in schema else _COLOUR_UNMAP_NO_SCHEMA

        if isinstance(mapping, SingleMapping):
            target = mapping.target_schema_name
            if not target or target not in schema:
                return _COLOUR_MAP_NO_SCHEMA
            return _COLOUR_DONE if f"{target}_new" in all_tables else _COLOUR_MAP_READY

        if isinstance(mapping, SplitMapping):
            targets = mapping.target_schema_names
            if not all(t in schema for t in targets):
                return _COLOUR_MAP_NO_SCHEMA
            return _COLOUR_SPLIT_DONE if all(f"{t}_new" in all_tables for t in targets) else _COLOUR_SPLIT_READY

        return "black"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_selected_old(self) -> str | None:
        sel = self._list_old.curselection()
        return self._list_old.get(sel[0]) if sel else None

    def _update_create_btn(self) -> None:
        if self._create_btn is None:
            return
        all_checked = all(v.get() for v in self._constraint_vars)
        self._create_btn.config(state=tk.NORMAL if all_checked else tk.DISABLED)

    def _reset_checklist(self) -> None:
        for v in self._constraint_vars:
            v.set(False)
        self._update_create_btn()

    def _set_status(self, msg: str) -> None:
        self._status_var.set(msg)
        log.debug("Status: %s", msg)

    def _on_close(self) -> None:
        self._ctrl.cleanup()
        self._root.destroy()

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._root.mainloop()
