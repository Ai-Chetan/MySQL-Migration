import tkinter as tk
import csv
from tkinter import ttk, messagebox, filedialog, Toplevel, Scrollbar, Text, END, Listbox, Frame, Label, Entry, Button, StringVar, BooleanVar
import mysql.connector
import re
import json
import os
import datetime
import decimal
import textwrap 

MAPPING_FILE = "table_mappings.json" #

# --- Mapping File Handling ---

def load_mappings():
    """Loads table and column mappings from the JSON file.""" #
    if os.path.exists(MAPPING_FILE): #
        with open(MAPPING_FILE, "r", encoding='utf-8') as f: #
            try:
                mappings = json.load(f) #
                # --- Compatibility check and upgrade old formats ---
                upgraded_mappings = {}
                for key, value in mappings.items():
                    # Key represents old table(s) or a special merge key
                    # Value represents new table(s) or mapping details

                    # Handle potential old simple string mapping (single old -> single new)
                    if isinstance(value, str) and not key.startswith("merge_"):
                        upgraded_mappings[key] = {
                            "type": "single",
                            "new_table_name_schema": value,
                            "column_mappings": {}
                        }
                    elif isinstance(value, dict):
                        # Assume it's a new format dict if it has 'type'
                        if "type" in value and value["type"] in ["single", "split", "merge"]:
                             # Ensure necessary keys exist for each type
                            if value["type"] == "single":
                                if "new_table_name_schema" not in value: value["new_table_name_schema"] = key
                                if "column_mappings" not in value: value["column_mappings"] = {}
                            elif value["type"] == "split":
                                if "new_tables" not in value: value["new_tables"] = [] # List of {"schema_name": "...", "column_mappings": {}}
                                else: # Ensure column mappings exist within each new table entry
                                    for nt in value.get("new_tables", []):
                                        if "column_mappings" not in nt: nt["column_mappings"] = {}
                            elif value["type"] == "merge":
                                if "source_tables" not in value: value["source_tables"] = [] # List of old table names
                                if "new_table_name_schema" not in value: value["new_table_name_schema"] = "" # Target new table
                                if "join_conditions" not in value: value["join_conditions"] = "" # User-defined JOIN clause string
                                if "column_mappings" not in value: value["column_mappings"] = {} # Maps source cols (table.col) to new col

                            upgraded_mappings[key] = value
                        # Handle old dict format (single old -> single new, before 'type')
                        elif "new_table_name_schema" in value or "column_mappings" in value:
                             upgraded_mappings[key] = {
                                "type": "single",
                                "new_table_name_schema": value.get("new_table_name_schema", key),
                                "column_mappings": value.get("column_mappings", {})
                             }
                        else:
                             print(f"Warning: Skipping unrecognized mapping format for key '{key}' in {MAPPING_FILE}.")
                    else:
                         print(f"Warning: Skipping unrecognized mapping format for key '{key}' in {MAPPING_FILE}.")

                return upgraded_mappings
            except json.JSONDecodeError: #
                messagebox.showerror("Mapping Error", f"Could not decode {MAPPING_FILE}. Starting with empty mappings.") #
                return {}
            except Exception as e: #
                 messagebox.showerror("Mapping Error", f"Error loading mappings from {MAPPING_FILE}: {e}") #
                 return {}
    else:
        # Create file if it doesn't exist
        try:
            with open(MAPPING_FILE, "w", encoding='utf-8') as f: #
               json.dump({}, f) #
            return {}
        except IOError as e: #
             messagebox.showerror("File Error", f"Could not create mapping file {MAPPING_FILE}: {e}") #
             return {}


def save_mappings(mappings):
    """Saves the current table and column mappings to the JSON file.""" #
    try:
        with open(MAPPING_FILE, "w", encoding='utf-8') as f: #
            json.dump(mappings, f, indent=4) #
    except IOError as e: #
         messagebox.showerror("File Error", f"Could not save mapping file {MAPPING_FILE}: {e}") #
    except TypeError as e: #
         # Add more detail to the error message for complex structures
         try:
             # Attempt to serialize again with a custom handler for debugging
             def handler(obj):
                 if isinstance(obj, set): return list(obj)
                 return f"Unserializable type: {type(obj).__name__}"
             json.dumps(mappings, indent=4, default=handler)
             # If the above works, the original error might be subtle
             messagebox.showerror("Save Error", f"Data structure cannot be saved as JSON: {e}\nCheck for complex objects or circular references.\nMappings: {mappings}")
         except Exception as dump_e:
              messagebox.showerror("Save Error", f"Data structure cannot be saved as JSON: {e}\nAdditional serialization error: {dump_e}\nMappings might be corrupted: {mappings}")


# --- GUI Utilities ---

def center_window(window, width=600, height=400):
    """Centers a Tkinter window on the screen.""" #
    window.update_idletasks() #
    screen_width = window.winfo_screenwidth() #
    screen_height = window.winfo_screenheight() #
    x = max(0, (screen_width - width) // 2) #
    y = max(0, (screen_height - height) // 2) #
    window.geometry(f"{width}x{height}+{x}+{y}") #

# --- Database Connection ---

def connect_db():
    """Connects to the MySQL database using credentials from the login window.""" #
    global conn, cursor
    username = username_entry.get() #
    password = password_entry.get() #
    if not username: #
         messagebox.showerror("Login Error", "Please enter a username.", parent=login_window) #
         return
    try: #
        conn = mysql.connector.connect( #
            host="localhost", # Consider making host configurable
            user=username, #
            password=password, #
            charset="utf8mb4", #
            get_warnings=True #
        )
        cursor = conn.cursor() #
        login_window.destroy() #
        init_main_window() #
    except mysql.connector.Error as err: #
        messagebox.showerror("Connection Error", f"Failed to connect: {err}", parent=login_window) #

# --- Main Window Logic ---

def populate_db_combobox():
    """Populates the database selection combobox, filtering system DBs.""" #
    if not conn or not conn.is_connected(): #
         return
    try: #
        global cursor, db_combobox #
        cursor.execute("SHOW DATABASES") #
        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys'} #
        databases = [db[0] for db in cursor.fetchall() if db[0] not in system_dbs] #
        db_combobox['values'] = databases #
        if databases: #
            db_combobox.current(0) #
        else:
             db_combobox['values'] = [] #
             db_combobox.set('') #
    except mysql.connector.Error as err: #
         messagebox.showerror("Error", f"Failed to fetch databases: {err}") #


def select_database():
    """Selects the database, loads schema file, performs auto-mapping, and populates table lists.""" #
    global conn, cursor, schema_file_path, table_mappings, current_db_name
    dbname = db_combobox.get() #
    schema_file_path_val = schema_file_entry.get() #

    if not dbname: #
         messagebox.showerror("Error", "Please select a database from the list.") #
         return
    if not schema_file_path_val: #
        messagebox.showerror("Error", "Please provide the path to the schema definition file.") #
        return #
    if not os.path.isfile(schema_file_path_val): #
         messagebox.showerror("Error", f"Schema file not found or is not a file:\n{schema_file_path_val}") #
         return

    schema_file_path = schema_file_path_val #

    try: #
        if not conn or not conn.is_connected(): #
             messagebox.showerror("Connection Error", "Database connection lost. Please reconnect.") #
             return #

        cursor.execute(f"USE `{dbname}`") #
        conn.commit() #
        current_db_name = dbname #
        db_label.config(text=f"Status: Connected to '{dbname}'. Schema: '{os.path.basename(schema_file_path)}'") #
        tables_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0,10)) #

        table_mappings = load_mappings() #
        parsed_schema = parse_schema_file(schema_file_path) #
        if not parsed_schema and os.path.exists(schema_file_path): # Only warn if file exists but is empty/invalid
            messagebox.showwarning("Schema Warning", f"Schema file '{os.path.basename(schema_file_path)}' was parsed, but no table definitions were found.") #

        # Auto-mapping is primarily for single table mappings
        auto_map_tables_and_columns(parsed_schema) #
        get_tables() #
        if 'constraint_vars' in globals() and constraint_vars: #
            for var in constraint_vars: #
                var.set(False) #
        check_constraints() #

    except mysql.connector.Error as err: #
        messagebox.showerror("Database Error", f"Failed to select database '{dbname}': {err}") #
        db_label.config(text="Status: Connection Error. Please try again.") #
        current_db_name = None #
        tables_frame.pack_forget() #
    except Exception as e: #
        messagebox.showerror("Error", f"An unexpected error occurred: {e}") #
        db_label.config(text="Status: An error occurred.") #
        current_db_name = None #
        tables_frame.pack_forget() #


def get_tables():
    """Fetches tables from the DB and populates the 'Old' and 'New' listboxes with appropriate colors.""" #
    global cursor, conn, tables_listbox_old, tables_listbox_new, schema_file_path, table_mappings, current_db_name

    tables_listbox_old.delete(0, tk.END) #
    tables_listbox_new.delete(0, tk.END) #
    for row in schema_tree_old.get_children(): schema_tree_old.delete(row) #
    for row in schema_tree_new.get_children(): schema_tree_new.delete(row) #

    if not conn or not conn.is_connected() or not current_db_name: #
        return

    try: #
         cursor.execute("SHOW TABLES") #
         all_tables_in_db = {table[0] for table in cursor.fetchall()} # Use set for faster lookups #
    except mysql.connector.Error as err: #
         messagebox.showerror("Error", f"Failed to fetch tables for database '{current_db_name}': {err}") #
         return #

    parsed_schema = parse_schema_file(schema_file_path) #

    # Populate Old Tables List
    processed_in_merge = set()
    for merge_key, merge_data in table_mappings.items():
         if isinstance(merge_data, dict) and merge_data.get("type") == "merge":
              processed_in_merge.update(merge_data.get("source_tables", []))

    # Add merged table representations to the old list
    for merge_key, merge_data in sorted(table_mappings.items()):
        if isinstance(merge_data, dict) and merge_data.get("type") == "merge":
             source_tables = merge_data.get("source_tables", [])
             target_schema_name = merge_data.get("new_table_name_schema")
             display_name = f"MERGE: {', '.join(source_tables)} -> {target_schema_name}"
             index = tables_listbox_old.size()
             tables_listbox_old.insert(tk.END, display_name)
             tables_listbox_old.itemconfig(index, {'fg': 'darkgreen', 'selectbackground': 'lightgreen'}) # Special color for merge

    # Add individual old tables
    for table_db in sorted(list(all_tables_in_db)): #
        if table_db.endswith("_new") or table_db in processed_in_merge: #
             continue

        index = tables_listbox_old.size() #
        tables_listbox_old.insert(tk.END, table_db) #

        mapping_info = table_mappings.get(table_db) #
        new_table_schema_names = [] # Can be multiple for split
        is_mapped = False
        mapping_type = None

        if isinstance(mapping_info, dict): #
            mapping_type = mapping_info.get("type")
            if mapping_type == "single":
                new_table_schema_names.append(mapping_info.get("new_table_name_schema"))
                is_mapped = True #
            elif mapping_type == "split":
                 new_table_schema_names = [nt.get("schema_name") for nt in mapping_info.get("new_tables", [])]
                 is_mapped = True
            # Merges are handled separately above

        # Determine color based on mapping and schema presence
        color = 'black' # Default/Processed #
        if not is_mapped: #
            color = 'orange' if table_db in parsed_schema else 'red' #
        elif mapping_type == "single":
            target_schema_name = new_table_schema_names[0]
            if not target_schema_name or target_schema_name not in parsed_schema: #
                color = 'purple' #
            else:
                new_db_table_exists = f"{target_schema_name}_new" in all_tables_in_db #
                color = 'black' if new_db_table_exists else 'blue' #
        elif mapping_type == "split":
            all_targets_in_schema = all(name in parsed_schema for name in new_table_schema_names if name)
            all_new_exist = all(f"{name}_new" in all_tables_in_db for name in new_table_schema_names if name)
            if not all_targets_in_schema:
                 color = 'purple' # If any target is missing from schema
            elif all_new_exist:
                 color = 'black' # All targets exist
            else:
                 color = 'darkcyan' # Ready for split creation (custom color)

        tables_listbox_old.itemconfig(index, {'fg': color}) #

    # Populate New Tables List
    for table_db in sorted(list(all_tables_in_db)): #
        if table_db.endswith("_new"): #
            tables_listbox_new.insert(tk.END, table_db) #
            base_name = table_db[:-4] #
            is_base_in_schema = base_name in parsed_schema #

            # Check if this base_name is a target in any mapping (single, split, merge)
            is_target_mapped = False
            for m_data in table_mappings.values():
                if isinstance(m_data, dict):
                    if m_data.get("type") == "single" and m_data.get("new_table_name_schema") == base_name:
                        is_target_mapped = True; break
                    elif m_data.get("type") == "split":
                        if any(nt.get("schema_name") == base_name for nt in m_data.get("new_tables", [])):
                             is_target_mapped = True; break
                    elif m_data.get("type") == "merge" and m_data.get("new_table_name_schema") == base_name:
                        is_target_mapped = True; break

            new_color = 'black' #
            if not is_base_in_schema and not is_target_mapped: #
                 new_color = 'grey' #

            new_index = tables_listbox_new.size() - 1 #
            tables_listbox_new.itemconfig(new_index, {'fg': new_color}) #

    root.update_idletasks() #


# --- Constraint Check Logic (Manual Gate) ---
def check_constraints(*args):
    """Enables/disables the Create button based on all checkboxes being checked.""" #
    all_checked = False #
    if 'constraint_vars' in globals() and constraint_vars and 'create_button' in globals() and create_button: #
        all_checked = all(var.get() for var in constraint_vars) #
        create_button.config(state=tk.NORMAL if all_checked else tk.DISABLED) #
    return all_checked #


# --- Help Window ---
def show_help():
    """Displays a help window explaining the tool's usage.""" #
    help_win = Toplevel(root) #
    help_win.title("Help - Schema Migration Tool") #
    center_window(help_win, 750, 650) # Increased height for new sections #
    help_win.transient(root) #

    help_text_widget = Text(help_win, wrap=tk.WORD, padx=10, pady=10, bd=0, font=("Arial", 10), relief=tk.FLAT) #
    help_scroll = ttk.Scrollbar(help_win, command=help_text_widget.yview) #
    help_text_widget.config(yscrollcommand=help_scroll.set) #

    help_scroll.pack(side=tk.RIGHT, fill=tk.Y) #
    help_text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True) #

    # Define Help Text Tags (keep your existing tag definitions)
    tags = {
        "h1": {"font": ("Arial", 14, "bold"), "spacing3": 5},
        "h2": {"font": ("Arial", 11, "bold"), "spacing1": 10, "spacing3": 3},
        "bold": {"font": ("Arial", 10, "bold")},
        "italic": {"font": ("Arial", 10, "italic")},
        "code": {"font": ("Courier New", 9), "background": "#f0f0f0", "lmargin1": 10, "lmargin2": 10},
        "warning": {"foreground": "red", "font": ("Arial", 10, "bold")},
        "color_red": {"foreground": "red", "font": ("Arial", 10, "bold")},
        "color_orange": {"foreground": "orange", "font": ("Arial", 10, "bold")},
        "color_purple": {"foreground": "purple", "font": ("Arial", 10, "bold")},
        "color_blue": {"foreground": "blue", "font": ("Arial", 10, "bold")},
        "color_black": {"foreground": "black", "font": ("Arial", 10, "bold")},
        "color_grey": {"foreground": "grey", "font": ("Arial", 10, "bold")},
        "color_darkcyan": {"foreground": "darkcyan", "font": ("Arial", 10, "bold")},
        "color_darkgreen": {"foreground": "darkgreen", "font": ("Arial", 10, "bold")},
        "list_item": {"lmargin1": 10, "lmargin2": 25}
    }
    for name, config in tags.items():
        help_text_widget.tag_configure(name, **config)

    # --- Help Content ---
    help_content = [
        ("Schema Migration Tool - Help\n", "h1"),
        ("Purpose:\n", "h2"),
        ("This tool assists in migrating MySQL database schemas. It allows comparing existing tables against a new schema file, defining mappings (single, split, merge), creating new tables, copying data automatically, and generating manual script templates for complex cases.\n\n", ""),

        ("Workflow:\n", "h2"),
        ("1.  Login:\n", ("bold", "list_item")),
        ("    Connect to your MySQL database.\n", "list_item"),
        ("2.  Load DB & Schema:\n", ("bold", "list_item")),
        ("    Select the target database, browse to your schema file, and click 'Load DB & Schema'.\n", "list_item"),
        ("3.  Review & Map Tables/Operations:\n", ("bold", "list_item")),
        ("    Examine the 'Old/Source' list (see 'List Colors').\n    Use 'Map Table (Single)', 'Split Table...', or 'Merge Tables...' to define relationships between old DB tables and new schema definitions. Split/Merge operations create special entries in the list.\n", "list_item"),
        ("4.  Map Columns (Optional but Recommended):\n", ("bold", "list_item")),
        ("    Select a mapped single table or a split mapping entry.\n    Use 'Map Columns...' to explicitly link columns if names differ or if you need specific mappings for split targets.\n", "list_item"),
        ("5.  Compare Schemas (Single Table Mappings Only):\n", ("bold", "list_item")),
        ("    When a single-mapped table is selected, the middle section shows schema comparisons (see 'Schema Highlighting'). This is disabled for Split/Merge entries.\n", "list_item"),
        ("6.  Execute Automatic Migration:\n", ("bold", "list_item")),
        ("    Select the desired table or mapping entry (Single, Split, or Merge) in the 'Old/Source' list.\n    Check the 'Manual Confirmation' boxes (safety gate).\n    Click 'CREATE New Table(s) & Copy Data'.\n    The tool performs data type checks, creates new table(s) (e.g., 'tablename_new'), and copies/merges data based on the mapping.\n", "list_item"),
        ("7.  Generate Manual Script (Alternative):\n", ("bold", "list_item")),
        ("    For complex cases (e.g., major data type changes, complex transformations) where automatic migration might fail or is insufficient:\n    Select the source table (single or split mapping) in the 'Old/Source' list.\n    Click 'Generate Manual Script...'.\n    A Python script template (.py file) will be created in the application's directory (see 'Manual Script Generation').\n", "list_item"),
        ("8.  View Data:\n", ("bold", "list_item")),
        ("    Use 'View Old/New Table Data' buttons to inspect results. Download options (CSV/JSON) are available.\n\n", "list_item"),

        ("Schema File Format:\n", "h2"),
        ("Plain text file defining tables like:\n", ""),
        ("Table: MyTable\n", "code"),
        ("  id INT AUTO_INCREMENT PRIMARY KEY\n", "code"),
        ("  name VARCHAR(100) NOT NULL\n", "code"),
        ("  # ... more columns ...\n\n", "code"),
        ("Comments start with # or --. Use standard SQL column definitions.\n\n", ""),

        ("List Colors (Old/Source Tables):\n", "h2"),
        ("  Red:\n", ("color_red", "list_item")), ("    Not mapped & not in schema file.\n", "list_item"),
        ("  Orange:\n", ("color_orange", "list_item")), ("    In schema file, but not explicitly mapped.\n", "list_item"),
        ("  Purple:\n", ("color_purple", "list_item")), ("    Mapped, but target schema name(s) not found in file.\n", "list_item"),
        ("  Blue:\n", ("color_blue", "list_item")), ("    Mapped (single), schema target exists, '_new' table not created.\n", "list_item"),
        ("  Dark Cyan:\n", ("color_darkcyan", "list_item")), (" Mapped (split), schema target(s) exist, '_new' tables not all created.\n", "list_item"),
        ("  Dark Green:\n", ("color_darkgreen", "list_item")), ("    Represents a Merge mapping. Select to execute.\n", "list_item"),
        ("  Black:\n", ("color_black", "list_item")), ("    Mapped (single/split), schema target(s) exist, AND corresponding '_new' table(s) exist.\n\n", "list_item"),

        ("List Colors (_new/Generated Tables):\n", "h2"),
        ("  Black:\n", ("color_black", "list_item")), ("    Standard color.\n", "list_item"),
        ("  Grey:\n", ("color_grey", "list_item")), ("    Base name (no '_new') not in schema or mappings. Orphaned?\n\n", "list_item"),

        ("Schema Highlighting (Middle Section - Single Table Mappings Only):\n", "h2"),
        ("  Matching (Grey):\n", ("bold", "list_item")), ("    Column exists in both with same definition.\n", "list_item"),
        ("  Changed (Yellow):\n", ("bold", "list_item")), ("    Definition attributes changed.\n", "list_item"),
        ("  Renamed (Light Blue):\n", ("bold", "list_item")), ("    Mapped with different names.\n", "list_item"),
        ("  Added (Green - New Schema):\n", ("bold", "list_item")), ("    Only in new schema.\n", "list_item"),
        ("  Removed (Pink - Old Schema):\n", ("bold", "list_item")), ("    Only in old schema.\n\n", "list_item"),

        ("Split & Merge Operations (Detailed):\n", "h2"),
        ("  Split Table:\n", ("bold", "list_item")),
        ("    Maps one old source table to multiple new target tables defined in the schema.\n", "list_item"),
        ("    How it works: When executed, the tool iterates through each defined target table. For each target, it creates the `target_name_new` table and runs a separate `INSERT INTO target_name_new (...) SELECT ... FROM old_source_table` query (batched). Data from the *entire* old source table is copied into *each* new target table, but only using the columns specified in the mapping for that specific target.\n", "list_item"),
        ("  Merge Tables:\n", ("bold", "list_item")),
        ("    Maps multiple old source tables into a single new target table.\n", "list_item"),
        ("    Requires defining JOIN conditions (SQL snippet, e.g., `INNER JOIN t2 ON t1.id=t2.t1_id`) and explicit column mappings (`source_table.column -> target_column`). Auto-generation provides a starting point that MUST be verified.\n", "list_item"),
        ("    How it works: When executed, the tool creates the single `target_name_new` table. It then runs *one* combined `INSERT INTO target_name_new (...) SELECT ... FROM source1 JOIN source2 ON ... JOIN source3 ON ...` query (batched). This query uses your defined JOIN conditions to link the source tables and your column mappings to select the correct data into the new table.\n\n", "list_item"),

        ("Data Type Conversion (Automatic Migration):\n", "h2"),
        ("- Checked during 'CREATE' for mapped columns.\n", "list_item"),
        ("- Unsafe: Likely to fail (e.g., text to int). Prevents creation.\n", ("italic", "list_item")),
        ("- Lossy: May truncate/lose precision (e.g., float to int). Requires confirmation.\n", ("italic", "list_item")),
        ("- Safe: Generally safe (e.g., int to bigint). Uses `CAST`.\n\n", ("italic", "list_item")),

        ("Manual Script Generation:\n", "h2"),
        ("  Purpose:\n", ("bold", "list_item")),
        ("    For migrations too complex for the automatic tool (e.g., significant data transformations, incompatible type changes requiring custom logic).\n", "list_item"),
        ("  Usage:\n", ("bold", "list_item")),
        ("    Select the source table (single or split mapping, NOT merge) in the 'Old/Source' list.\n    Click 'Generate Manual Script...'.\n", "list_item"),
        ("  Output:\n", ("bold", "list_item")),
        ("    A Python (.py) file is created in the tool's directory (e.g., `manual_migration_oldtable_to_newtable.py`).\n", "list_item"),
        ("  Content:\n", ("bold", "list_item")),
        ("    The script contains:\n    - Connection details (edit required).\n    - Old/New schema and sample data (as comments).\n    - A function (`migrate_data`) with runnable Python/SQL code:\n      * Generated `CREATE TABLE IF NOT EXISTS` statement.\n      * Basic `SELECT * FROM old_table`.\n      * Loop structure for processing rows.\n      * Generated `INSERT` statement structure.\n      * Placeholders and `TODO` comments where you MUST add custom data transformation logic and type handling.\n      * Error handling and commit/rollback.\n", "list_item"),
        ("  Action Required:\n", ("bold", "list_item")),
        ("    You MUST open the generated `.py` file, review the generated code, fill in the `TODO` sections with your custom logic, update connection details, and then run the script independently from the command line (`python your_script_name.py`).\n\n", "list_item"),


        ("Manual Confirmation Checkboxes:\n", "h2"),
        ("Act purely as a manual safety checklist. ", "bold"), ("Must check all to enable the 'CREATE' button for automatic migration.\n\n", ""),

        ("IMPORTANT WARNING:\n", "warning"),
        ("This tool modifies your database (creates tables, inserts data) and generates scripts that can modify data. ", "warning"), ("ALWAYS BACK UP YOUR DATABASE", "warning"), (" before use. Verify all actions and scripts.\n", "warning")
    ]
    # --- End Help Content ---


    # Insert Content into Text Widget (keep your existing loop)
    for text, style_tags in help_content:
        help_text_widget.insert(END, text, style_tags)

    help_text_widget.config(state=tk.DISABLED) # Make text read-only

# --- Schema/DB Interaction Functions ---

def get_db_schema(table_name):
    """Fetches the schema of a table directly from the database using DESCRIBE.""" #
    if not conn or not conn.is_connected(): return None #
    try: #
        # Use qualified table name if needed, assume current DB for now
        cursor.execute(f"DESCRIBE `{table_name}`") #
        # Return as dict {col_name: (full_row_tuple)}
        schema = {col[0]: col for col in cursor.fetchall()} #
        return schema #
    except mysql.connector.Error as err: #
        # Avoid showing messagebox if it's just part of an internal check
        print(f"DB Schema Error: Could not fetch schema for DB table '{table_name}': {err}")
        return None #


def get_schema_file_definition(old_table_name_db_or_merge_key):
    """Gets the target schema definition(s) from the parsed file using mappings.""" #
    parsed_schema = parse_schema_file(schema_file_path) #
    mapping_info = table_mappings.get(old_table_name_db_or_merge_key) #

    if isinstance(mapping_info, dict): #
        map_type = mapping_info.get("type")
        if map_type == "single":
             target_schema_name = mapping_info.get("new_table_name_schema") #
             if target_schema_name and target_schema_name in parsed_schema: #
                 return {target_schema_name: parsed_schema[target_schema_name]} # Return dict for consistency
             else: return None #
        elif map_type == "split":
             target_defs = {}
             for nt in mapping_info.get("new_tables", []):
                  schema_name = nt.get("schema_name")
                  if schema_name and schema_name in parsed_schema:
                       target_defs[schema_name] = parsed_schema[schema_name]
             return target_defs if target_defs else None
        elif map_type == "merge":
             target_schema_name = mapping_info.get("new_table_name_schema")
             if target_schema_name and target_schema_name in parsed_schema:
                  return {target_schema_name: parsed_schema[target_schema_name]} # Return dict for consistency
             else: return None
    elif old_table_name_db_or_merge_key in parsed_schema: # Fallback for unmapped single table
        # Check if it's a simple table name (not a merge key)
        if not old_table_name_db_or_merge_key.startswith("MERGE:"):
             target_schema_name = old_table_name_db_or_merge_key #
             return {target_schema_name: parsed_schema[target_schema_name]} #

    return None # Indicate schema not found for this mapping/table #


def parse_schema_file(file_path):
    """Parses the schema definition text file (handles comments, simple table/col defs).""" #
    schema_dict = {} #
    current_table = None #
    if not file_path or not os.path.exists(file_path): #
         return {} #
    try: #
        with open(file_path, "r", encoding='utf-8') as file: #
            for line_num, line in enumerate(file, 1): #
                line = line.strip() #
                if not line or line.startswith("--") or line.startswith("#"): # Skip empty/comment lines
                    continue #

                table_match = re.match(r"Table:\s*(\w+)", line, re.IGNORECASE) #
                if table_match: #
                    current_table = table_match.group(1) #
                    schema_dict[current_table] = {} # Overwrite/use last definition if duplicate #
                    continue #

                if current_table: #
                    # Handles optional quotes/backticks around column name
                    col_match = re.match(r"[`']?([\w_]+)[`']?\s+(.+)", line) #
                    if col_match: #
                        column_name = col_match.group(1) #
                        attributes = col_match.group(2).strip() #
                        schema_dict[current_table][column_name] = attributes # Overwrite/use last definition #
    except Exception as e: #
        messagebox.showerror("Error", f"Error parsing schema file '{file_path}': {e}") #
        return {} #

    return schema_dict #


def show_schema(event=None):
    """Displays the old (DB) and new (file) schema for the selected table, applying highlighting. (Only fully functional for single table mappings).""" #
    selected_indices = tables_listbox_old.curselection() #
    # Clear trees first
    for row in schema_tree_old.get_children(): schema_tree_old.delete(row) #
    for row in schema_tree_new.get_children(): schema_tree_new.delete(row) #

    if not selected_indices: #
        return # Nothing selected, trees cleared #
    selected_item_text = tables_listbox_old.get(selected_indices) #

    # Handle Merge/Split selections (limited display)
    if selected_item_text.startswith("MERGE:") or (selected_item_text in table_mappings and table_mappings[selected_item_text].get("type") == "split"):
         schema_tree_old.insert("", tk.END, values=("(Schema comparison not available", "for Merge/Split mappings)", "", "", "", ""))
         schema_tree_new.insert("", tk.END, values=("(Select mapping buttons", "to view/edit details)", "", "", "", ""))
         return

    # Assume single table selection if not Merge/Split
    selected_table_db = selected_item_text #

    # Get Old Schema (from DB)
    db_schema_raw = get_db_schema(selected_table_db) #
    if db_schema_raw is None: return #

    db_schema_processed = {} # Store data formatted for treeview and comparison #
    for col_name, details in db_schema_raw.items(): #
        padded_details = list(details) + [''] * (6 - len(details)) #
        if padded_details[4] is None: padded_details[4] = 'NULL' # Display None default as 'NULL' #
        schema_tree_old.insert("", tk.END, values=padded_details, iid=f"old_{col_name}") #
        db_schema_processed[col_name] = padded_details #

    # Get New Schema (from File via Mappings - assuming single target for display)
    new_schema_defs = get_schema_file_definition(selected_table_db) # Needs adjustment for split? #
    new_schema_processed = {} #

    # Display the first target schema if available (for single/split)
    target_schema_name_for_display = None
    new_schema_file_def = None
    if new_schema_defs:
         # For single mapping, there's only one key
         target_schema_name_for_display = list(new_schema_defs.keys())[0]
         new_schema_file_def = new_schema_defs[target_schema_name_for_display]

    if new_schema_file_def: #
        schema_frame_new.config(text=f"New Schema (from File: {target_schema_name_for_display})") # Update label
        for new_col_name, definition in new_schema_file_def.items(): #
            # Parse definition string into Treeview columns
            parts = definition.split() #
            field_type_full = parts[0] if len(parts) > 0 else "" #
            null_value = "NO" if "NOT NULL" in definition.upper() else "YES" #
            key_value = "" #
            default_value_parsed = None #
            extra = "" #
            def_upper = definition.upper() # For case-insensitive checks #

            # Basic Key parsing
            if "PRIMARY KEY" in def_upper: key_value = "PRI" #
            elif "UNIQUE" in def_upper: key_value = "UNI" #
            if "AUTO_INCREMENT" in def_upper: extra = "auto_increment" #

            # Default value parsing
            default_match = re.search(r"DEFAULT\s+((?:'(?:[^']|\\')*'|\"(?:[^\"]|\\\")*\"|[\w.-]+)|NULL)", definition, re.IGNORECASE) #
            if default_match: #
               default_raw = default_match.group(1) #
               default_value_parsed = 'NULL' if default_raw.upper() == 'NULL' else default_raw.strip("'\"") #
            else: #
                default_value_parsed = 'NULL' # Assume NULL if no default specified (might vary by SQL mode/type) #

            # Prepare row data for display
            row_data = (new_col_name, field_type_full, null_value, key_value, default_value_parsed, extra) #
            schema_tree_new.insert("", tk.END, values=row_data, iid=f"new_{new_col_name}") #
            new_schema_processed[new_col_name] = row_data #
    else: #
        schema_frame_new.config(text="New Schema (from File)")
        schema_tree_new.insert("", tk.END, values=("(No definition found in schema file)", "", "", "", "", "")) #

    highlight_differences(db_schema_processed, new_schema_processed) #


def highlight_differences(db_schema_processed, new_schema_processed):
    """Highlights differences in the schema treeviews based on processed data and mappings. (Assumes single table mapping context).""" #
    # Clear existing tags first
    for item in schema_tree_old.get_children(): schema_tree_old.item(item, tags=()) #
    for item in schema_tree_new.get_children(): schema_tree_new.item(item, tags=()) #

    selected_indices = tables_listbox_old.curselection() #
    if not selected_indices: return #
    selected_table_db = tables_listbox_old.get(selected_indices) #

    # Check if it's a merge/split mapping, if so, exit highlighting
    if selected_table_db.startswith("MERGE:") or (selected_table_db in table_mappings and table_mappings[selected_table_db].get("type") in ["merge", "split"]):
         return

    # Proceed with highlighting logic for single table mappings
    mapping_info = table_mappings.get(selected_table_db, {})
    column_mappings = {}
    if isinstance(mapping_info, dict) and mapping_info.get("type") == "single":
         column_mappings = mapping_info.get("column_mappings", {}) #

    reverse_column_mappings = {v: k for k, v in column_mappings.items()} #

    all_old_cols = set(db_schema_processed.keys()) #
    processed_old_cols = set() #

    # Compare NEW schema against OLD
    for new_col_name, new_data in new_schema_processed.items(): #
        old_col_name = reverse_column_mappings.get(new_col_name, new_col_name) #

        if old_col_name in db_schema_processed: #
            old_data = db_schema_processed[old_col_name] #
            processed_old_cols.add(old_col_name) #

            try: #
                # Compare: Type(basic), Null, Key, Default(str), Extra
                old_type_basic = get_base_type(old_data[1]) #
                new_type_basic = get_base_type(new_data[1]) #
                old_default_str = str(old_data[4]) # Already 'NULL' string if was None #
                new_default_str = str(new_data[4]) #

                is_different = (old_type_basic != new_type_basic or #
                               old_data[2] != new_data[2] or # Null
                               old_data[3] != new_data[3] or # Key
                               old_default_str != new_default_str or # Default
                               old_data[5] != new_data[5])   # Extra

                tag = "renamed" if old_col_name != new_col_name else ("changed" if is_different else "matching") #

                schema_tree_old.item(f"old_{old_col_name}", tags=(tag,)) #
                schema_tree_new.item(f"new_{new_col_name}", tags=(tag,)) #
            except IndexError: #
                  print(f"Warning: Index error comparing schemas for {old_col_name}/{new_col_name}.") #
                  # Tag as changed on error
                  if f"old_{old_col_name}" in schema_tree_old.get_children(""): #
                       schema_tree_old.item(f"old_{old_col_name}", tags=("changed",)) #
                  if f"new_{new_col_name}" in schema_tree_new.get_children(""): #
                       schema_tree_new.item(f"new_{new_col_name}", tags=("changed",)) #
        else: #
            # Column is added in the new schema
            schema_tree_new.item(f"new_{new_col_name}", tags=("added",)) #

    # Identify removed columns (old cols not processed)
    for old_col_name in all_old_cols - processed_old_cols: #
        schema_tree_old.item(f"old_{old_col_name}", tags=("removed",)) #

    # Configure tag colors
    tag_colors = { #
         "matching": "#E0E0E0", "changed": "#FFFACD", "renamed": "#ADD8E6", #
        "removed": "#FFB6C1", "added": "#90EE90" #
    } #
    for tree in [schema_tree_old, schema_tree_new]: #
        for tag, color in tag_colors.items(): #
            tree.tag_configure(tag, background=color) #

# --- Data Type Conversion Logic ---

def get_base_type(dtype_string):
    """Extracts the base SQL type (e.g., 'varchar' from 'varchar(255)').""" #
    if not dtype_string: return "" #
    return dtype_string.split('(')[0].split()[0].lower() #


def is_conversion_safe(old_type_str, new_type_str):
    """Heuristically determines if a data type conversion is generally 'safe', 'unsafe', or 'lossy'.""" #
    old_base = get_base_type(old_type_str) #
    new_base = get_base_type(new_type_str) #
    if old_base == new_base: return 'safe' #

    # Type categories
    integer_types = {'tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint'} #
    approx_numeric_types = {'float', 'double'} #
    exact_numeric_types = {'decimal', 'numeric'} #
    numeric_types = integer_types | approx_numeric_types | exact_numeric_types #
    string_types = {'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set'} #
    datetime_types = {'date', 'datetime', 'timestamp', 'time', 'year'} #
    binary_types = {'binary', 'varbinary', 'tinyblob', 'blob', 'mediumblob', 'longblob', 'bit'} #
    json_type = {'json'} #

    # Determine categories
    old_cat = ('int' if old_base in integer_types else #
               'approx' if old_base in approx_numeric_types else #
               'exact' if old_base in exact_numeric_types else #
               'str' if old_base in string_types else #
               'dt' if old_base in datetime_types else #
               'bin' if old_base in binary_types else #
               'json' if old_base in json_type else 'other') #
    new_cat = ('int' if new_base in integer_types else #
               'approx' if new_base in approx_numeric_types else #
               'exact' if new_base in exact_numeric_types else #
               'str' if new_base in string_types else #
               'dt' if new_base in datetime_types else #
               'bin' if new_base in binary_types else #
               'json' if new_base in json_type else 'other') #

    # --- Conversion Rules ---
    # Anything to String is generally safe or lossy (formatting/encoding)
    if new_cat == 'str': #
        return 'lossy' if old_cat == 'bin' else 'safe' #

    # Numeric conversions
    if old_cat in ('int', 'approx', 'exact') and new_cat in ('int', 'approx', 'exact'): #
        if new_cat == 'int': return 'lossy' if old_cat in ('approx', 'exact') else 'safe' #
        if new_cat == 'approx': return 'lossy' # Precision loss possible #
        if new_cat == 'exact': return 'lossy' if old_cat == 'approx' else 'safe' #

    # Date/Time conversions
    if old_cat == 'dt' and new_cat == 'dt': #
        if old_base == 'date' and new_base in ('datetime', 'timestamp'): return 'safe' #
        return 'lossy' if old_base != new_base else 'safe' #

    # Binary conversions
    if old_cat == 'bin' and new_cat == 'bin': return 'safe' #
    if old_cat == 'str' and new_cat == 'bin': return 'lossy' # Encoding matters #

    # JSON conversions
    if new_cat == 'json': return 'safe' # Most things can cast to JSON string representation #
    if old_cat == 'json' and new_cat == 'str': return 'safe' #

    # --- Default Unsafe ---
    # Covers String/Bin/JSON/DT -> Numeric, Numeric/DT -> Bin, etc.
    return 'unsafe' #


def get_cast_type(full_type_definition):
    """Extracts a type suitable for MySQL CAST function.""" #
    type_upper = full_type_definition.upper() #
    parts = type_upper.split('(')[0].split() #
    base_type = parts[0] #
    is_unsigned = "UNSIGNED" in parts or "UNSIGNED" in type_upper #

    if base_type in ('TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT'): #
        return "UNSIGNED" if is_unsigned else "SIGNED" #
    elif base_type in ('FLOAT', 'DOUBLE', 'REAL'): #
        return "DOUBLE" # Cast to DOUBLE #
    elif base_type in ('DECIMAL', 'NUMERIC', 'FIXED'): #
        match = re.search(r'\((\d+)(?:,(\d+))?\)', full_type_definition) #
        precision = match.group(1) if match else '65' # High precision default #
        scale = match.group(2) if match and match.group(2) else '30' # High scale default #
        return f"DECIMAL({precision},{scale})" #
    elif base_type in ('CHAR', 'VARCHAR', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'ENUM', 'SET'): #
         # Using CHAR is safest generic cast target for strings
         return "CHAR CHARACTER SET utf8mb4" #
    elif base_type == 'DATE': return "DATE" #
    elif base_type == 'DATETIME': return "DATETIME" #
    elif base_type == 'TIMESTAMP': return "DATETIME" # Cast to DATETIME #
    elif base_type == 'TIME': return "TIME" #
    elif base_type == 'YEAR': return "SIGNED" # Cast YEAR to SIGNED #
    elif base_type in ('BINARY', 'VARBINARY', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'BIT'): #
        return "BINARY" # Length might matter #
    elif base_type == 'JSON': return "JSON" #

    print(f"Warning: Could not determine specific CAST type for '{full_type_definition}'. Using CHAR.") #
    return "CHAR CHARACTER SET utf8mb4" #


# --- Table Creation and Data Copy ---

def get_copy_column_pairs(old_table_name, new_schema_table_name, db_schema_dict, new_schema_dict, current_mappings):
    """Determines column pairs for copying data (single table source), returns list of tuples or None if unsafe conversion found.""" #
    pairs = [] #
    # Adjust to get column mappings specific to the target table if split mapping
    column_mappings = {}
    map_info = current_mappings.get(old_table_name)
    if isinstance(map_info, dict):
        if map_info.get("type") == "single":
             column_mappings = map_info.get("column_mappings", {}) #
        elif map_info.get("type") == "split":
             # Find the specific target table's mappings within the split
             for nt in map_info.get("new_tables", []):
                 if nt.get("schema_name") == new_schema_table_name:
                      column_mappings = nt.get("column_mappings", {})
                      break
        # Merge mappings handled separately

    reverse_column_mappings = {v: k for k, v in column_mappings.items()} #

    for new_col_name, new_col_definition in new_schema_dict.items(): #
        old_col_name = reverse_column_mappings.get(new_col_name, new_col_name) #

        if old_col_name in db_schema_dict: #
            old_col_details = db_schema_dict[old_col_name] #
            old_col_type = old_col_details[1] #
            new_col_type = new_col_definition.split()[0] # Basic type extraction #

            safety = is_conversion_safe(old_col_type, new_col_type) #
            requires_cast = False #
            cast_type_str = None #

            if safety == 'unsafe': #
                 messagebox.showerror("Unsafe Conversion Detected", #
                                     f"Cannot prepare data copy due to unsafe type conversion:\n" #
                                     f"Table: '{old_table_name}' -> '{new_schema_table_name}'\n" #
                                     f"Column: `{old_col_name}` ({old_col_type}) -> `{new_col_name}` ({new_col_type})\n" #
                                     "Manual intervention required.") #
                 return None # Signal error #

            if get_base_type(old_col_type) != get_base_type(new_col_type): #
                if safety in ('safe', 'lossy'): #
                    requires_cast = True #
                    cast_type_str = get_cast_type(new_col_type) #

            select_expression = f"`{old_col_name}`" #
            if requires_cast and cast_type_str: #
                 select_expression = f"CAST(`{old_col_name}` AS {cast_type_str})" #

            pairs.append((select_expression, new_col_name, requires_cast, cast_type_str, old_col_type, new_col_type)) #

    return pairs #

def get_merge_copy_details(merge_key, current_mappings, parsed_schema):
     """Gets column pairs and constructs the SELECT part for a merge operation."""
     map_info = current_mappings.get(merge_key)
     if not map_info or map_info.get("type") != "merge":
         return None, None, "Invalid merge mapping info", None

     target_schema_name = map_info.get("new_table_name_schema")
     source_tables = map_info.get("source_tables", [])
     join_conditions = map_info.get("join_conditions", "")
     column_mappings = map_info.get("column_mappings", {}) # maps source `table.column` -> target_column
     new_schema_dict = parsed_schema.get(target_schema_name)

     if not target_schema_name or not source_tables or not new_schema_dict:
         return None, None, "Missing target schema, source tables, or schema definition in mapping", None

     all_source_schemas = {}
     for src_table in source_tables:
         schema = get_db_schema(src_table)
         if not schema:
             return None, None, f"Could not fetch schema for source table '{src_table}'", None
         all_source_schemas[src_table] = schema

     select_pairs = [] # List of (select_expression, new_col_name, old_col_type, new_col_type)
     lossy_conversions = []
     fatal_error = None

     # We need to map target columns back to their source table.column definition
     # This version assumes the column_mappings dict correctly maps source `table.column` to the target column
     for new_col_name, new_col_definition in new_schema_dict.items():
         source_specifier = None
         # Find which source 'table.column' maps to this new_col_name
         for src_key, target_col in column_mappings.items():
             if target_col == new_col_name:
                 source_specifier = src_key
                 break

         if not source_specifier or '.' not in source_specifier:
             # Maybe allow mapping without table prefix if unambiguous? Risky.
             # For now, require explicit table.column mapping for merges.
             print(f"Warning: Target column '{new_col_name}' in merge '{target_schema_name}' has no valid 'table.column' mapping. Skipping.")
             continue

         source_table, source_col_name = source_specifier.split('.', 1)

         if source_table not in all_source_schemas or source_col_name not in all_source_schemas[source_table]:
             fatal_error = f"Mapped source '{source_specifier}' for target '{new_col_name}' not found in source table schemas."
             break

         source_col_details = all_source_schemas[source_table][source_col_name]
         old_col_type = source_col_details[1]
         new_col_type = new_col_definition.split()[0] # Basic type

         safety = is_conversion_safe(old_col_type, new_col_type)
         requires_cast = False
         cast_type_str = None

         if safety == 'unsafe':
             fatal_error = (f"Unsafe type conversion detected for merge:\n"
                            f"Source: `{source_table}`.`{source_col_name}` ({old_col_type})\n"
                            f"Target: `{target_schema_name}`.`{new_col_name}` ({new_col_type})")
             break # Stop processing on unsafe conversion

         if get_base_type(old_col_type) != get_base_type(new_col_type):
             if safety == 'lossy':
                 lossy_conversions.append(f"- `{new_col_name}` ({old_col_type} -> {new_col_type}) from `{source_specifier}`")
             requires_cast = True
             cast_type_str = get_cast_type(new_col_type)

         select_expression = f"`{source_table}`.`{source_col_name}`" # Use qualified names
         if requires_cast and cast_type_str:
             select_expression = f"CAST(`{source_table}`.`{source_col_name}` AS {cast_type_str})"

         select_pairs.append((select_expression, new_col_name, old_col_type, new_col_type))

     if fatal_error:
         return None, None, fatal_error, None

     # Construct the FROM and JOIN part
     if not source_tables: return None, None, "No source tables defined", None

     from_clause = f"`{source_tables[0]}`"
     # Simple sequential JOIN based on provided string - THIS IS A MAJOR SIMPLIFICATION
     # A robust solution needs proper JOIN parsing or structured input
     from_clause += f" {join_conditions}" if join_conditions else ""

     select_clause = ", ".join([p[0] for p in select_pairs])
     insert_cols = [f"`{p[1]}`" for p in select_pairs] # Target columns for INSERT

     return insert_cols, select_clause, from_clause, lossy_conversions


def copy_data_in_batches(source_ref, new_table_name, insert_cols, select_clause, from_clause, cursor, conn):
    """Copies data using INSERT ... SELECT, handling batches. source_ref is old table name or merge key.""" #
    if not insert_cols or not select_clause or not from_clause: #
        print(f"Invalid parameters for data copy to {new_table_name}. Skipping.") #
        return True # Or False? Arguably not a success, but avoids blocking workflow?

    insert_cols_str = ", ".join(insert_cols) #

    try: #
        # Count rows based on the FROM clause (might be complex for JOINs)
        # Simple count from the first table in merge/split source as an estimate
        count_source_table = source_ref
        if source_ref.startswith("MERGE:"):
             map_info = table_mappings.get(source_ref)
             if map_info and map_info.get("source_tables"):
                  count_source_table = map_info["source_tables"][0]
             else: count_source_table = None # Cannot determine count source

        total_rows = 0
        if count_source_table:
             cursor.execute(f"SELECT COUNT(*) FROM `{count_source_table}`") #
             total_rows = cursor.fetchone()[0] #
             if total_rows == 0: #
                 print(f"Source table '{count_source_table}' for {new_table_name} appears empty. No data to copy.") #
                 return True #
             print(f"Approx rows to copy for {new_table_name} (based on '{count_source_table}'): {total_rows}") #
        else:
             print(f"Warning: Could not determine primary source table to estimate row count for {new_table_name}.")
             total_rows = float('inf') # Indicate unknown count

        BATCH_SIZE = 5000 # Smaller batch size might be safer #
        rows_copied = 0 #
        print(f"Starting data copy into '{new_table_name}'...") #
        offset = 0
        while True:
            if not conn.is_connected(): #
                 messagebox.showerror("Connection Error", "Database connection lost during data copy.") #
                 return False #

            # Add LIMIT/OFFSET to the *end* of the constructed FROM clause
            # ORDER BY is tricky with JOINs, using primary key of first table if possible, else default '1'
            # This needs refinement for reliable ordering in merges.
            order_by_col = "1" # Default
            if not source_ref.startswith("MERGE:") and count_source_table:
                 try:
                      cursor.execute(f"SHOW KEYS FROM `{count_source_table}` WHERE Key_name = 'PRIMARY'")
                      pk = cursor.fetchone()
                      if pk: order_by_col = f"`{count_source_table}`.`{pk[4]}`" # Use qualified PK column
                 except: pass # Ignore errors finding PK

            limited_from_clause = f"{from_clause} ORDER BY {order_by_col} LIMIT {BATCH_SIZE} OFFSET {offset}"

            copy_query = f"INSERT INTO `{new_table_name}` ({insert_cols_str}) SELECT {select_clause} FROM {limited_from_clause};" #

            try: #
                cursor.execute(copy_query) #
                batch_rows = cursor.rowcount #
                rows_copied += batch_rows #

                # Check for warnings
                cursor.execute("SHOW WARNINGS") #
                warnings = cursor.fetchall() #
                if warnings: #
                    warning_messages = "\n".join([f"  - L{w[0]} C{w[1]}: {w[2]}" for w in warnings[:5]]) # Show first 5 warnings #
                    print(f"Warnings during batch insert (Offset {offset}, {batch_rows} rows):\n{warning_messages}") #
                    if len(warnings) > 5: print("  ...") #
                conn.commit() #
                print(f"  Copied batch: {rows_copied}{'/' + str(total_rows) if total_rows != float('inf') else ''} rows...") #

                if batch_rows < BATCH_SIZE: # Last batch
                     break
                offset += BATCH_SIZE

            except mysql.connector.Error as batch_err: #
                 conn.rollback() #
                 messagebox.showerror("Data Copy Error", #
                                    f"Error copying data batch for {new_table_name} (offset {offset}). Rolled back.\n" #
                                    f"Error: {batch_err}\nQuery:\n{copy_query}\n") #
                 return False #

        print(f"Finished copying data for {new_table_name}. Total rows affected: {rows_copied}") #
        if total_rows != float('inf') and rows_copied != total_rows: #
             print(f"Warning: Rows copied ({rows_copied}) != estimated source count ({total_rows}).") #
        return True #
    except mysql.connector.Error as err: #
        messagebox.showerror("Data Copy Error", f"Failed during data copy prep for {new_table_name}: {err}") #
        return False #


def generate_create_statement(target_table_name, new_schema_definition):
    """Generates the CREATE TABLE statement from the parsed schema definition, avoiding duplicate PK."""
    column_definitions = []
    constraints = []
    inline_pk_found = False # Flag to check if PK is defined with a column

    for col_name, definition in new_schema_definition.items():
        col_def_line = f"  `{col_name}` {definition}"
        column_definitions.append(col_def_line)
        # Check if this line defines the primary key
        if "PRIMARY KEY" in definition.upper():
            inline_pk_found = True
        # Simple check for inline UNIQUE (can be combined with other constraints)
        # if "UNIQUE" in definition.upper():
        #     constraints.append(f"  UNIQUE KEY `uq_{col_name}` (`{col_name}`)") # Example constraint

    # Add a separate PRIMARY KEY constraint ONLY if not defined inline
    if not inline_pk_found:
        # Try to find the PK column from definitions (less reliable)
        pk_candidates = [col for col, defin in new_schema_definition.items() if "PRIMARY KEY" in defin.upper()]
        # Or look for a column commonly named 'id' if no explicit inline PK found
        if not pk_candidates and 'id' in new_schema_definition:
             pk_candidates = ['id']

        if pk_candidates: # Add separate constraint if a candidate found and no inline PK
            # Assuming single column PK for simplicity here
            constraints.append(f"  PRIMARY KEY (`{pk_candidates[0]}`)")

    statement = f"CREATE TABLE `{target_table_name}` (\n"
    statement += ",\n".join(column_definitions)
    if constraints:
        statement += ",\n" + ",\n".join(constraints)
    statement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    return statement

def create_new_table_and_copy_data():
    """Handles the process of creating the new table(s) and copying data based on the selected mapping (single, split, or merge).""" #
    if not check_constraints(): # Check manual confirmation gate
         messagebox.showwarning("Confirmation Needed", "Please check all boxes in 'Manual Confirmation Gate' first.") #
         return #

    selected_indices = tables_listbox_old.curselection() #
    if not selected_indices: #
        messagebox.showerror("Error", "Please select a table or mapping from the 'Old/Source Tables' list.") #
        return
    selected_item_text = tables_listbox_old.get(selected_indices) #

    # Determine mapping type and key/source table
    mapping_key = selected_item_text # This is the key in table_mappings dict
    map_info = table_mappings.get(mapping_key)
    mapping_type = None
    source_table_for_single_split = None # The single source table for single/split

    if isinstance(map_info, dict):
        mapping_type = map_info.get("type")
        if mapping_type in ["single", "split"]:
             source_table_for_single_split = mapping_key # For single/split, the key is the source table name
        elif mapping_type == "merge":
             # Merge key is the display name, actual mapping info is under this key
             pass
        else: # Should not happen with load_mappings checks
             messagebox.showerror("Error", f"Invalid mapping type found for '{mapping_key}'.")
             return
    elif not mapping_key.startswith("MERGE:"): # Unmapped table treated as potential single mapping
         mapping_type = "single"
         source_table_for_single_split = mapping_key
         # Create a temporary mapping entry for processing if auto-map found schema
         parsed_schema = parse_schema_file(schema_file_path)
         if mapping_key in parsed_schema:
              map_info = {"type": "single", "new_table_name_schema": mapping_key, "column_mappings": {}}
         else:
              messagebox.showerror("Error", f"Table '{mapping_key}' is not mapped and not found in schema.")
              return
    else:
         messagebox.showerror("Error", f"Could not find valid mapping information for '{mapping_key}'.")
         return

    # --- Process based on mapping type ---
    overall_success = True
    created_tables = []

    # == SINGLE Table Mapping ==
    if mapping_type == "single":
         target_schema_name = map_info.get("new_table_name_schema")
         if not target_schema_name: messagebox.showerror("Error", f"No target schema name defined for '{source_table_for_single_split}'."); return

         new_db_table_name = f"{target_schema_name}_new" #
         db_schema = get_db_schema(source_table_for_single_split) #
         new_schema_defs = get_schema_file_definition(source_table_for_single_split) # Dict with one entry

         if not db_schema: messagebox.showerror("Error", f"Could not get DB schema for '{source_table_for_single_split}'."); return #
         if not new_schema_defs or target_schema_name not in new_schema_defs: messagebox.showerror("Error", f"No schema definition found for target '{target_schema_name}'."); return #

         new_schema_def = new_schema_defs[target_schema_name] #

         # Check for Existing Target Table
         try: #
             cursor.execute(f"SHOW TABLES LIKE '{new_db_table_name}'") #
             if cursor.fetchone(): #
                 messagebox.showwarning("Warning", f"Target table '{new_db_table_name}' already exists. Drop manually to recreate.") #
                 return #
         except mysql.connector.Error as err: #
              messagebox.showerror("DB Error", f"Error checking for table '{new_db_table_name}': {err}") #
              return #

         # Analyze Column Changes (checks for unsafe conversions internally)
         copy_pairs_analysis = get_copy_column_pairs(source_table_for_single_split, target_schema_name, db_schema, new_schema_def, table_mappings) #
         if copy_pairs_analysis is None: # Unsafe conversion detected and message shown
             return #

         # Check for lossy conversions and prompt user
         lossy_conversions_prompt = [f"- `{p[1]}` ({p[4]} -> {p[5]})" for p in copy_pairs_analysis #
                                    if get_base_type(p[4]) != get_base_type(p[5]) and is_conversion_safe(p[4], p[5]) == 'lossy'] #
         if lossy_conversions_prompt: #
             message = "Potential data loss/truncation for conversions:\n\n" + "\n".join(lossy_conversions_prompt) + "\n\nProceed anyway?" #
             if not messagebox.askyesno("Potential Data Loss Warning", message): #
                 messagebox.showinfo("Aborted", "Table creation aborted by user.") #
                 return #

         # Generate and Execute CREATE TABLE
         create_statement = generate_create_statement(new_db_table_name, new_schema_def) #
         try: #
             print(f"Executing CREATE TABLE statement for {new_db_table_name}...") #
             cursor.execute(create_statement) #
             conn.commit() #
             print(f"Table '{new_db_table_name}' created successfully.") #
             created_tables.append(new_db_table_name)
         except mysql.connector.Error as err: #
             conn.rollback() #
             messagebox.showerror("Create Table Error", f"Failed to create table '{new_db_table_name}':\n{err}\n\nSQL:\n{create_statement}") #
             return #

         # Prepare for data copy
         insert_cols = [f"`{pair[1]}`" for pair in copy_pairs_analysis]
         select_clause = ", ".join([pair[0] for pair in copy_pairs_analysis])
         from_clause = f"`{source_table_for_single_split}`"

         # Copy Data
         copy_successful = copy_data_in_batches(source_table_for_single_split, new_db_table_name, insert_cols, select_clause, from_clause, cursor, conn) #
         if not copy_successful: overall_success = False

    # == SPLIT Table Mapping ==
    elif mapping_type == "split":
        target_tables_info = map_info.get("new_tables", [])
        if not target_tables_info: messagebox.showerror("Error", f"No target tables defined for split mapping '{source_table_for_single_split}'."); return

        db_schema = get_db_schema(source_table_for_single_split)
        if not db_schema: messagebox.showerror("Error", f"Could not get DB schema for source '{source_table_for_single_split}'."); return

        new_schema_defs = get_schema_file_definition(source_table_for_single_split) # Dict of schema_name: definition
        if not new_schema_defs: messagebox.showerror("Error", "Could not find schema definitions for split targets."); return

        # --- Pre-checks for all target tables ---
        all_checks_passed = True
        lossy_conversion_warnings = {} # target_schema_name: [warnings]
        copy_details_per_target = {} # target_schema_name: (copy_pairs, insert_cols, select_clause)

        for target_info in target_tables_info:
            target_schema_name = target_info.get("schema_name")
            if not target_schema_name: continue # Skip if no name

            new_db_table_name = f"{target_schema_name}_new"
            new_schema_def = new_schema_defs.get(target_schema_name)

            if not new_schema_def:
                messagebox.showerror("Error", f"Schema definition missing for split target '{target_schema_name}'."); all_checks_passed = False; break

            try:
                cursor.execute(f"SHOW TABLES LIKE '{new_db_table_name}'")
                if cursor.fetchone():
                    messagebox.showwarning("Warning", f"Target split table '{new_db_table_name}' already exists. Drop manually to recreate."); all_checks_passed = False; break
            except mysql.connector.Error as err:
                 messagebox.showerror("DB Error", f"Error checking for table '{new_db_table_name}': {err}"); all_checks_passed = False; break

            copy_pairs = get_copy_column_pairs(source_table_for_single_split, target_schema_name, db_schema, new_schema_def, table_mappings)
            if copy_pairs is None: all_checks_passed = False; break # Unsafe conversion

            lossy = [f"- `{p[1]}` ({p[4]} -> {p[5]})" for p in copy_pairs if get_base_type(p[4]) != get_base_type(p[5]) and is_conversion_safe(p[4], p[5]) == 'lossy']
            if lossy: lossy_conversion_warnings[target_schema_name] = lossy

            insert_cols = [f"`{pair[1]}`" for pair in copy_pairs]
            select_clause = ", ".join([pair[0] for pair in copy_pairs])
            copy_details_per_target[target_schema_name] = (copy_pairs, insert_cols, select_clause)

        if not all_checks_passed: return # Stop if pre-checks failed

        # --- Confirm Lossy Conversions (if any) ---
        if lossy_conversion_warnings:
             full_warning_msg = "Potential data loss/truncation for split conversions:\n\n"
             for t_name, warnings in lossy_conversion_warnings.items():
                  full_warning_msg += f"For target '{t_name}':\n" + "\n".join(warnings) + "\n\n"
             full_warning_msg += "Proceed anyway?"
             if not messagebox.askyesno("Potential Data Loss Warning (Split)", full_warning_msg):
                 messagebox.showinfo("Aborted", "Table creation aborted by user."); return

        # --- Create and Copy for each target ---
        from_clause = f"`{source_table_for_single_split}`"
        for target_info in target_tables_info:
             target_schema_name = target_info.get("schema_name")
             if not target_schema_name: continue

             new_db_table_name = f"{target_schema_name}_new"
             new_schema_def = new_schema_defs[target_schema_name]
             _, insert_cols, select_clause = copy_details_per_target[target_schema_name]

             # Generate and Execute CREATE TABLE
             create_statement = generate_create_statement(new_db_table_name, new_schema_def)
             try:
                 print(f"Executing CREATE TABLE statement for split target {new_db_table_name}...")
                 cursor.execute(create_statement)
                 conn.commit()
                 print(f"Table '{new_db_table_name}' created successfully.")
                 created_tables.append(new_db_table_name)
             except mysql.connector.Error as err:
                 conn.rollback()
                 messagebox.showerror("Create Table Error (Split)", f"Failed to create table '{new_db_table_name}':\n{err}\n\nSQL:\n{create_statement}")
                 overall_success = False; break # Stop processing further tables on error

             # Copy Data
             copy_successful = copy_data_in_batches(source_table_for_single_split, new_db_table_name, insert_cols, select_clause, from_clause, cursor, conn)
             if not copy_successful: overall_success = False; break # Stop processing further tables

    # == MERGE Table Mapping ==
    elif mapping_type == "merge":
         target_schema_name = map_info.get("new_table_name_schema")
         if not target_schema_name: messagebox.showerror("Error", "No target schema name defined for merge."); return

         new_db_table_name = f"{target_schema_name}_new"
         parsed_schema = parse_schema_file(schema_file_path)
         new_schema_def = parsed_schema.get(target_schema_name) # Fetch directly

         if not new_schema_def:
             messagebox.showerror("Error", f"Schema definition not found for merge target '{target_schema_name}'."); return

         # Check for Existing Target Table
         try:
             cursor.execute(f"SHOW TABLES LIKE '{new_db_table_name}'")
             if cursor.fetchone():
                 messagebox.showwarning("Warning", f"Target merge table '{new_db_table_name}' already exists. Drop manually to recreate."); return
         except mysql.connector.Error as err:
              messagebox.showerror("DB Error", f"Error checking for table '{new_db_table_name}': {err}"); return

         # Get merge details (columns, select, from, lossy warnings)
         insert_cols, select_clause, from_clause, lossy_conversions = get_merge_copy_details(mapping_key, table_mappings, parsed_schema)

         if insert_cols is None: # An error occurred during detail gathering
             messagebox.showerror("Merge Setup Error", from_clause) # from_clause holds error message here
             return

         # Confirm Lossy Conversions
         if lossy_conversions:
              message = "Potential data loss/truncation for merge conversions:\n\n" + "\n".join(lossy_conversions) + "\n\nProceed anyway?"
              if not messagebox.askyesno("Potential Data Loss Warning (Merge)", message):
                  messagebox.showinfo("Aborted", "Table creation aborted by user."); return

         # Generate and Execute CREATE TABLE
         create_statement = generate_create_statement(new_db_table_name, new_schema_def)
         try:
             print(f"Executing CREATE TABLE statement for merge target {new_db_table_name}...")
             cursor.execute(create_statement)
             conn.commit()
             print(f"Table '{new_db_table_name}' created successfully.")
             created_tables.append(new_db_table_name)
         except mysql.connector.Error as err:
             conn.rollback()
             messagebox.showerror("Create Table Error (Merge)", f"Failed to create table '{new_db_table_name}':\n{err}\n\nSQL:\n{create_statement}")
             return

         # Copy Data (using the complex select/from clauses)
         copy_successful = copy_data_in_batches(mapping_key, new_db_table_name, insert_cols, select_clause, from_clause, cursor, conn)
         if not copy_successful: overall_success = False

    # --- Post-Creation ---
    if created_tables: # Only show success if at least one table was attempted
         if overall_success: #
             messagebox.showinfo("Success", f"Table(s) '{', '.join(created_tables)}' created and data copy process initiated successfully!") #
         else: #
             messagebox.showerror("Error", f"Process completed with errors. Some tables ('{', '.join(created_tables)}') may be created, but data copy FAILED for at least one. Check logs/table state.") #

    get_tables() # Refresh table lists regardless of success/failure #
    # Reset Checkboxes
    if 'constraint_vars' in globals() and constraint_vars: #
        for var in constraint_vars: var.set(False) #
    check_constraints() #


# --- Data Viewing ---

def safe_serializer(obj):
    """JSON serializer for types not handled by default encoder.""" #
    if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)): #
        return obj.isoformat() #
    elif isinstance(obj, decimal.Decimal): #
        return str(obj) # Use string for exact Decimal representation #
    elif isinstance(obj, bytes): #
        try: #
            return obj.decode('utf-8', errors='replace') # Try UTF-8 first #
        except UnicodeDecodeError: #
            return obj.hex() # Fallback to hex if decode fails #
    # Let the default encoder handle built-in types and raise TypeError for others
    raise TypeError(f"Type {type(obj)} not serializable for JSON") #


def view_data(table_name, columns, data, data_window):
    """General function to display data in a Treeview and add download options.""" #
    frame = ttk.Frame(data_window) #
    frame.pack(fill=tk.BOTH, expand=True) #

    tree_scroll_y = ttk.Scrollbar(frame, orient="vertical") #
    tree_scroll_x = ttk.Scrollbar(frame, orient="horizontal") #
    data_tree = ttk.Treeview(frame, columns=columns, show="headings", #
                             yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set) #
    tree_scroll_y.config(command=data_tree.yview) #
    tree_scroll_x.config(command=data_tree.xview) #

    tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y) #
    tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X) #
    data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True) #

    for col in columns: #
        data_tree.heading(col, text=col, anchor='w') #
        data_tree.column(col, width=120, anchor='w', stretch=tk.NO) #

    MAX_ROWS_DISPLAY = 5000 # Limit direct display for performance #
    display_data = data[:MAX_ROWS_DISPLAY] #
    for i, row in enumerate(display_data): #
        display_row = [] #
        for v in row: #
            if isinstance(v, bytes): #
                 try: display_row.append(v.decode('utf-8', errors='replace')) #
                 except: display_row.append(f"<{len(v)} bytes>") # Show placeholder for un-decodable bytes #
            elif v is None: display_row.append('NULL') #
            else: display_row.append(str(v)) #
        data_tree.insert("", tk.END, values=display_row) #

    if len(data) > MAX_ROWS_DISPLAY: #
         data_tree.insert("", tk.END, values=(f"... (displaying first {MAX_ROWS_DISPLAY} of {len(data)} rows) ...",)) #

    # --- Download Functions ---
    def download_csv(): #
        initial_filename = re.sub(r'[^\w\-]+', '_', table_name) #
        file_path = filedialog.asksaveasfilename( #
            title="Save as CSV", initialfile=f"{initial_filename}_data.csv", defaultextension=".csv", #
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")], parent=data_window ) #
        if file_path: #
            try: #
                with open(file_path, 'w', newline='', encoding='utf-8') as csvfile: #
                    csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL) #
                    csv_writer.writerow(columns) # Header #
                    for row_data in data: # Write ALL data #
                         csv_row = [] #
                         for v in row_data: #
                              if v is None: csv_row.append('') #
                              elif isinstance(v, bytes): #
                                   try: csv_row.append(v.decode('utf-8', errors='replace')) #
                                   except: csv_row.append(f"<bytes:{len(v)}>") # CSV placeholder #
                              else: csv_row.append(str(v)) #
                         csv_writer.writerow(csv_row) #
                messagebox.showinfo("Download Successful", f"Full data ({len(data)} rows) downloaded to\n{file_path}", parent=data_window) #
            except Exception as e: #
                 messagebox.showerror("Download Error", f"Failed to download CSV data: {e}", parent=data_window) #

    def download_json(): #
        initial_filename = re.sub(r'[^\w\-]+', '_', table_name) #
        file_path = filedialog.asksaveasfilename( #
            title="Save as JSON", initialfile=f"{initial_filename}_data.json", defaultextension=".json", #
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")], parent=data_window ) #
        if file_path: #
            try: #
                 json_data = [dict(zip(columns, row_data)) for row_data in data] # Write ALL data #
                 with open(file_path, 'w', encoding='utf-8') as jsonfile: #
                    json.dump(json_data, jsonfile, indent=2, default=safe_serializer) # Use safe_serializer #
                 messagebox.showinfo("Download Successful", f"Full data ({len(data)} rows) downloaded to\n{file_path}", parent=data_window) #
            except TypeError as e: #
                 messagebox.showerror("JSON Error", f"Failed to serialize data to JSON.\nError: {e}\nCheck for unsupported data types.", parent=data_window) #
            except Exception as e: #
                messagebox.showerror("Download Error", f"Failed to download JSON data: {e}", parent=data_window) #

    # --- Download Buttons ---
    download_frame = ttk.Frame(data_window) #
    download_frame.pack(pady=10) #
    ttk.Label(download_frame, text="Download Full Data:").pack(side=tk.LEFT, padx=5) #
    ttk.Button(download_frame, text="CSV", command=download_csv).pack(side=tk.LEFT, padx=5) #
    ttk.Button(download_frame, text="JSON", command=download_json).pack(side=tk.LEFT, padx=5) #


def view_old_data():
    """Fetches and displays data from the selected 'Old Table' (non-merge entries).""" #
    selected_index = tables_listbox_old.curselection() #
    if not selected_index: #
        messagebox.showerror("Error", "Please select a table from 'Old Tables' list.") #
        return
    selected_item_text = tables_listbox_old.get(selected_index) #

    # Don't view data for merge representations
    if selected_item_text.startswith("MERGE:"):
         messagebox.showinfo("Info", "Select an individual source table to view its original data.")
         return

    selected_table = selected_item_text # It's a regular table name

    try: #
        if not conn or not conn.is_connected(): #
            messagebox.showerror("Error", "Database connection lost.") #
            return #
        print(f"Fetching data for OLD table: {selected_table}...") #
        cursor.execute(f"SELECT * FROM `{selected_table}`") #
        data = cursor.fetchall() #
        print(f"Fetched {len(data)} rows.") #
        if not data and cursor.rowcount == 0: #
             messagebox.showinfo("No Data", f"Table '{selected_table}' is empty.") #
             return #

        columns = [desc[0] for desc in cursor.description] #
        data_window = Toplevel(root) #
        data_window.title(f"Data from OLD: {selected_table} ({len(data)} rows)") #
        center_window(data_window, 800, 500) #
        view_data(selected_table, columns, data, data_window) #

    except mysql.connector.Error as err: #
        messagebox.showerror("DB Error", f"Error fetching data for '{selected_table}': {err}") #
    except Exception as e: #
        messagebox.showerror("Error", f"Unexpected error viewing old data: {e}") #


def view_new_data():
    """Fetches and displays data from the selected 'New Table' or the one(s) corresponding to the selected Old/Source mapping.""" #
    selected_table = None #
    source_listbox = None #
    source_description = "" # More context for the title

    # Prioritize selection from the _new listbox
    selected_index_new = tables_listbox_new.curselection() #
    if selected_index_new: #
        selected_table = tables_listbox_new.get(selected_index_new) #
        source_description = f"New Table '{selected_table}'" #
    else: #
        # Check old table list selection as fallback
        old_selected_index = tables_listbox_old.curselection() #
        if old_selected_index: #
             selected_item_text = tables_listbox_old.get(old_selected_index) #
             mapping_key = selected_item_text
             map_info = table_mappings.get(mapping_key)

             potential_new_tables = []
             if isinstance(map_info, dict):
                 map_type = map_info.get("type")
                 if map_type == "single":
                     target_schema_name = map_info.get("new_table_name_schema", mapping_key if not mapping_key.startswith("MERGE:") else None) #
                     if target_schema_name: potential_new_tables.append(f"{target_schema_name}_new")
                     source_description = f"Old '{mapping_key}' ->" #
                 elif map_type == "split":
                      target_names = [nt.get("schema_name") for nt in map_info.get("new_tables", [])]
                      potential_new_tables = [f"{name}_new" for name in target_names if name]
                      source_description = f"Split '{mapping_key}' ->"
                 elif map_type == "merge":
                      target_schema_name = map_info.get("new_table_name_schema")
                      if target_schema_name: potential_new_tables.append(f"{target_schema_name}_new")
                      source_description = f"Merge '{mapping_key}' ->"
             elif not mapping_key.startswith("MERGE:"): # Unmapped but maybe exists
                  potential_new_tables.append(f"{mapping_key}_new")
                  source_description = f"Unmapped '{mapping_key}' ->"

             if not potential_new_tables:
                  messagebox.showerror("Error", f"Could not determine corresponding new table(s) for '{selected_item_text}'."); return

             # If multiple targets (split), ask user which one to view
             if len(potential_new_tables) > 1:
                  # Simple approach: view the first one that exists
                  found_table = None
                  for pnt in potential_new_tables:
                       try: #
                            if not conn or not conn.is_connected(): raise mysql.connector.Error("Not connected") #
                            cursor.execute(f"SHOW TABLES LIKE '{pnt}'") #
                            if cursor.fetchone(): found_table = pnt; break #
                       except mysql.connector.Error: continue # Ignore DB error checking existence
                  if found_table:
                       selected_table = found_table
                       source_description += f" (showing '{found_table}')"
                  else:
                       messagebox.showerror("Error", f"None of the corresponding tables ({', '.join(potential_new_tables)}) found."); return #
             else: # Single target derived
                 selected_table = potential_new_tables[0] #
                 # Verify _new table exists
                 try: #
                     if not conn or not conn.is_connected(): raise mysql.connector.Error("Not connected") #
                     cursor.execute(f"SHOW TABLES LIKE '{selected_table}'") #
                     if not cursor.fetchone(): messagebox.showerror("Error", f"Corresponding table '{selected_table}' not found."); return #
                 except mysql.connector.Error as err: messagebox.showerror("Error", f"DB error checking for '{selected_table}': {err}"); return #
        else: #
             messagebox.showerror("Error", "Select a table/mapping from 'Old/Source' or a table from 'New Tables'."); return #

    if selected_table: #
        try: #
            if not conn or not conn.is_connected(): messagebox.showerror("Error", "Database connection lost."); return #
            print(f"Fetching data for NEW table: {selected_table}...") #
            cursor.execute(f"SELECT * FROM `{selected_table}`") #
            data = cursor.fetchall() #
            print(f"Fetched {len(data)} rows.") #
            if not data and cursor.rowcount == 0: messagebox.showinfo("No Data", f"Table '{selected_table}' is empty."); return #

            columns = [desc[0] for desc in cursor.description] #
            data_window = Toplevel(root) #
            data_window.title(f"Data from NEW ({source_description} {selected_table}) - {len(data)} rows") #
            center_window(data_window, 800, 500) #
            view_data(selected_table, columns, data, data_window) #

        except mysql.connector.Error as err: messagebox.showerror("DB Error", f"Error fetching data for '{selected_table}': {err}") #
        except Exception as e: messagebox.showerror("Error", f"Unexpected error viewing new data: {e}") #


# --- Mapping Logic ---

def auto_map_tables_and_columns(parsed_schema):
    """Automatically maps tables and columns with identical names on initial load (for single mappings).""" #
    global table_mappings
    if not conn or not conn.is_connected() or not parsed_schema: return #

    try: #
        cursor.execute("SHOW TABLES") #
        all_tables_db = [table[0] for table in cursor.fetchall()] #
    except mysql.connector.Error as e: #
        print(f"Warning: Could not fetch tables during auto-mapping: {e}") #
        return #

    made_changes = False #
    print("Starting auto-mapping...") #
    for table_db in all_tables_db: #
        if table_db.endswith("_new"): continue #

        # Auto map table if name matches schema and not already mapped in any way
        if table_db in parsed_schema and table_db not in table_mappings: #
             # Check if this table is part of an existing merge source
             is_in_merge = any(isinstance(m, dict) and m.get("type") == "merge" and table_db in m.get("source_tables", []) for m in table_mappings.values())
             if not is_in_merge:
                 print(f"- Auto-mapping table: '{table_db}' (single)") #
                 table_mappings[table_db] = {"type": "single", "new_table_name_schema": table_db, "column_mappings": {}} #
                 made_changes = True #

        # Auto map columns for *single* type mappings only
        map_info = table_mappings.get(table_db)
        if isinstance(map_info, dict) and map_info.get("type") == "single": #
             target_schema_name = map_info.get("new_table_name_schema") #
             if target_schema_name in parsed_schema: #
                 db_schema = get_db_schema(table_db) #
                 if db_schema: #
                      new_schema_cols = parsed_schema[target_schema_name] #
                      if "column_mappings" not in map_info: map_info["column_mappings"] = {} #
                      current_col_mappings = map_info["column_mappings"] #
                      for db_col_name in db_schema.keys(): #
                          if (db_col_name in new_schema_cols and #
                              db_col_name not in current_col_mappings and #
                              db_col_name not in current_col_mappings.values()): #
                              print(f"  - Auto-mapping column: {table_db}.{db_col_name}") #
                              current_col_mappings[db_col_name] = db_col_name #
                              made_changes = True #

    if made_changes: #
        print("Saving updated auto-mappings...") #
        save_mappings(table_mappings) #
    else: #
        print("No new auto-mappings found.") #


def browse_file():
    """Opens file dialog to select schema file.""" #
    current_dir = os.path.dirname(schema_file_entry.get()) #
    if not os.path.isdir(current_dir): current_dir = os.path.expanduser("~") #

    file_path = filedialog.askopenfilename( #
        title="Select Schema Definition File", initialdir=current_dir, #
        filetypes=[("Schema files", "*.txt;*.sql"), ("All files", "*.*")] ) #
    if file_path: #
        schema_file_entry.delete(0, tk.END) #
        schema_file_entry.insert(0, file_path) #


def map_tables():
    """Opens a window to manually map an old DB table to a single schema table name.""" #
    selected_indices = tables_listbox_old.curselection() #
    if not selected_indices: #
        messagebox.showerror("Error", "Select a table from 'Old Tables' list to map.") #
        return #
    selected_table_db = tables_listbox_old.get(selected_indices) #

    # Prevent mapping of merge representations
    if selected_table_db.startswith("MERGE:"):
         messagebox.showerror("Error", "Cannot apply single mapping to a 'MERGE' entry. Use 'Merge Tables...' button.")
         return

    parsed_schema = parse_schema_file(schema_file_path) #
    if not parsed_schema: #
         messagebox.showerror("Error", "Cannot map tables, schema file empty or invalid.") #
         return #

    schema_table_names = sorted(list(parsed_schema.keys())) #

    # Find targets already mapped from OTHER tables (any type)
    currently_mapped_targets = set()
    for key, m_data in table_mappings.items():
        if key == selected_table_db: continue # Skip self
        if isinstance(m_data, dict):
             m_type = m_data.get("type")
             if m_type == "single":
                  currently_mapped_targets.add(m_data.get("new_table_name_schema"))
             elif m_type == "split":
                  currently_mapped_targets.update(nt.get("schema_name") for nt in m_data.get("new_tables", []))
             elif m_type == "merge":
                   currently_mapped_targets.add(m_data.get("new_table_name_schema"))

    available_schema_tables = [name for name in schema_table_names if name not in currently_mapped_targets and name is not None] #

    current_target = None
    map_info = table_mappings.get(selected_table_db)
    if isinstance(map_info, dict) and map_info.get("type") == "single":
         current_target = map_info.get("new_table_name_schema") #

    # Ensure current target is in the list if it exists
    if current_target and current_target in schema_table_names and current_target not in available_schema_tables: #
         available_schema_tables.insert(0, current_target) #

    if not available_schema_tables: #
         messagebox.showinfo("No Tables to Map", "All tables in the schema file seem mapped from other tables or are unavailable.") #
         return #

    # --- Mapping Window ---
    map_win = Toplevel(root) #
    map_win.title(f"Map Table '{selected_table_db}' (Single Target)") #
    center_window(map_win, 400, 200) #
    map_win.transient(root) #
    map_win.grab_set() #

    ttk.Label(map_win, text=f"Map DB table '{selected_table_db}' to\nwhich table name in schema file?").pack(pady=10) #
    new_table_var = tk.StringVar(map_win) #
    if current_target and current_target in available_schema_tables: new_table_var.set(current_target) #
    elif available_schema_tables: new_table_var.set(available_schema_tables[0]) #

    dropdown = ttk.Combobox(map_win, textvariable=new_table_var, values=available_schema_tables, state="readonly", width=35) #
    dropdown.pack(pady=5) #

    def confirm_mapping(): #
        chosen_schema_table = new_table_var.get() #
        if not chosen_schema_table: messagebox.showerror("Error", "Select a schema table name.", parent=map_win); return #

        # Overwrite any existing mapping for this table with a 'single' type
        table_mappings[selected_table_db] = { #
             "type": "single",
             "new_table_name_schema": chosen_schema_table, #
             "column_mappings": {} # Reset column mappings on table map change #
         }

        # Attempt column auto-map after table map
        target_schema_def = parsed_schema.get(chosen_schema_table, {}) #
        db_schema = get_db_schema(selected_table_db) #
        if db_schema and target_schema_def: #
            current_col_maps = table_mappings[selected_table_db]["column_mappings"] #
            print(f"Attempting column auto-map after table map '{selected_table_db}' -> '{chosen_schema_table}'") #
            newly_mapped_cols = 0 #
            for db_col in db_schema: #
                 if db_col in target_schema_def and db_col not in current_col_maps and db_col not in current_col_maps.values(): #
                      print(f"  - Auto-mapping column: {selected_table_db}.{db_col}") #
                      current_col_maps[db_col] = db_col; newly_mapped_cols += 1 #
            if newly_mapped_cols > 0: print(f"  Auto-mapped {newly_mapped_cols} columns.") #

        save_mappings(table_mappings) #
        map_win.destroy() #
        get_tables(); show_schema() #

    def unmap_table(): #
         if selected_table_db in table_mappings: #
              if messagebox.askyesno("Confirm Unmap", f"Remove mapping for table '{selected_table_db}'?", parent=map_win): #
                  del table_mappings[selected_table_db] #
                  save_mappings(table_mappings) #
                  messagebox.showinfo("Unmapped", f"Table '{selected_table_db}' unmapped.", parent=map_win) #
                  map_win.destroy(); get_tables(); show_schema() #
         else: messagebox.showinfo("Info", "Table not currently mapped.", parent=map_win) #

    button_frame = ttk.Frame(map_win) #
    button_frame.pack(pady=15) #
    ttk.Button(button_frame, text="Confirm Map", command=confirm_mapping, width=15).pack(side=tk.LEFT, padx=10) #
    ttk.Button(button_frame, text="Unmap Table", command=unmap_table, width=15).pack(side=tk.LEFT, padx=10) #
    map_win.wait_window() #


# --- Split Table Mapping Window ---
def map_split_table():
     selected_indices = tables_listbox_old.curselection()
     if not selected_indices: messagebox.showerror("Error", "Select a table from 'Old Tables' list to split."); return
     selected_table_db = tables_listbox_old.get(selected_indices)

     if selected_table_db.startswith("MERGE:"): messagebox.showerror("Error", "Cannot split a 'MERGE' entry."); return

     parsed_schema = parse_schema_file(schema_file_path)
     if not parsed_schema: messagebox.showerror("Error", "Cannot map split, schema file empty or invalid."); return

     schema_table_names = sorted(list(parsed_schema.keys()))

     # --- Get current split mapping if exists ---
     current_split_targets = [] # List of {"schema_name": ..., "column_mappings": ...}
     current_target_names = set() # Just the names for quick lookup
     map_info = table_mappings.get(selected_table_db)
     is_currently_split = False
     if isinstance(map_info, dict) and map_info.get("type") == "split":
         current_split_targets = map_info.get("new_tables", [])
         current_target_names = {t.get("schema_name") for t in current_split_targets if t.get("schema_name")}
         is_currently_split = True

     # --- Window Setup ---
     split_win = Toplevel(root)
     split_win.title(f"Split Table '{selected_table_db}'")
     center_window(split_win, 500, 450)
     split_win.transient(root); split_win.grab_set()

     ttk.Label(split_win, text=f"Define target tables in schema for source '{selected_table_db}':").pack(pady=10)

     # --- Target Table List ---
     list_frame = ttk.Frame(split_win); list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
     list_frame.rowconfigure(0, weight=1); list_frame.columnconfigure(0, weight=1)
     target_listbox = Listbox(list_frame, selectmode=tk.SINGLE)
     target_listbox.grid(row=0, column=0, sticky="nsew")
     scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=target_listbox.yview)
     scrollbar.grid(row=0, column=1, sticky="ns")
     target_listbox.config(yscrollcommand=scrollbar.set)
     for target in current_split_targets: # Populate with existing
         target_listbox.insert(tk.END, target.get("schema_name", "Invalid Entry"))

     # --- Add/Remove Controls ---
     controls_frame = ttk.Frame(split_win); controls_frame.pack(pady=5)
     add_var = StringVar(split_win)
     # Filter available schema tables: not already a target for *this* split
     available_schema_for_add = [name for name in schema_table_names if name not in current_target_names]
     add_combo = ttk.Combobox(controls_frame, textvariable=add_var, values=available_schema_for_add, state="readonly", width=25)
     add_combo.grid(row=0, column=0, padx=5); 
     if available_schema_for_add: add_combo.current(0)

     def add_target():
         target_name = add_var.get()
         if target_name and target_name not in current_target_names:
             current_split_targets.append({"schema_name": target_name, "column_mappings": {}})
             current_target_names.add(target_name)
             target_listbox.insert(tk.END, target_name)
             # Update combobox
             available_schema_for_add.remove(target_name)
             add_combo['values'] = available_schema_for_add
             if available_schema_for_add: add_combo.current(0)
             else: add_var.set("")
         elif not target_name: messagebox.showerror("Error", "Select a schema table to add.", parent=split_win)
         else: messagebox.showerror("Error", f"Table '{target_name}' is already a target.", parent=split_win)

     def remove_target():
         selected = target_listbox.curselection()
         if selected:
             idx = selected[0]
             removed_name = target_listbox.get(idx)
             target_listbox.delete(idx)
             current_target_names.remove(removed_name)
             # Remove from the list of dicts
             global current_split_targets
             current_split_targets = [t for t in current_split_targets if t.get("schema_name") != removed_name]
             # Update combobox
             available_schema_for_add.append(removed_name)
             available_schema_for_add.sort()
             add_combo['values'] = available_schema_for_add
             if available_schema_for_add: add_combo.current(0)

     add_button = ttk.Button(controls_frame, text="Add Target", command=add_target)
     add_button.grid(row=0, column=1, padx=5)
     remove_button = ttk.Button(controls_frame, text="Remove Selected", command=remove_target)
     remove_button.grid(row=0, column=2, padx=5)

     # --- Main Buttons ---
     def confirm_split():
         if not current_split_targets:
              if messagebox.askyesno("Confirm Unmap", f"No target tables selected. Remove split mapping for '{selected_table_db}'?", parent=split_win):
                   if selected_table_db in table_mappings: del table_mappings[selected_table_db]
                   save_mappings(table_mappings)
                   split_win.destroy(); get_tables(); show_schema()
              return

         # Auto-map columns for each target
         db_schema = get_db_schema(selected_table_db)
         if db_schema:
              for target_info in current_split_targets:
                   target_name = target_info.get("schema_name")
                   target_schema_def = parsed_schema.get(target_name, {})
                   if target_schema_def:
                        current_cols = target_info.setdefault("column_mappings", {})
                        for db_col in db_schema:
                             if db_col in target_schema_def and db_col not in current_cols and db_col not in current_cols.values():
                                  current_cols[db_col] = db_col
                                  print(f"Auto-mapped column for split: {selected_table_db}.{db_col} -> {target_name}.{db_col}")

         table_mappings[selected_table_db] = {
             "type": "split",
             "new_tables": current_split_targets
         }
         save_mappings(table_mappings)
         split_win.destroy(); get_tables(); show_schema()

     def cancel_split():
          split_win.destroy()

     button_frame = ttk.Frame(split_win); button_frame.pack(pady=15)
     ttk.Button(button_frame, text="Confirm Split", command=confirm_split, width=15).pack(side=tk.LEFT, padx=10)
     ttk.Button(button_frame, text="Cancel", command=cancel_split, width=15).pack(side=tk.LEFT, padx=10)

     split_win.wait_window()


# --- Merge Tables Mapping Window (with Auto-Generation Attempt) ---
def map_merge_tables():
    """Opens a window to merge tables, attempting to auto-generate joins and column maps."""
    parsed_schema = parse_schema_file(schema_file_path)
    if not parsed_schema: messagebox.showerror("Error", "Cannot define merge, schema file empty or invalid."); return

    try: # Get all non-_new DB tables
        cursor.execute("SHOW TABLES")
        all_db_tables = sorted([t[0] for t in cursor.fetchall() if not t[0].endswith("_new")])
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Failed to fetch database tables: {err}"); return

    schema_table_names = sorted(list(parsed_schema.keys()))

    # --- Window Setup ---
    merge_win = Toplevel(root)
    merge_win.title("Merge Multiple Tables into One (Auto-Generate Attempt)")
    center_window(merge_win, 750, 600) # Increased height slightly
    merge_win.transient(root); merge_win.grab_set()

    # --- Helper Functions ---
    def attempt_auto_generation():
        """Tries to guess JOINs and column mappings based on selections."""
        selected_indices = source_listbox.curselection()
        source_tables = [source_listbox.get(i) for i in selected_indices]
        target_table = target_var.get()

        if len(source_tables) < 2 or not target_table:
            messagebox.showinfo("Info", "Select at least 2 source tables and 1 target table first.", parent=merge_win)
            return

        # --- Attempt Auto-Join ---
        generated_joins = []
        source_schemas = {}
        primary_keys = {} # table: pk_column_name

        try:
            print("Fetching schemas for auto-generation...")
            for i, table in enumerate(source_tables):
                schema = get_db_schema(table)
                if not schema: raise ValueError(f"Could not get schema for {table}")
                source_schemas[table] = schema
                # Basic PK detection (id or PRI key)
                pk = None
                if 'id' in schema:
                    pk = 'id'
                else:
                    for col, details in schema.items():
                        if details[3] == 'PRI': # Key column index is 3
                            pk = col
                            break
                primary_keys[table] = pk
                print(f"  - Schema for {table} fetched. Detected PK: {pk}")

            # Simple chained join heuristic (Table1 -> Table2 -> Table3 ...)
            base_table = source_tables[0]
            for i in range(1, len(source_tables)):
                current_table = source_tables[i]
                prev_table = source_tables[i-1]
                prev_pk = primary_keys.get(prev_table)

                # Look for foreign key in current table referencing previous table's PK
                found_join = False
                potential_fk_col = f"{prev_table}_id" # Common convention
                if prev_pk and potential_fk_col in source_schemas[current_table]:
                     generated_joins.append(f"INNER JOIN `{current_table}` ON `{prev_table}`.`{prev_pk}` = `{current_table}`.`{potential_fk_col}`")
                     found_join = True
                     print(f"  - Auto-join found: {prev_table}.{prev_pk} = {current_table}.{potential_fk_col}")

                # Add more heuristics here if needed (e.g., check current_table.id vs prev_table.current_table_id)

                if not found_join:
                    print(f"  - Could not automatically determine join between {prev_table} and {current_table}. Add manually.")
                    # Add a placeholder comment or leave blank? Add comment.
                    generated_joins.append(f"-- Could not auto-determine JOIN for table: {current_table}")


        except Exception as e:
            messagebox.showerror("Auto-Join Error", f"Error during schema fetching or analysis: {e}", parent=merge_win)
            join_text.delete("1.0", tk.END) # Clear on error
            join_text.insert("1.0", "# Error during auto-generation. Please define JOINs manually.")

        join_text.delete("1.0", tk.END)
        if generated_joins:
            join_text.insert("1.0", "\n".join(generated_joins))
            messagebox.showwarning("Verify JOINs", "Auto-generated JOIN conditions are a guess based on common naming conventions. Please VERIFY AND EDIT them carefully!", parent=merge_win)
        else:
             join_text.insert("1.0", "# Could not auto-generate JOINs. Please define manually.")


        # --- Attempt Auto-Column Mapping ---
        target_schema_def = parsed_schema.get(target_table)
        if not target_schema_def:
            messagebox.showerror("Error", f"Target schema '{target_table}' not found.", parent=merge_win)
            return

        generated_col_maps = []
        mapped_target_cols = set() # Track target cols already mapped to prevent duplicates

        print("Attempting auto-column mapping...")
        for target_col in target_schema_def.keys():
             if target_col in mapped_target_cols: continue # Skip if already mapped

             # Find matching column in source tables
             found_source = None
             for src_table in source_tables:
                 if target_col in source_schemas[src_table]:
                     # Basic approach: Map first match found. Ambiguity is possible.
                     found_source = f"`{src_table}`.`{target_col}`"
                     generated_col_maps.append(f"{src_table}.{target_col} -> {target_col}")
                     mapped_target_cols.add(target_col)
                     print(f"  - Auto-map found: {src_table}.{target_col} -> {target_col}")
                     break # Take the first source table that has the column

        map_text.delete("1.0", tk.END)
        if generated_col_maps:
            map_text.insert("1.0", "\n".join(generated_col_maps))
            messagebox.showwarning("Verify Mappings", "Auto-generated column mappings are based on exact name matches. Please VERIFY AND EDIT them carefully, ensuring sources are correct!", parent=merge_win)
        else:
            map_text.insert("1.0", "# Could not auto-generate column maps. Define manually.")


    # --- Widgets ---
    # Source Tables Selection
    source_frame = ttk.LabelFrame(merge_win, text="1. Select Source Tables (DB)", padding=10)
    source_frame.pack(padx=10, pady=5, fill=tk.X)
    source_listbox = Listbox(source_frame, selectmode=tk.MULTIPLE, exportselection=False, height=6)
    source_scrollbar = ttk.Scrollbar(source_frame, orient=tk.VERTICAL, command=source_listbox.yview)
    source_listbox.config(yscrollcommand=source_scrollbar.set)
    for table in all_db_tables: source_listbox.insert(tk.END, table)
    source_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    source_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Target Table Selection
    target_frame = ttk.LabelFrame(merge_win, text="2. Select Target Table (Schema)", padding=10)
    target_frame.pack(padx=10, pady=5, fill=tk.X)
    target_var = StringVar(merge_win)
    target_combo = ttk.Combobox(target_frame, textvariable=target_var, values=schema_table_names, state="readonly", width=40)
    target_combo.pack()
    if schema_table_names: target_combo.current(0)

    # Auto-Generate Button
    auto_gen_button = ttk.Button(merge_win, text="3. Attempt Auto-Generate Joins & Mappings", command=attempt_auto_generation)
    auto_gen_button.pack(padx=10, pady=8)


    # Join Conditions Input
    join_frame = ttk.LabelFrame(merge_win, text="4. Verify/Edit JOIN Conditions (SQL snippet)", padding=10)
    join_frame.pack(padx=10, pady=5, fill=tk.X)
    join_text = Text(join_frame, height=4, wrap=tk.WORD)
    join_text.pack(fill=tk.X)
    join_text.insert("1.0", "# Select source/target tables and click auto-generate, or enter manually.\n# Example: INNER JOIN table2 ON table1.id = table2.t1_id")


    # Column Mappings Input
    map_frame = ttk.LabelFrame(merge_win, text="5. Verify/Edit Column Mappings ('source_table.column -> target_column')", padding=10)
    map_frame.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)
    map_text = Text(map_frame, height=8, wrap=tk.NONE) # Increased height
    map_scroll_y = ttk.Scrollbar(map_frame, orient=tk.VERTICAL, command=map_text.yview)
    map_scroll_x = ttk.Scrollbar(map_frame, orient=tk.HORIZONTAL, command=map_text.xview)
    map_text.config(yscrollcommand=map_scroll_y.set, xscrollcommand=map_scroll_x.set)
    map_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
    map_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
    map_text.pack(fill=tk.BOTH, expand=True)
    map_text.insert("1.0", "# Click auto-generate, or enter manually one per line.")


    # --- Confirmation Logic ---
    def confirm_merge():
        selected_indices = source_listbox.curselection()
        source_tables = [source_listbox.get(i) for i in selected_indices]
        target_table = target_var.get()
        # Get potentially edited joins and maps from text boxes
        join_conditions = join_text.get("1.0", tk.END).strip()
        column_map_lines = map_text.get("1.0", tk.END).strip().split('\n')

        if len(source_tables) < 2: messagebox.showerror("Error", "Select at least two source tables.", parent=merge_win); return
        if not target_table: messagebox.showerror("Error", "Select a target schema table.", parent=merge_win); return
        # Basic validation - more needed for SQL safety
        if not join_conditions or join_conditions.startswith("#"): messagebox.showerror("Error", "Enter valid JOIN conditions.", parent=merge_win); return

        col_mappings_dict = {}
        try:
            for line_num, line in enumerate(column_map_lines, 1):
                clean_line = line.strip()
                if not clean_line or clean_line.startswith("#"): continue # Skip empty/comment lines
                if '->' not in clean_line: raise ValueError(f"Line {line_num}: Missing '->'")
                source_spec, target_col = clean_line.split('->', 1) # Split only on first '->'
                source_spec = source_spec.strip()
                target_col = target_col.strip()
                if '.' not in source_spec: raise ValueError(f"Line {line_num}: Source '{source_spec}' must be 'table.column'")
                # Basic check if source table is selected
                src_table_check = source_spec.split('.')[0]
                if src_table_check not in source_tables: raise ValueError(f"Line {line_num}: Source table '{src_table_check}' not in selected source tables.")
                # Basic check if target col is in schema (optional but good)
                target_schema_def = parsed_schema.get(target_table, {})
                if target_col not in target_schema_def: print(f"Warning: Target column '{target_col}' (Line {line_num}) not found in schema definition for '{target_table}'.")

                col_mappings_dict[source_spec] = target_col

        except Exception as e:
            messagebox.showerror("Mapping Error", f"Invalid column mapping format: {e}\nPlease use 'source_table.column -> target_column', one per line.", parent=merge_win); return
        if not col_mappings_dict: messagebox.showerror("Error", "Define at least one valid column mapping.", parent=merge_win); return

        # Create a unique key for the merge mapping
        merge_key = f"MERGE: {', '.join(sorted(source_tables))} -> {target_table}"

        # Check if this merge conflicts with existing single/split mappings for source tables
        for src in source_tables:
             if src in table_mappings and table_mappings[src].get("type") != "merge":
                 messagebox.showerror("Conflict", f"Source table '{src}' is already mapped individually or in a split. Remove that mapping first.", parent=merge_win)
                 return
             # Also check if target conflicts with other mappings (add if needed)

        # Check if merge key already exists, prompt to overwrite if necessary
        if merge_key in table_mappings:
            if not messagebox.askyesno("Overwrite Merge?", f"A merge mapping '{merge_key}' already exists.\nOverwrite it with these settings?", parent=merge_win):
                return

        table_mappings[merge_key] = {
            "type": "merge",
            "source_tables": sorted(source_tables),
            "new_table_name_schema": target_table,
            "join_conditions": join_conditions, # Save the (potentially edited) joins
            "column_mappings": col_mappings_dict # Save the (potentially edited) maps
        }
        save_mappings(table_mappings)
        messagebox.showinfo("Merge Saved", f"Merge mapping '{merge_key}' saved.\nSelect it in the main list and click 'CREATE' to execute.", parent=merge_win)
        merge_win.destroy(); get_tables(); show_schema()


    def cancel_merge():
         merge_win.destroy()

    # --- Final Buttons ---
    button_frame = ttk.Frame(merge_win); button_frame.pack(pady=15)
    ttk.Button(button_frame, text="Confirm & Save Merge", command=confirm_merge, width=20).pack(side=tk.LEFT, padx=10)
    ttk.Button(button_frame, text="Cancel", command=cancel_merge, width=15).pack(side=tk.LEFT, padx=10)

    merge_win.wait_window()
    
# --- Column Mapping Window ---
def map_columns():
    """Opens a window to manually map columns between a selected old table and its mapped new schema(s)."""
    selected_indices = tables_listbox_old.curselection()
    if not selected_indices: messagebox.showerror("Error", "Select a table or mapping from 'Old/Source Tables' list."); return

    selected_item_text = tables_listbox_old.get(selected_indices)
    mapping_key = selected_item_text
    map_info = table_mappings.get(mapping_key)

    mapping_type = None
    source_table_db = None
    target_schema_names = []
    target_col_defs = {} # {target_schema_name: {col: def_str}}
    current_col_mappings_per_target = {} # {target_schema_name: {old_col: new_col}}

    parsed_schema = parse_schema_file(schema_file_path)

    if selected_item_text.startswith("MERGE:"):
        # Column mapping for Merge needs a dedicated, more complex UI
        messagebox.showinfo("Info", "Column mapping for MERGE is defined within the 'Merge Tables...' window.")
        return
    elif isinstance(map_info, dict):
        mapping_type = map_info.get("type")
        if mapping_type == "single":
            source_table_db = mapping_key
            target_name = map_info.get("new_table_name_schema")
            if target_name: target_schema_names.append(target_name)
            if target_name in parsed_schema: target_col_defs[target_name] = parsed_schema[target_name]
            current_col_mappings_per_target[target_name] = map_info.get("column_mappings", {})
        elif mapping_type == "split":
            source_table_db = mapping_key
            for nt in map_info.get("new_tables", []):
                target_name = nt.get("schema_name")
                if target_name:
                    target_schema_names.append(target_name)
                    if target_name in parsed_schema: target_col_defs[target_name] = parsed_schema[target_name]
                    current_col_mappings_per_target[target_name] = nt.get("column_mappings", {})
        else:
            messagebox.showerror("Error", "Cannot map columns for this mapping type.")
            return
    elif not selected_item_text.startswith("MERGE:"): # Unmapped single table
         # Treat as potential single mapping if schema exists
         if mapping_key in parsed_schema:
             mapping_type = "single"
             source_table_db = mapping_key
             target_schema_names.append(mapping_key)
             target_col_defs[mapping_key] = parsed_schema[mapping_key]
             # Ensure a temp entry exists for saving later if needed
             if mapping_key not in table_mappings:
                 table_mappings[mapping_key] = {"type": "single", "new_table_name_schema": mapping_key, "column_mappings": {}}
             current_col_mappings_per_target[mapping_key] = table_mappings[mapping_key].get("column_mappings", {})
         else:
             messagebox.showerror("Error", f"Table '{mapping_key}' is not mapped and not found in schema."); return
    else:
        messagebox.showerror("Error", f"Cannot map columns for '{selected_item_text}'. Map the table first."); return

    if not source_table_db or not target_schema_names:
        messagebox.showerror("Error", "Could not determine source or target tables for column mapping."); return

    db_schema = get_db_schema(source_table_db)
    if not db_schema: return
    if not all(tname in target_col_defs for tname in target_schema_names):
        missing = [tname for tname in target_schema_names if tname not in target_col_defs]
        messagebox.showerror("Error", f"Schema definition(s) missing for target(s): {', '.join(missing)}"); return

    # --- Column Mapping Window Setup ---
    col_map_win = Toplevel(root)
    col_map_win.title(f"Map Columns: {source_table_db} -> {', '.join(target_schema_names)}")
    center_window(col_map_win, 750, 500)
    col_map_win.transient(root); col_map_win.grab_set()

    # --- Target Selection (if split) ---
    target_frame = ttk.Frame(col_map_win, padding=5)
    target_frame.pack(fill=tk.X)
    ttk.Label(target_frame, text="Target Table:").pack(side=tk.LEFT, padx=5)
    target_var = StringVar(col_map_win)
    target_combo = ttk.Combobox(target_frame, textvariable=target_var, values=target_schema_names, state="readonly", width=30)
    target_combo.pack(side=tk.LEFT, padx=5)
    if target_schema_names: target_var.set(target_schema_names[0])

    # --- Main Mapping Area ---
    main_frame = ttk.Frame(col_map_win, padding=10)
    main_frame.pack(fill=tk.BOTH, expand=True)
    main_frame.columnconfigure(0, weight=1); main_frame.columnconfigure(2, weight=1)
    main_frame.rowconfigure(2, weight=1) # Allow map display to expand

    # Dropdowns for unmapped columns
    left_frame = ttk.Frame(main_frame); left_frame.grid(row=0, column=0, padx=10, pady=5, sticky="nsew")
    db_col_label = ttk.Label(left_frame, text=f"Unmapped Source Columns ({source_table_db}):")
    db_col_label.pack(anchor='w')
    db_col_var = tk.StringVar(col_map_win)
    db_col_combo = ttk.Combobox(left_frame, textvariable=db_col_var, state="readonly", width=35)
    db_col_combo.pack(pady=5, fill='x')

    ttk.Label(main_frame, text="->").grid(row=0, column=1, padx=10, pady=25, sticky='n')

    right_frame = ttk.Frame(main_frame); right_frame.grid(row=0, column=2, padx=10, pady=5, sticky="nsew")
    new_col_label = ttk.Label(right_frame, text="Unmapped Target Columns:")
    new_col_label.pack(anchor='w')
    new_col_var = tk.StringVar(col_map_win)
    new_col_combo = ttk.Combobox(right_frame, textvariable=new_col_var, state="readonly", width=35)
    new_col_combo.pack(pady=5, fill='x')

    # Existing Mappings Display
    existing_map_frame = ttk.LabelFrame(main_frame, text="Existing Column Mappings for Target"); existing_map_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")
    map_text_frame = ttk.Frame(existing_map_frame); map_text_frame.pack(fill=tk.BOTH, expand=True); map_text_frame.rowconfigure(0, weight=1); map_text_frame.columnconfigure(0, weight=1)
    map_text = tk.Text(map_text_frame, height=8, wrap=tk.NONE, bd=0, font=("Courier New", 9)); map_scroll_y = ttk.Scrollbar(map_text_frame, orient=tk.VERTICAL, command=map_text.yview); map_scroll_x = ttk.Scrollbar(map_text_frame, orient=tk.HORIZONTAL, command=map_text.xview); map_text.config(yscrollcommand=map_scroll_y.set, xscrollcommand=map_scroll_x.set)
    map_text.grid(row=0, column=0, sticky='nsew', padx=(5,0), pady=(5,0)); map_scroll_y.grid(row=0, column=1, sticky='ns', padx=(0,5), pady=(5,0)); map_scroll_x.grid(row=1, column=0, sticky='ew', padx=(5,0), pady=(0,5))
    map_text.config(state=tk.DISABLED)

    # --- Actions and Logic ---
    def get_col_name_from_display(display_str): return display_str.split(' (')[0] if display_str else None

    def update_display_for_target(*args):
        current_target_name = target_var.get()
        if not current_target_name: return

        current_col_mappings = current_col_mappings_per_target.get(current_target_name, {})
        new_schema_def = target_col_defs.get(current_target_name, {})

        db_cols = set(db_schema.keys())
        new_cols = set(new_schema_def.keys())
        mapped_db_cols = set(current_col_mappings.keys())
        mapped_new_cols = set(current_col_mappings.values())
        unmapped_db_cols = sorted(list(db_cols - mapped_db_cols))
        unmapped_new_cols = sorted(list(new_cols - mapped_new_cols))

        # Create display lists with types
        unmapped_db_cols_display = [f"{c} ({get_base_type(db_schema[c][1])})" for c in unmapped_db_cols]
        unmapped_new_cols_display = [f"{c} ({get_base_type(new_schema_def[c].split()[0])})" for c in unmapped_new_cols]

        # Update comboboxes
        db_col_combo['values'] = unmapped_db_cols_display
        if unmapped_db_cols_display: db_col_var.set(unmapped_db_cols_display[0])
        else: db_col_var.set("")

        new_col_combo['values'] = unmapped_new_cols_display
        if unmapped_new_cols_display: new_col_var.set(unmapped_new_cols_display[0])
        else: new_col_var.set("")

        # Update labels
        new_col_label.config(text=f"Unmapped Target Columns ({current_target_name}):")
        existing_map_frame.config(text=f"Existing Column Mappings for '{current_target_name}'")

        # Update existing mappings display
        map_text.config(state=tk.NORMAL)
        map_text.delete("1.0", tk.END)
        if current_col_mappings:
            max_old_len = max(len(k) for k in current_col_mappings.keys()) if current_col_mappings else 0
            for old_c, new_c in sorted(current_col_mappings.items()):
                old_type = get_base_type(db_schema.get(old_c, ['','?'])[1])
                new_type = get_base_type(new_schema_def.get(new_c, '?').split()[0])
                map_text.insert(tk.END, f"{old_c:<{max_old_len}} ({old_type:<10}) -> {new_c} ({new_type})\n")
        else: map_text.insert(tk.END, "No specific column mappings defined for this target.")
        map_text.config(state=tk.DISABLED)

        # Update button states
        map_button.config(state=tk.NORMAL if unmapped_db_cols and unmapped_new_cols else tk.DISABLED)
        unmap_button.config(state=tk.NORMAL if current_col_mappings else tk.DISABLED)

    target_var.trace_add("write", update_display_for_target)

    def confirm_col_mapping():
        current_target_name = target_var.get()
        if not current_target_name: return

        selected_old = get_col_name_from_display(db_col_var.get())
        selected_new = get_col_name_from_display(new_col_var.get())
        if not selected_old or not selected_new: messagebox.showerror("Error", "Select one column from each list.", parent=col_map_win); return

        current_col_mappings = current_col_mappings_per_target.setdefault(current_target_name, {})

        # Check conflicts within the current target's mappings
        if selected_old in current_col_mappings: messagebox.showerror("Error", f"Source Column '{selected_old}' already mapped for this target.", parent=col_map_win); return
        if selected_new in current_col_mappings.values(): messagebox.showerror("Error", f"Target Column '{selected_new}' already mapped for this target.", parent=col_map_win); return

        current_col_mappings[selected_old] = selected_new

        # Save back to the main table_mappings structure
        if mapping_type == "single":
             if mapping_key in table_mappings and isinstance(table_mappings[mapping_key], dict):
                 table_mappings[mapping_key]["column_mappings"] = current_col_mappings
        elif mapping_type == "split":
             if mapping_key in table_mappings and isinstance(table_mappings[mapping_key], dict):
                 for nt in table_mappings[mapping_key].get("new_tables", []):
                     if nt.get("schema_name") == current_target_name:
                         nt["column_mappings"] = current_col_mappings
                         break

        save_mappings(table_mappings)
        # messagebox.showinfo("Mapping Saved", f"Mapped: {selected_old} -> {selected_new} for target '{current_target_name}'", parent=col_map_win)
        update_display_for_target() # Refresh display immediately
        # col_map_win.destroy(); show_schema() # Keep window open after map

    def unmap_selected_col():
        current_target_name = target_var.get()
        if not current_target_name: return

        current_col_mappings = current_col_mappings_per_target.get(current_target_name, {})
        selected_old_disp = db_col_var.get() # Get from dropdown or mapped text click
        selected_new_disp = new_col_var.get() # Get from dropdown or mapped text click

        # Try finding the mapping pair based on the currently displayed mapping text
        # This needs a way to select from the Text widget... simpler: use dropdowns if populated, else show error
        selected_old = get_col_name_from_display(selected_old_disp)
        selected_new = get_col_name_from_display(selected_new_disp)

        col_to_unmap_old = None
        col_to_report = None

        # Search in the existing mappings for the target
        for old_c, new_c in current_col_mappings.items():
             # Primarily unmap based on source column if selected in dropdown
             if selected_old and old_c == selected_old:
                 col_to_unmap_old = old_c
                 col_to_report = f"{old_c} -> {new_c}"
                 break
             # Fallback: unmap based on target column if selected in dropdown
             elif selected_new and new_c == selected_new:
                  col_to_unmap_old = old_c
                  col_to_report = f"{old_c} -> {new_c}"
                  break

        if col_to_unmap_old:
             if messagebox.askyesno("Confirm Unmap", f"Remove mapping for target '{current_target_name}':\n{col_to_report}?", parent=col_map_win):
                  del current_col_mappings[col_to_unmap_old]
                  # Save back to main structure (similar logic as confirm_col_mapping)
                  if mapping_type == "single":
                      if mapping_key in table_mappings and isinstance(table_mappings[mapping_key], dict):
                          table_mappings[mapping_key]["column_mappings"] = current_col_mappings
                  elif mapping_type == "split":
                      if mapping_key in table_mappings and isinstance(table_mappings[mapping_key], dict):
                          for nt in table_mappings[mapping_key].get("new_tables", []):
                              if nt.get("schema_name") == current_target_name:
                                  nt["column_mappings"] = current_col_mappings
                                  break
                  save_mappings(table_mappings)
                  # messagebox.showinfo("Mapping Removed", f"Removed mapping: {col_to_report}", parent=col_map_win)
                  update_display_for_target() # Refresh display immediately
                  # col_map_win.destroy(); show_schema() # Keep window open
        else: messagebox.showerror("Error", "Could not identify mapping to remove based on current selection.", parent=col_map_win)

    # --- Buttons ---
    button_frame = ttk.Frame(main_frame); button_frame.grid(row=1, column=0, columnspan=3, pady=15)
    map_button = ttk.Button(button_frame, text="Confirm Column Map", command=confirm_col_mapping, width=20)
    map_button.pack(side=tk.LEFT, padx=10); map_button.config(state=tk.DISABLED) # Initial state
    unmap_button = ttk.Button(button_frame, text="Unmap Selected Column", command=unmap_selected_col, width=20)
    unmap_button.pack(side=tk.LEFT, padx=10); unmap_button.config(state=tk.DISABLED) # Initial state
    ttk.Button(button_frame, text="Close", command=lambda: [col_map_win.destroy(), show_schema()], width=10).pack(side=tk.LEFT, padx=10)

    update_display_for_target() # Initial population
    col_map_win.wait_window()

# --- Manual Script Generation ---
def generate_manual_script():
    """Generates a more complete, runnable Python script template for manual data migration."""
    selected_indices = tables_listbox_old.curselection()
    if not selected_indices:
        messagebox.showerror("Error", "Select a table from the 'Old/Source Tables' list first.")
        return
    selected_item_text = tables_listbox_old.get(selected_indices)

    if selected_item_text.startswith("MERGE:"):
        messagebox.showinfo("Info", "Manual script generation is not currently supported for MERGE mappings.")
        return

    old_table_name = selected_item_text
    map_info = table_mappings.get(old_table_name)
    mapping_type = None
    target_schema_name = None
    all_target_schema_names = []
    new_schema_def = None

    parsed_schema = parse_schema_file(schema_file_path)

    # Determine mapping and target schema (using first target for split)
    if isinstance(map_info, dict):
        mapping_type = map_info.get("type")
        if mapping_type == "single":
            target_schema_name = map_info.get("new_table_name_schema")
            if target_schema_name: all_target_schema_names.append(target_schema_name)
        elif mapping_type == "split":
            targets = map_info.get("new_tables", [])
            if targets:
                target_schema_name = targets[0].get("schema_name")
                all_target_schema_names = [nt.get("schema_name") for nt in targets if nt.get("schema_name")]
            else:
                 messagebox.showerror("Error", f"Split mapping for '{old_table_name}' has no defined targets."); return
    elif old_table_name in parsed_schema:
         mapping_type = "single"
         target_schema_name = old_table_name
         all_target_schema_names.append(target_schema_name)
    else:
         messagebox.showerror("Error", f"Table '{old_table_name}' is not mapped and has no corresponding schema definition."); return

    if not target_schema_name:
         messagebox.showerror("Error", f"Could not determine a target schema name for '{old_table_name}'."); return

    if target_schema_name not in parsed_schema:
         messagebox.showerror("Error", f"Schema definition for target '{target_schema_name}' not found."); return
    new_schema_def = parsed_schema[target_schema_name]
    new_db_table_name = f"{target_schema_name}_new"


    # --- Fetch Information ---
    old_schema_dict = None
    primary_key_col = None
    try:
        if not conn or not conn.is_connected(): raise mysql.connector.Error("Not connected")
        old_schema_dict = get_db_schema(old_table_name)
        if not old_schema_dict: raise ValueError("Failed to fetch old schema")

        old_schema_lines = []
        for col, details in old_schema_dict.items():
            old_schema_lines.append(f"#   - {col}: {details[1]}")
            if details[3] == 'PRI': primary_key_col = col

        sample_data = []
        sample_columns = []
        try:
            cursor.execute(f"SELECT * FROM `{old_table_name}` LIMIT 2")
            sample_columns = [desc[0] for desc in cursor.description]
            sample_data = cursor.fetchall()
        except Exception as data_err:
             print(f"Warning: Could not fetch sample data for '{old_table_name}': {data_err}")

        sample_data_lines = []
        if sample_data:
             sample_data_lines.append(f"#   Columns: {', '.join(sample_columns)}")
             for i, row in enumerate(sample_data):
                 row_str = [repr(val) for val in row]
                 sample_data_lines.append(f"#   Row {i+1}: ({', '.join(row_str)})")
        else: sample_data_lines.append("#   (Could not fetch sample data)")

        new_schema_lines = [f"#   - {col}: {definition}" for col, definition in new_schema_def.items()]

    except Exception as e:
        messagebox.showerror("DB Error", f"Failed to get schema/data for script generation: {e}")
        return

    # --- Generate Code Components ---
    create_table_sql_str = generate_create_statement(new_db_table_name, new_schema_def)
    new_cols_for_insert = list(new_schema_def.keys())
    cols_sql_str = ", ".join([f"`{col}`" for col in new_cols_for_insert])
    placeholders_sql_str = ", ".join(["%s"] * len(new_cols_for_insert))

    # Generate mapping lines WITHOUT leading indentation
    data_mapping_lines = []
    for col in new_cols_for_insert:
        source_col_expr = f"old_row.get('{col}')" if col in old_schema_dict else "None # TODO: Assign correct source or default value"
        data_mapping_lines.append(f"new_data_dict['{col}'] = {source_col_expr}") # NO LEADING SPACES
        if col in old_schema_dict:
            old_type = get_base_type(old_schema_dict[col][1])
            new_type = get_base_type(new_schema_def[col].split()[0])
            if old_type != new_type:
                 data_mapping_lines.append(f"# Warning: Type mismatch ({old_type} -> {new_type}). Ensure proper conversion for '{col}'.")
        else:
            data_mapping_lines.append(f"# Info: Column '{col}' not found in source table '{old_table_name}'. Ensure appropriate value is assigned.")

    data_mapping_code_raw = "\n".join(data_mapping_lines)
    # Define the indentation prefix required inside the target 'try' block's transformation section (16 spaces)
    indentation_prefix = ' ' * 16
    # Apply this prefix to every line of the generated mapping code
    indented_data_mapping_code = textwrap.indent(data_mapping_code_raw, indentation_prefix)


    # --- Generate Script Content ---
    # Template uses standard 4-space indentation
    script_content = f"""\
# Manual Migration Script for: {old_table_name} -> {new_db_table_name}
# Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#
# This script provides a runnable template for manually migrating data.
# It includes generated CREATE TABLE and INSERT structures based on schemas.
#
# !!! REVIEW AND EDIT LOGIC (especially // TODO comments) BEFORE RUNNING !!!
# !!! ENSURE DATABASE BACKUPS ARE IN PLACE !!!
#

import mysql.connector
import datetime
import decimal
import sys

# --- Database Connection Details (Update these) ---
DB_CONFIG = {{
    'user': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'host': 'localhost',
    'database': '{current_db_name}',
    'charset': 'utf8mb4',
    'raise_on_warnings': False
}}

# --- Schema Information (for reference) ---
#
# Old Table: {old_table_name}
# Old Schema:
{chr(10).join(old_schema_lines)}
#
# Sample Data (Max 2 Rows):
{chr(10).join(sample_data_lines)}
#
# Target Table Name (in DB): {new_db_table_name}
# Target Schema Definition ('{target_schema_name}' from schema file):
{chr(10).join(new_schema_lines)}
"""

    if mapping_type == "split":
        script_content += f"""\
#
# Note: This script targets the first table ('{target_schema_name}') of a SPLIT mapping.
# Other target tables in this split: {', '.join(t for t in all_target_schema_names if t != target_schema_name)}
# You may need to adapt or duplicate this script for other targets.
"""

    # Use {{ }} for literal braces in the generated code's f-strings
    # Use {indented_data_mapping_code} where the correctly indented block goes
    script_content += f"""\

# --- Migration Logic ---

def migrate_data():
    conn = None
    cursor = None
    rows_processed = 0
    rows_inserted = 0
    rows_failed = 0

    try:
        print("Connecting to database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True) # Use dictionary cursor for name access
        print("Connected.")

        # 1. Create the New Table (if it doesn't exist)
        create_table_sql = \"\"\"
{create_table_sql_str}
\"\"\"
        print(f"Ensuring target table `{new_db_table_name}` exists...")
        try:
            cursor.execute(create_table_sql)
            print("Table check/creation complete.")
        except mysql.connector.Error as create_err:
            if create_err.errno == 1050: # Table already exists
                print(f"Table `{new_db_table_name}` already exists.")
            else:
                print(f"ERROR creating table: {{create_err}}") # Escaped brace
                print("SQL:", create_table_sql)
                raise

        # 2. Select Data from the Old Table
        select_sql = "SELECT * FROM `{old_table_name}`;"
        print(f"Executing: {{select_sql}}") # Escaped brace
        cursor.execute(select_sql)
        old_data_rows = cursor.fetchall()
        print(f"Fetched {{len(old_data_rows)}} rows from `{old_table_name}`.") # Escaped brace

        # 3. Process and Insert Data into the New Table
        print(f"Processing and inserting data into `{new_db_table_name}`...") # Escaped brace

        insert_sql_template = "INSERT INTO `{new_db_table_name}` ({cols_sql_str}) VALUES ({placeholders_sql_str});"
        target_columns_ordered = {new_cols_for_insert!r}

        for old_row in old_data_rows:
            rows_processed += 1
            source_row_id_for_log = old_row.get('{primary_key_col or "id"}', 'N/A')

            try:
                # --- Data Transformation ---
                new_data_dict = {{}} # Indentation level: 16 spaces

                # TODO: REVIEW AND MODIFY THE MAPPINGS AND TRANSFORMATIONS BELOW!
                # This section provides a basic structure. You MUST handle type
                # conversions, default values, data cleaning, lookups etc.
{indented_data_mapping_code} # This block starts at 16 spaces

                # --- Execute INSERT ---
                # Create tuple in the correct order defined by target_columns_ordered
                values_tuple = tuple(new_data_dict.get(col) for col in target_columns_ordered) # Indentation level: 16 spaces

                cursor.execute(insert_sql_template, values_tuple) # Indentation level: 16 spaces
                rows_inserted += 1 # Indentation level: 16 spaces

            except mysql.connector.Error as insert_err: # Indentation level: 12 spaces
                rows_failed += 1
                print(f"Error inserting row {{rows_processed}} (Source ID: {{source_row_id_for_log}}): {{insert_err}}", file=sys.stderr)
            except Exception as transform_err: # Indentation level: 12 spaces
                rows_failed += 1
                print(f"Error transforming row {{rows_processed}} (Source ID: {{source_row_id_for_log}}): {{transform_err}}", file=sys.stderr)

        # 4. Final Commit (Indentation level: 8 spaces)
        print("Committing final transaction...")
        conn.commit()
        print("Migration process finished.")

    except mysql.connector.Error as err: # Indentation level: 4 spaces
        print(f"Database Error: {{err}}", file=sys.stderr)
        if conn: conn.rollback()
    except Exception as e: # Indentation level: 4 spaces
        print(f"An unexpected error occurred: {{e}}", file=sys.stderr)
        if conn: conn.rollback()
    finally: # Indentation level: 4 spaces
        if cursor: cursor.close()
        if conn: conn.close()
        print("Database connection closed.")
        print(f"Summary: Processed={{rows_processed}}, Inserted={{rows_inserted}}, Failed={{rows_failed}}")

# --- Run the Migration ---
if __name__ == "__main__": # Indentation level: 0
    db_name_for_prompt = DB_CONFIG.get('database', 'UNKNOWN_DATABASE') # Indentation level: 4 spaces
    confirm = input(f"This script will attempt to migrate data from '{old_table_name}' to '{new_db_table_name}' in database '{{db_name_for_prompt}}'.\\n"
                    f"!!! ENSURE YOU HAVE REVIEWED/EDITED THE SCRIPT AND BACKED UP YOUR DATA !!!\\n"
                    f"Type 'yes' to proceed: ")
    if confirm.lower() == 'yes':
        migrate_data()
    else:
        print("Migration aborted by user.")

"""

    # --- Write to File ---
    filename = f"manual_migration_{old_table_name}_to_{target_schema_name}.py"
    filepath = os.path.join(os.getcwd(), filename)

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(script_content)
        messagebox.showinfo("Script Generated", f"Manual migration script template saved as:\n{filepath}\n\nPlease review and edit it carefully before running.")
    except IOError as e:
        messagebox.showerror("File Error", f"Could not save script to '{filepath}':\n{e}")
    except Exception as e:
        messagebox.showerror("Error", f"An unexpected error occurred while generating the script: {e}")

# --- Application Cleanup ---
def close_app(window):
    """Closes DB connection gracefully and destroys the window."""
    print("Closing application...")
    try:
        if 'cursor' in globals() and cursor: cursor.close()
        if 'conn' in globals() and conn and conn.is_connected(): conn.close(); print("Database connection closed.")
    except Exception as e: print(f"Error during DB cleanup: {e}")
    finally:
        if window: window.destroy()
        print("Application window destroyed.")


# --- GUI Initialization ---
def init_main_window():
    """Initializes the main application window and its widgets."""
    global root, tables_listbox_old, tables_listbox_new, schema_tree_old, schema_tree_new
    global db_combobox, schema_file_entry, schema_file_path, table_mappings
    global db_label, tables_frame, db_select_frame, current_db_name
    global constraint_vars, create_button
    global schema_frame_old, schema_frame_new

    root = tk.Tk()
    root.title("Database Schema Migration Tool")
    center_window(root, 1350, 750)
    root.minsize(1200, 650)

    current_db_name = None
    table_mappings = {}
    schema_file_path = ""

    style = ttk.Style(); # style.theme_use('clam')

    # --- Top Frame ---
    db_select_frame = ttk.Frame(root, padding=10)
    db_select_frame.pack(fill=tk.X, padx=10, pady=(10,0))
    db_select_frame.columnconfigure(1, weight=1)
    ttk.Label(db_select_frame, text="Database:").grid(row=0, column=0, sticky="w", padx=(0,5), pady=2)
    db_combobox = ttk.Combobox(db_select_frame, width=40, state="readonly")
    db_combobox.grid(row=0, column=1, sticky="ew", padx=5, pady=2)
    ttk.Label(db_select_frame, text="Schema File:").grid(row=1, column=0, sticky="w", padx=(0,5), pady=2)
    schema_file_entry = ttk.Entry(db_select_frame, width=60)
    schema_file_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=2)
    browse_button = ttk.Button(db_select_frame, text="Browse...", command=browse_file, width=10)
    browse_button.grid(row=1, column=2, padx=(5,10), pady=2)
    connect_button = ttk.Button(db_select_frame, text="Load DB & Schema", command=select_database, width=20)
    connect_button.grid(row=2, column=1, columnspan=3, padx=5, pady=(8,5), sticky='e')

    # --- Status Label ---
    db_label = ttk.Label(root, text="Status: Not connected.", font=("Arial", 10), anchor='w', relief=tk.SUNKEN, padding=3)
    db_label.pack(pady=(2, 5), padx=10, fill=tk.X)

    # --- Main Content Frame ---
    tables_frame = ttk.Frame(root) # Packed later
    tables_frame.columnconfigure(1, weight=4, minsize=400) # Schema Trees
    tables_frame.columnconfigure(0, weight=1, minsize=250) # Table Lists
    tables_frame.columnconfigure(2, weight=1, minsize=250) # Actions
    tables_frame.rowconfigure(0, weight=1)

    # --- Left Frame (Table Lists) ---
    left_frame = ttk.Frame(tables_frame); left_frame.grid(row=0, column=0, padx=(10,5), pady=10, sticky="nsew"); left_frame.rowconfigure(1, weight=1); left_frame.rowconfigure(3, weight=1); left_frame.columnconfigure(0, weight=1)
    ttk.Label(left_frame, text="DB Tables (Old/Source):").grid(row=0, column=0, sticky="w", pady=(0,2))
    listbox_old_frame = ttk.Frame(left_frame); listbox_old_frame.grid(row=1, column=0, sticky="nsew"); listbox_old_frame.rowconfigure(0, weight=1); listbox_old_frame.columnconfigure(0, weight=1)
    tables_listbox_old = tk.Listbox(listbox_old_frame, width=35, exportselection=False, font=("Courier New", 9)); tables_listbox_old.grid(row=0, column=0, sticky="nsew"); scrollbar_old_y = ttk.Scrollbar(listbox_old_frame, orient=tk.VERTICAL, command=tables_listbox_old.yview); scrollbar_old_y.grid(row=0, column=1, sticky="ns"); tables_listbox_old.config(yscrollcommand=scrollbar_old_y.set);
    tables_listbox_old.bind("<<ListboxSelect>>", show_schema) # Single click shows schema
    tables_listbox_old.bind("<Double-Button-1>", lambda e: view_old_data()) # Double click shows data

    ttk.Label(left_frame, text="DB Tables (_new/Generated):").grid(row=2, column=0, sticky="w", pady=(5,2))
    listbox_new_frame = ttk.Frame(left_frame); listbox_new_frame.grid(row=3, column=0, sticky="nsew"); listbox_new_frame.rowconfigure(0, weight=1); listbox_new_frame.columnconfigure(0, weight=1)
    tables_listbox_new = tk.Listbox(listbox_new_frame, width=35, exportselection=False, font=("Courier New", 9)); tables_listbox_new.grid(row=0, column=0, sticky="nsew"); scrollbar_new_y = ttk.Scrollbar(listbox_new_frame, orient=tk.VERTICAL, command=tables_listbox_new.yview); scrollbar_new_y.grid(row=0, column=1, sticky="ns"); tables_listbox_new.config(yscrollcommand=scrollbar_new_y.set);
    tables_listbox_new.bind("<Double-Button-1>", lambda e: view_new_data()) # Double click shows data

    # --- Middle Frame (Schema Trees) ---
    middle_frame = ttk.Frame(tables_frame); middle_frame.grid(row=0, column=1, padx=5, pady=10, sticky="nsew"); middle_frame.rowconfigure(0, weight=1); middle_frame.rowconfigure(2, weight=1); middle_frame.columnconfigure(0, weight=1)
    schema_frame_old = ttk.LabelFrame(middle_frame, text="Original Schema (from DB)"); schema_frame_old.grid(row=0, column=0, sticky="nsew", pady=(0,5)); schema_frame_old.rowconfigure(0, weight=1); schema_frame_old.columnconfigure(0, weight=1)
    columns = ("Field", "Type", "Null", "Key", "Default", "Extra"); schema_tree_old = ttk.Treeview(schema_frame_old, columns=columns, show="headings", height=8); schema_tree_old.grid(row=0, column=0, sticky="nsew"); tree_scroll_old_y = ttk.Scrollbar(schema_frame_old, orient="vertical", command=schema_tree_old.yview); tree_scroll_old_y.grid(row=0, column=1, sticky="ns"); tree_scroll_old_x = ttk.Scrollbar(schema_frame_old, orient="horizontal", command=schema_tree_old.xview); tree_scroll_old_x.grid(row=1, column=0, sticky="ew"); schema_tree_old.configure(yscrollcommand=tree_scroll_old_y.set, xscrollcommand=tree_scroll_old_x.set)
    for i, col in enumerate(columns): width = 150 if i == 1 else (60 if i == 2 else (50 if i==3 else 100)); schema_tree_old.heading(col, text=col, anchor='w'); schema_tree_old.column(col, width=width, anchor='w', stretch=True)
    schema_frame_new = ttk.LabelFrame(middle_frame, text="New Schema (from File)"); schema_frame_new.grid(row=2, column=0, sticky="nsew", pady=(5,0)); schema_frame_new.rowconfigure(0, weight=1); schema_frame_new.columnconfigure(0, weight=1)
    schema_tree_new = ttk.Treeview(schema_frame_new, columns=columns, show="headings", height=8); schema_tree_new.grid(row=0, column=0, sticky="nsew"); tree_scroll_new_y = ttk.Scrollbar(schema_frame_new, orient="vertical", command=schema_tree_new.yview); tree_scroll_new_y.grid(row=0, column=1, sticky="ns"); tree_scroll_new_x = ttk.Scrollbar(schema_frame_new, orient="horizontal", command=schema_tree_new.xview); tree_scroll_new_x.grid(row=1, column=0, sticky="ew"); schema_tree_new.configure(yscrollcommand=tree_scroll_new_y.set, xscrollcommand=tree_scroll_new_x.set)
    for i, col in enumerate(columns): width = 150 if i == 1 else (60 if i == 2 else (50 if i==3 else 100)); schema_tree_new.heading(col, text=col, anchor='w'); schema_tree_new.column(col, width=width, anchor='w', stretch=True)

    # --- Right Frame (Actions & Confirmation) ---
    right_frame = ttk.Frame(tables_frame); right_frame.grid(row=0, column=2, padx=(5,10), pady=10, sticky="nsew"); right_frame.columnconfigure(0, weight=1)
    action_frame = ttk.LabelFrame(right_frame, text="Actions", padding=10); action_frame.pack(pady=(0,10), fill=tk.X)

    # Mapping Buttons Frame
    map_button_frame = ttk.Frame(action_frame); map_button_frame.pack(fill=tk.X)
    ttk.Button(map_button_frame, text="Map Table (Single)", command=map_tables).pack(pady=2, fill=tk.X)
    ttk.Button(map_button_frame, text="Split Table...", command=map_split_table).pack(pady=2, fill=tk.X)
    ttk.Button(map_button_frame, text="Merge Tables...", command=map_merge_tables).pack(pady=2, fill=tk.X)
    ttk.Button(map_button_frame, text="Map Columns...", command=map_columns).pack(pady=2, fill=tk.X)

    # View/Refresh/Generate/Help Buttons Frame
    view_button_frame = ttk.Frame(action_frame); view_button_frame.pack(fill=tk.X, pady=(10,0)) # Frame to group these buttons
    ttk.Button(view_button_frame, text="View Old Table Data", command=view_old_data).pack(pady=2, fill=tk.X)
    ttk.Button(view_button_frame, text="View New Table Data", command=view_new_data).pack(pady=2, fill=tk.X)
    ttk.Button(view_button_frame, text="Refresh Tables & Schema", command=select_database).pack(pady=2, fill=tk.X)
    ttk.Button(view_button_frame, text="Generate Manual Script...", command=generate_manual_script).pack(pady=(10, 2), fill=tk.X) # Packed into view_button_frame
    ttk.Button(view_button_frame, text="Help", command=show_help).pack(pady=(2, 2), fill=tk.X) # Packed into view_button_frame after Generate

    # Confirmation Gate Frame
    constraint_frame = ttk.LabelFrame(right_frame, text="Manual Confirmation Gate", padding=10); constraint_frame.pack(pady=10, fill=tk.X)
    constraints = ["Compared Schemas?", "Checked Data Types?", "Verified Mappings?", "Database Backed Up?", "Aware of Potential Loss?", "Proceed with Create?"]
    constraint_vars = [tk.BooleanVar() for _ in constraints]
    for i, constraint in enumerate(constraints): cb = ttk.Checkbutton(constraint_frame, text=constraint, variable=constraint_vars[i], command=check_constraints); cb.grid(row=i, column=0, sticky="w", pady=1, padx=5)

    # Create Button
    ttk.Separator(right_frame, orient=tk.HORIZONTAL).pack(pady=10, fill=tk.X)
    create_button = ttk.Button(right_frame, text="CREATE New Table(s) & Copy Data", command=create_new_table_and_copy_data, state=tk.DISABLED); create_button.pack(pady=10, ipady=5, fill=tk.X)

    root.protocol("WM_DELETE_WINDOW", lambda: close_app(root))
    populate_db_combobox()
    root.mainloop()


# --- Login Window Setup ---
login_window = tk.Tk()
login_window.title("Database Login")
center_window(login_window, 300, 200)
login_window.resizable(False, False)

ttk.Label(login_window, text="MySQL Login", font=("Arial", 12, "bold")).pack(pady=(10, 5))
ttk.Label(login_window, text="Username:").pack(pady=(5,0))
username_entry = ttk.Entry(login_window, width=30); username_entry.pack(pady=2); username_entry.insert(0, "root") # Set default username
ttk.Label(login_window, text="Password:").pack(pady=(5,0))
password_entry = ttk.Entry(login_window, show="*", width=30); password_entry.pack(pady=2)
connect_button_login = ttk.Button(login_window, text="Connect", command=connect_db); connect_button_login.pack(pady=15)
password_entry.bind("<Return>", lambda event=None: connect_button_login.invoke()) # Allow Enter key
username_entry.focus()

# --- Global Placeholders ---
conn, cursor, root = None, None, None
tables_listbox_old, tables_listbox_new = None, None
schema_tree_old, schema_tree_new = None, None
schema_frame_old, schema_frame_new = None, None # Added these globals
db_combobox, schema_file_entry = None, None
db_label, tables_frame, db_select_frame = None, None, None
create_button = None
schema_file_path, current_db_name = "", None
table_mappings = {}
constraint_vars = []

login_window.mainloop()