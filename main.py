import tkinter as tk
import csv
from tkinter import ttk, messagebox, filedialog, Toplevel, Scrollbar, Text, END
import mysql.connector
import re
import json
import os
import datetime
import decimal

MAPPING_FILE = "table_mappings.json"

# --- Mapping File Handling ---

def load_mappings():
    """Loads table and column mappings from the JSON file."""
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, "r", encoding='utf-8') as f:
            try:
                mappings = json.load(f)
                # Ensure nested structure exists for compatibility
                for old_table, mapping_data in list(mappings.items()): # Use list to iterate over copy
                    if isinstance(mapping_data, str): # Old format
                        mappings[old_table] = {
                            "new_table_name_schema": mapping_data,
                            "column_mappings": {}
                        }
                    elif isinstance(mapping_data, dict):
                         if "new_table_name_schema" not in mapping_data:
                              mappings[old_table]["new_table_name_schema"] = old_table
                         if "column_mappings" not in mapping_data:
                              mappings[old_table]["column_mappings"] = {}
                    else: # Handle unexpected data type
                        print(f"Warning: Invalid mapping format found for '{old_table}' in {MAPPING_FILE}. Removing entry.")
                        del mappings[old_table]
                return mappings
            except json.JSONDecodeError:
                messagebox.showerror("Mapping Error", f"Could not decode {MAPPING_FILE}. Starting with empty mappings.")
                return {}
            except Exception as e:
                 messagebox.showerror("Mapping Error", f"Error loading mappings from {MAPPING_FILE}: {e}")
                 return {}
    else:
        # Create file if it doesn't exist
        try:
            with open(MAPPING_FILE, "w", encoding='utf-8') as f:
                json.dump({}, f)
            return {}
        except IOError as e:
             messagebox.showerror("File Error", f"Could not create mapping file {MAPPING_FILE}: {e}")
             return {}


def save_mappings(mappings):
    """Saves the current table and column mappings to the JSON file."""
    try:
        with open(MAPPING_FILE, "w", encoding='utf-8') as f:
            json.dump(mappings, f, indent=4)
    except IOError as e:
         messagebox.showerror("File Error", f"Could not save mapping file {MAPPING_FILE}: {e}")
    except TypeError as e:
         messagebox.showerror("Save Error", f"Data structure cannot be saved as JSON: {e}\nMappings: {mappings}")


# --- GUI Utilities ---

def center_window(window, width=600, height=400):
    """Centers a Tkinter window on the screen."""
    window.update_idletasks()
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    x = max(0, (screen_width - width) // 2)
    y = max(0, (screen_height - height) // 2)
    window.geometry(f"{width}x{height}+{x}+{y}")

# --- Database Connection ---

def connect_db():
    """Connects to the MySQL database using credentials from the login window."""
    global conn, cursor
    username = username_entry.get()
    password = password_entry.get()
    if not username:
         messagebox.showerror("Login Error", "Please enter a username.", parent=login_window)
         return
    try:
        conn = mysql.connector.connect(
            host="localhost", # Consider making host configurable
            user=username,
            password=password,
            charset="utf8mb4",
            get_warnings=True
        )
        cursor = conn.cursor()
        login_window.destroy()
        init_main_window()
    except mysql.connector.Error as err:
        messagebox.showerror("Connection Error", f"Failed to connect: {err}", parent=login_window)

# --- Main Window Logic ---

def populate_db_combobox():
    """Populates the database selection combobox, filtering system DBs."""
    if not conn or not conn.is_connected():
         return
    try:
        global cursor, db_combobox
        cursor.execute("SHOW DATABASES")
        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys'}
        databases = [db[0] for db in cursor.fetchall() if db[0] not in system_dbs]
        db_combobox['values'] = databases
        if databases:
            db_combobox.current(0)
        else:
             db_combobox['values'] = []
             db_combobox.set('')
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Failed to fetch databases: {err}")


def select_database():
    """Selects the database, loads schema file, performs auto-mapping, and populates table lists."""
    global conn, cursor, schema_file_path, table_mappings, current_db_name
    dbname = db_combobox.get()
    schema_file_path_val = schema_file_entry.get()

    if not dbname:
         messagebox.showerror("Error", "Please select a database from the list.")
         return
    if not schema_file_path_val:
        messagebox.showerror("Error", "Please provide the path to the schema definition file.")
        return
    if not os.path.isfile(schema_file_path_val):
         messagebox.showerror("Error", f"Schema file not found or is not a file:\n{schema_file_path_val}")
         return

    schema_file_path = schema_file_path_val

    try:
        if not conn or not conn.is_connected():
             messagebox.showerror("Connection Error", "Database connection lost. Please reconnect.")
             return

        cursor.execute(f"USE `{dbname}`")
        conn.commit()
        current_db_name = dbname
        db_label.config(text=f"Status: Connected to '{dbname}'. Schema: '{os.path.basename(schema_file_path)}'")
        tables_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0,10))

        table_mappings = load_mappings()
        parsed_schema = parse_schema_file(schema_file_path)
        if not parsed_schema and os.path.exists(schema_file_path): # Only warn if file exists but is empty/invalid
            messagebox.showwarning("Schema Warning", f"Schema file '{os.path.basename(schema_file_path)}' was parsed, but no table definitions were found.")

        auto_map_tables_and_columns(parsed_schema)
        get_tables()
        if 'constraint_vars' in globals() and constraint_vars:
            for var in constraint_vars:
                var.set(False)
        check_constraints()

    except mysql.connector.Error as err:
        messagebox.showerror("Database Error", f"Failed to select database '{dbname}': {err}")
        db_label.config(text="Status: Connection Error. Please try again.")
        current_db_name = None
        tables_frame.pack_forget()
    except Exception as e:
        messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        db_label.config(text="Status: An error occurred.")
        current_db_name = None
        tables_frame.pack_forget()


def get_tables():
    """Fetches tables from the DB and populates the 'Old' and 'New' listboxes with appropriate colors."""
    global cursor, conn, tables_listbox_old, tables_listbox_new, schema_file_path, table_mappings, current_db_name

    tables_listbox_old.delete(0, tk.END)
    tables_listbox_new.delete(0, tk.END)
    for row in schema_tree_old.get_children(): schema_tree_old.delete(row)
    for row in schema_tree_new.get_children(): schema_tree_new.delete(row)

    if not conn or not conn.is_connected() or not current_db_name:
        return

    try:
         cursor.execute("SHOW TABLES")
         all_tables_in_db = {table[0] for table in cursor.fetchall()} # Use set for faster lookups
    except mysql.connector.Error as err:
         messagebox.showerror("Error", f"Failed to fetch tables for database '{current_db_name}': {err}")
         return

    parsed_schema = parse_schema_file(schema_file_path)

    # Populate Old Tables List
    for table_db in sorted(list(all_tables_in_db)): # Sort for consistent order
        if table_db.endswith("_new"):
             continue

        index = tables_listbox_old.size()
        tables_listbox_old.insert(tk.END, table_db)

        mapping_info = table_mappings.get(table_db)
        new_table_name_in_schema = None
        is_mapped = False
        if isinstance(mapping_info, dict):
            new_table_name_in_schema = mapping_info.get("new_table_name_schema")
            is_mapped = True

        corresponding_new_table_db = f"{new_table_name_in_schema}_new" if new_table_name_in_schema else f"{table_db}_new"
        new_db_table_exists = corresponding_new_table_db in all_tables_in_db

        # Determine color based on mapping and schema presence
        color = 'black' # Default/Processed
        if not is_mapped:
            color = 'orange' if table_db in parsed_schema else 'red'
        else:
            if not new_table_name_in_schema or new_table_name_in_schema not in parsed_schema:
                color = 'purple'
            elif new_db_table_exists:
                color = 'black'
            else:
                color = 'blue'
        tables_listbox_old.itemconfig(index, {'fg': color})

    # Populate New Tables List
    for table_db in sorted(list(all_tables_in_db)):
        if table_db.endswith("_new"):
            tables_listbox_new.insert(tk.END, table_db)
            base_name = table_db[:-4]
            is_base_in_schema = base_name in parsed_schema
            is_base_mapped = any(isinstance(m, dict) and m.get("new_table_name_schema") == base_name for m in table_mappings.values())

            new_color = 'black'
            if not is_base_in_schema and not is_base_mapped:
                 new_color = 'grey'
            new_index = tables_listbox_new.size() - 1
            tables_listbox_new.itemconfig(new_index, {'fg': new_color})

    root.update_idletasks()


# --- Constraint Check Logic (Manual Gate) ---
def check_constraints(*args):
    """Enables/disables the Create button based on all checkboxes being checked."""
    all_checked = False
    if 'constraint_vars' in globals() and constraint_vars and 'create_button' in globals() and create_button:
        all_checked = all(var.get() for var in constraint_vars)
        create_button.config(state=tk.NORMAL if all_checked else tk.DISABLED)
    return all_checked


# --- Help Window ---
def show_help():
    """Displays a help window explaining the tool's usage."""
    help_win = Toplevel(root)
    help_win.title("Help - Schema Migration Tool")
    center_window(help_win, 750, 600)
    help_win.transient(root)

    help_text_widget = Text(help_win, wrap=tk.WORD, padx=10, pady=10, bd=0, font=("Arial", 10), relief=tk.FLAT)
    help_scroll = ttk.Scrollbar(help_win, command=help_text_widget.yview)
    help_text_widget.config(yscrollcommand=help_scroll.set)

    help_scroll.pack(side=tk.RIGHT, fill=tk.Y)
    help_text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Define Help Text Tags
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
        "list_item": {"lmargin1": 10, "lmargin2": 25}
    }
    for name, config in tags.items():
        help_text_widget.tag_configure(name, **config)

    # Insert Content (Structure from previous thought process)
    help_content = [
        ("Schema Migration Tool - Help\n", "h1"),
        ("Purpose:\n", "h2"),
        ("This tool assists in migrating MySQL database schemas. It allows you to compare existing table structures against a new schema defined in a text file, visualize differences, create new tables based on the file, and copy data from the old tables to the new ones.\n\n", ""),
        ("Workflow:\n", "h2"),
        ("1.  Login:\n", ("bold", "list_item")),
        ("    Connect to your MySQL database using the initial login window.\n", "list_item"),
        ("2.  Load DB & Schema:\n", ("bold", "list_item")),
        ("    Select the target database from the dropdown list.\n    Browse to and select your schema definition file (.txt, .sql).\n    Click 'Load DB & Schema'. This populates the table lists.\n", "list_item"),
        ("3.  Review & Map Tables:\n", ("bold", "list_item")),
        ("    Examine the 'DB Tables (Old/Source)' list. See 'List Colors' below.\n    Use the 'Map Tables...' button to link tables from the DB to their corresponding names in the schema file if they differ or weren't auto-mapped.\n", "list_item"),
        ("4.  Review & Map Columns:\n", ("bold", "list_item")),
        ("    Select a mapped table in the 'Old' list. The schema comparison will appear.\n    If column names have changed, use the 'Map Columns...' button to link old column names to new ones.\n", "list_item"),
        ("5.  Compare Schemas:\n", ("bold", "list_item")),
        ("    The middle section shows the original DB schema and the new schema from the file side-by-side. See 'Schema Highlighting' below.\n", "list_item"),
        ("6.  Confirm & Create:\n", ("bold", "list_item")),
        ("    Check all the boxes in the 'Manual Confirmation' section on the right (this is a safety gate).\n    Click 'CREATE New Table & Copy Data'.\n    The tool will perform data type checks (see 'Data Type Conversion').\n    If safe, it creates a new table (e.g., 'tablename_new') and copies data.\n", "list_item"),
        ("7.  View Data:\n", ("bold", "list_item")),
        ("    Use the 'View Old/New Table Data' buttons to inspect the contents.\n    You can download the data as CSV or JSON from the data view window.\n\n", "list_item"),
        ("Schema File Format:\n", "h2"),
        ("The schema file should be plain text. Define tables like this:\n\n", ""),
        ("Table: MyTable\n", "code"),
        ("  id INT AUTO_INCREMENT PRIMARY KEY\n", "code"),
        ("  name VARCHAR(100) NOT NULL DEFAULT ''\n", "code"),
        ("  email VARCHAR(150) UNIQUE\n", "code"),
        ("  created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n", "code"),
        ("\n", "code"),
        ("Table: AnotherTable\n", "code"),
        ("  # ... columns ...\n\n", "code"),
        ("Lines starting with # or -- are ignored as comments. Ensure correct SQL syntax for column definitions.\n\n", ""),
        ("List Colors (Old/Source Tables):\n", "h2"),
        ("  Red:\n", ("color_red", "list_item")),
        ("    Not mapped AND not found in the schema file. Potentially orphaned or needs adding to schema.\n", "list_item"),
        ("  Orange:\n", ("color_orange", "list_item")),
        ("    Found in schema file by name, but NOT explicitly mapped yet. Use 'Map Tables...' to confirm.\n", "list_item"),
        ("  Purple:\n", ("color_purple", "list_item")),
        ("    Mapped, but the target name specified in the mapping does NOT exist in the schema file.\n", "list_item"),
        ("  Blue:\n", ("color_blue", "list_item")),
        ("    Mapped correctly to a name in the schema file, but the corresponding '_new' table has NOT been created yet. Ready for creation.\n", "list_item"),
        ("  Black:\n", ("color_black", "list_item")),
        ("    Mapped correctly, schema target exists, AND the corresponding '_new' table already exists in the database.\n\n", "list_item"),
        ("List Colors (_new/Generated Tables):\n", "h2"),
        ("  Black:\n", ("color_black", "list_item")),
        ("    Standard color for generated '_new' tables.\n", "list_item"),
        ("  Grey:\n", ("color_grey", "list_item")),
        ("    The base name (without '_new') is not found in the schema file or mappings. May indicate an orphaned or old generated table.\n\n", "list_item"),
        ("Schema Highlighting (Middle Section):\n", "h2"),
        ("Compares columns between the DB (Old) and File (New) based on mappings or names:\n", "list_item"),
        ("  Matching (Grey Background):\n", ("bold", "list_item")),
        ("    Column exists in both with the same name/mapping and definition (Type, Null, Key, Default, Extra).\n", "list_item"),
        ("  Changed (Yellow Background):\n", ("bold", "list_item")),
        ("    Column exists in both, but one or more definition attributes have changed.\n", "list_item"),
        ("  Renamed (Light Blue Background):\n", ("bold", "list_item")),
        ("    Column is mapped between the old and new schemas with different names.\n", "list_item"),
        ("  Added (Green Background - New Schema):\n", ("bold", "list_item")),
        ("    Column exists only in the new schema definition.\n", "list_item"),
        ("  Removed (Pink Background - Old Schema):\n", ("bold", "list_item")),
        ("    Column exists only in the old database schema (will not be in the new table unless added to schema file).\n\n", "list_item"),
        ("Data Type Conversion:\n", "h2"),
        ("When creating the new table and copying data:\n", "list_item"),
        ("- ", ("bold", "list_item")),
        ("If data types differ, the tool checks if the conversion is generally safe.\n", "list_item"),
        ("- ", ("bold", "list_item")),
        ("Unsafe:\n", ("italic", "list_item")),
        ("    Conversions likely to fail or cause significant issues (e.g., text to int) will prevent table creation.\n", "list_item"),
        ("- ", ("bold", "list_item")),
        ("Lossy:\n", ("italic", "list_item")),
        ("    Conversions that might work but could truncate data or lose precision (e.g., float to int, long varchar to short) will trigger a warning prompt. You must explicitly agree to proceed.\n", "list_item"),
        ("- ", ("bold", "list_item")),
        ("Safe:\n", ("italic", "list_item")),
        ("    Conversions considered generally safe (e.g., int to bigint, int to varchar) will proceed automatically using `CAST`.\n\n", "list_item"),
        ("Manual Confirmation Checkboxes:\n", "h2"),
        ("The checkboxes on the right ('Compared Schemas?', etc.) ", "bold"),
        ("DO NOT", "warning"),
        (" directly affect the SQL generated. They act purely as a ", "bold"),
        ("manual safety checklist", "italic"),
        (". You must check all boxes to enable the 'CREATE New Table & Copy Data' button. This encourages you to double-check the schema comparison and mappings before proceeding.\n\n", ""),
        ("IMPORTANT WARNING:\n", "warning"),
        ("This tool modifies your database (creates tables, inserts data). ", "warning"),
        ("ALWAYS BACK UP YOUR DATABASE", "warning"),
        (" before performing any operations with this tool. Use with caution, especially on production systems.\n", "warning")
    ]

    for text, style_tags in help_content:
        help_text_widget.insert(END, text, style_tags)

    help_text_widget.config(state=tk.DISABLED) # Make text read-only
    # close_button = ttk.Button(help_win, text="Close", command=help_win.destroy)
    # close_button.pack(pady=10)


# --- Schema/DB Interaction Functions ---

def get_db_schema(table_name):
    """Fetches the schema of a table directly from the database using DESCRIBE."""
    if not conn or not conn.is_connected(): return None
    try:
        cursor.execute(f"DESCRIBE `{table_name}`")
        # Return as dict {col_name: (full_row_tuple)}
        schema = {col[0]: col for col in cursor.fetchall()}
        return schema
    except mysql.connector.Error as err:
        messagebox.showerror("Schema Error", f"Could not fetch schema for DB table '{table_name}': {err}")
        return None

def get_schema_file_definition(old_table_name_db):
    """Gets the target schema definition from the parsed file using mappings."""
    parsed_schema = parse_schema_file(schema_file_path)
    mapping_info = table_mappings.get(old_table_name_db)

    target_schema_name = None
    if isinstance(mapping_info, dict):
        target_schema_name = mapping_info.get("new_table_name_schema")
    elif old_table_name_db in parsed_schema: # Fallback if not mapped
        target_schema_name = old_table_name_db

    if target_schema_name and target_schema_name in parsed_schema:
        return parsed_schema[target_schema_name]
    else:
        return None # Indicate schema not found for this table


def parse_schema_file(file_path):
    """Parses the schema definition text file (handles comments, simple table/col defs)."""
    schema_dict = {}
    current_table = None
    if not file_path or not os.path.exists(file_path):
         return {}
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line or line.startswith("--") or line.startswith("#"): # Skip empty/comment lines
                    continue

                table_match = re.match(r"Table:\s*(\w+)", line, re.IGNORECASE)
                if table_match:
                    current_table = table_match.group(1)
                    schema_dict[current_table] = {} # Overwrite/use last definition if duplicate
                    continue

                if current_table:
                    # Handles optional quotes/backticks around column name
                    col_match = re.match(r"[`']?([\w_]+)[`']?\s+(.+)", line)
                    if col_match:
                        column_name = col_match.group(1)
                        attributes = col_match.group(2).strip()
                        schema_dict[current_table][column_name] = attributes # Overwrite/use last definition
    except Exception as e:
        messagebox.showerror("Error", f"Error parsing schema file '{file_path}': {e}")
        return {}

    return schema_dict


def show_schema(event=None):
    """Displays the old (DB) and new (file) schema for the selected table, applying highlighting."""
    selected_indices = tables_listbox_old.curselection()
    # Clear trees first
    for row in schema_tree_old.get_children(): schema_tree_old.delete(row)
    for row in schema_tree_new.get_children(): schema_tree_new.delete(row)

    if not selected_indices:
        return # Nothing selected, trees cleared
    selected_table_db = tables_listbox_old.get(selected_indices)

    # Get Old Schema (from DB)
    db_schema_raw = get_db_schema(selected_table_db)
    if db_schema_raw is None: return

    db_schema_processed = {} # Store data formatted for treeview and comparison
    for col_name, details in db_schema_raw.items():
        padded_details = list(details) + [''] * (6 - len(details))
        if padded_details[4] is None: padded_details[4] = 'NULL' # Display None default as 'NULL'
        schema_tree_old.insert("", tk.END, values=padded_details, iid=f"old_{col_name}")
        db_schema_processed[col_name] = padded_details

    # Get New Schema (from File via Mappings)
    new_schema_file_def = get_schema_file_definition(selected_table_db)
    new_schema_processed = {}

    if new_schema_file_def:
        for new_col_name, definition in new_schema_file_def.items():
            # Parse definition string into Treeview columns
            parts = definition.split()
            field_type_full = parts[0] if len(parts) > 0 else ""
            null_value = "NO" if "NOT NULL" in definition.upper() else "YES"
            key_value = ""
            default_value_parsed = None
            extra = ""
            def_upper = definition.upper() # For case-insensitive checks

            # Basic Key parsing
            if "PRIMARY KEY" in def_upper: key_value = "PRI"
            elif "UNIQUE" in def_upper: key_value = "UNI"
            if "AUTO_INCREMENT" in def_upper: extra = "auto_increment"

            # Default value parsing
            default_match = re.search(r"DEFAULT\s+((?:'(?:[^']|\\')*'|\"(?:[^\"]|\\\")*\"|[\w.-]+)|NULL)", definition, re.IGNORECASE)
            if default_match:
                default_raw = default_match.group(1)
                default_value_parsed = 'NULL' if default_raw.upper() == 'NULL' else default_raw.strip("'\"")
            else:
                default_value_parsed = 'NULL' # Assume NULL if no default specified (might vary by SQL mode/type)

            # Prepare row data for display
            row_data = (new_col_name, field_type_full, null_value, key_value, default_value_parsed, extra)
            schema_tree_new.insert("", tk.END, values=row_data, iid=f"new_{new_col_name}")
            new_schema_processed[new_col_name] = row_data
    else:
        schema_tree_new.insert("", tk.END, values=("(No definition found in schema file)", "", "", "", "", ""))

    highlight_differences(db_schema_processed, new_schema_processed)


def highlight_differences(db_schema_processed, new_schema_processed):
    """Highlights differences in the schema treeviews based on processed data and mappings."""
    # Clear existing tags first
    for item in schema_tree_old.get_children(): schema_tree_old.item(item, tags=())
    for item in schema_tree_new.get_children(): schema_tree_new.item(item, tags=())

    selected_indices = tables_listbox_old.curselection()
    if not selected_indices: return
    selected_table_db = tables_listbox_old.get(selected_indices)

    column_mappings = table_mappings.get(selected_table_db, {}).get("column_mappings", {})
    reverse_column_mappings = {v: k for k, v in column_mappings.items()}

    all_old_cols = set(db_schema_processed.keys())
    processed_old_cols = set()

    # Compare NEW schema against OLD
    for new_col_name, new_data in new_schema_processed.items():
        old_col_name = reverse_column_mappings.get(new_col_name, new_col_name)

        if old_col_name in db_schema_processed:
            old_data = db_schema_processed[old_col_name]
            processed_old_cols.add(old_col_name)

            try:
                # Compare: Type(basic), Null, Key, Default(str), Extra
                old_type_basic = get_base_type(old_data[1])
                new_type_basic = get_base_type(new_data[1])
                old_default_str = str(old_data[4]) # Already 'NULL' string if was None
                new_default_str = str(new_data[4])

                is_different = (old_type_basic != new_type_basic or
                               old_data[2] != new_data[2] or # Null
                               old_data[3] != new_data[3] or # Key
                               old_default_str != new_default_str or # Default
                               old_data[5] != new_data[5])   # Extra

                tag = "renamed" if old_col_name != new_col_name else ("changed" if is_different else "matching")

                schema_tree_old.item(f"old_{old_col_name}", tags=(tag,))
                schema_tree_new.item(f"new_{new_col_name}", tags=(tag,))
            except IndexError:
                 print(f"Warning: Index error comparing schemas for {old_col_name}/{new_col_name}.")
                 # Tag as changed on error
                 if f"old_{old_col_name}" in schema_tree_old.get_children(""):
                      schema_tree_old.item(f"old_{old_col_name}", tags=("changed",))
                 if f"new_{new_col_name}" in schema_tree_new.get_children(""):
                      schema_tree_new.item(f"new_{new_col_name}", tags=("changed",))
        else:
            # Column is added in the new schema
            schema_tree_new.item(f"new_{new_col_name}", tags=("added",))

    # Identify removed columns (old cols not processed)
    for old_col_name in all_old_cols - processed_old_cols:
        schema_tree_old.item(f"old_{old_col_name}", tags=("removed",))

    # Configure tag colors
    tag_colors = {
        "matching": "#E0E0E0", "changed": "#FFFACD", "renamed": "#ADD8E6",
        "removed": "#FFB6C1", "added": "#90EE90"
    }
    for tree in [schema_tree_old, schema_tree_new]:
        for tag, color in tag_colors.items():
            tree.tag_configure(tag, background=color)

# --- Data Type Conversion Logic ---

def get_base_type(dtype_string):
    """Extracts the base SQL type (e.g., 'varchar' from 'varchar(255)')."""
    if not dtype_string: return ""
    return dtype_string.split('(')[0].split()[0].lower()


def is_conversion_safe(old_type_str, new_type_str):
    """Heuristically determines if a data type conversion is generally 'safe', 'unsafe', or 'lossy'."""
    old_base = get_base_type(old_type_str)
    new_base = get_base_type(new_type_str)
    if old_base == new_base: return 'safe'

    # Type categories
    integer_types = {'tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint'}
    approx_numeric_types = {'float', 'double'}
    exact_numeric_types = {'decimal', 'numeric'}
    numeric_types = integer_types | approx_numeric_types | exact_numeric_types
    string_types = {'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set'}
    datetime_types = {'date', 'datetime', 'timestamp', 'time', 'year'}
    binary_types = {'binary', 'varbinary', 'tinyblob', 'blob', 'mediumblob', 'longblob', 'bit'}
    json_type = {'json'}

    # Determine categories
    old_cat = ('int' if old_base in integer_types else
               'approx' if old_base in approx_numeric_types else
               'exact' if old_base in exact_numeric_types else
               'str' if old_base in string_types else
               'dt' if old_base in datetime_types else
               'bin' if old_base in binary_types else
               'json' if old_base in json_type else 'other')
    new_cat = ('int' if new_base in integer_types else
               'approx' if new_base in approx_numeric_types else
               'exact' if new_base in exact_numeric_types else
               'str' if new_base in string_types else
               'dt' if new_base in datetime_types else
               'bin' if new_base in binary_types else
               'json' if new_base in json_type else 'other')

    # --- Conversion Rules ---
    # Anything to String is generally safe or lossy (formatting/encoding)
    if new_cat == 'str':
        return 'lossy' if old_cat == 'bin' else 'safe'

    # Numeric conversions
    if old_cat in ('int', 'approx', 'exact') and new_cat in ('int', 'approx', 'exact'):
        if new_cat == 'int': return 'lossy' if old_cat in ('approx', 'exact') else 'safe'
        if new_cat == 'approx': return 'lossy' # Precision loss possible
        if new_cat == 'exact': return 'lossy' if old_cat == 'approx' else 'safe'

    # Date/Time conversions
    if old_cat == 'dt' and new_cat == 'dt':
        if old_base == 'date' and new_base in ('datetime', 'timestamp'): return 'safe'
        return 'lossy' if old_base != new_base else 'safe'

    # Binary conversions
    if old_cat == 'bin' and new_cat == 'bin': return 'safe'
    if old_cat == 'str' and new_cat == 'bin': return 'lossy' # Encoding matters

    # JSON conversions
    if new_cat == 'json': return 'safe' # Most things can cast to JSON string representation
    if old_cat == 'json' and new_cat == 'str': return 'safe'

    # --- Default Unsafe ---
    # Covers String/Bin/JSON/DT -> Numeric, Numeric/DT -> Bin, etc.
    return 'unsafe'


def get_cast_type(full_type_definition):
    """Extracts a type suitable for MySQL CAST function."""
    type_upper = full_type_definition.upper()
    parts = type_upper.split('(')[0].split()
    base_type = parts[0]
    is_unsigned = "UNSIGNED" in parts or "UNSIGNED" in type_upper

    if base_type in ('TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT'):
        return "UNSIGNED" if is_unsigned else "SIGNED"
    elif base_type in ('FLOAT', 'DOUBLE', 'REAL'):
        return "DOUBLE" # Cast to DOUBLE
    elif base_type in ('DECIMAL', 'NUMERIC', 'FIXED'):
        match = re.search(r'\((\d+)(?:,(\d+))?\)', full_type_definition)
        precision = match.group(1) if match else '65' # High precision default
        scale = match.group(2) if match and match.group(2) else '30' # High scale default
        return f"DECIMAL({precision},{scale})"
    elif base_type in ('CHAR', 'VARCHAR', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'ENUM', 'SET'):
         # Determine max length needed? Difficult. Use reasonable default or TEXT?
         # Using CHAR might truncate, using TEXT might be inefficient?
         # Let's try casting to the target base type directly if possible, otherwise CHAR.
         # CAST might not support all these directly. CHAR is safest fallback.
         return "CHAR CHARACTER SET utf8mb4"
    elif base_type == 'DATE': return "DATE"
    elif base_type == 'DATETIME': return "DATETIME"
    elif base_type == 'TIMESTAMP': return "DATETIME" # Cast to DATETIME
    elif base_type == 'TIME': return "TIME"
    elif base_type == 'YEAR': return "SIGNED" # Cast YEAR to SIGNED
    elif base_type in ('BINARY', 'VARBINARY', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'BIT'):
        return "BINARY" # Length might matter
    elif base_type == 'JSON': return "JSON"

    print(f"Warning: Could not determine specific CAST type for '{full_type_definition}'. Using CHAR.")
    return "CHAR CHARACTER SET utf8mb4"


# --- Table Creation and Data Copy ---

def get_copy_column_pairs(old_table_name, new_schema_table_name, db_schema_dict, new_schema_dict, current_mappings):
    """Determines column pairs for copying data, returns list of tuples or None if unsafe conversion found."""
    pairs = []
    column_mappings = current_mappings.get(old_table_name, {}).get("column_mappings", {})
    reverse_column_mappings = {v: k for k, v in column_mappings.items()}

    for new_col_name, new_col_definition in new_schema_dict.items():
        old_col_name = reverse_column_mappings.get(new_col_name, new_col_name)

        if old_col_name in db_schema_dict:
            old_col_details = db_schema_dict[old_col_name]
            old_col_type = old_col_details[1]
            new_col_type = new_col_definition.split()[0] # Basic type extraction

            safety = is_conversion_safe(old_col_type, new_col_type)
            requires_cast = False
            cast_type_str = None

            if safety == 'unsafe':
                 messagebox.showerror("Unsafe Conversion Detected",
                                     f"Cannot prepare data copy due to unsafe type conversion:\n"
                                     f"Table: '{old_table_name}' -> '{new_schema_table_name}'\n"
                                     f"Column: `{old_col_name}` ({old_col_type}) -> `{new_col_name}` ({new_col_type})\n"
                                     "Manual intervention required.")
                 return None # Signal error

            if get_base_type(old_col_type) != get_base_type(new_col_type):
                if safety in ('safe', 'lossy'):
                    requires_cast = True
                    cast_type_str = get_cast_type(new_col_type)

            select_expression = f"`{old_col_name}`"
            if requires_cast and cast_type_str:
                 select_expression = f"CAST(`{old_col_name}` AS {cast_type_str})"

            pairs.append((select_expression, new_col_name, requires_cast, cast_type_str, old_col_type, new_col_type))

    return pairs


def copy_data_in_batches(old_table_name, new_table_name, copy_pairs, cursor, conn):
    """Copies data using INSERT ... SELECT based on column pairs, handling CAST and batches."""
    if not copy_pairs:
        print(f"No common/mappable columns found. Skipping data migration for {old_table_name} -> {new_table_name}.")
        return True

    new_columns_for_insert = [f"`{pair[1]}`" for pair in copy_pairs]
    old_columns_for_select = [pair[0] for pair in copy_pairs]
    new_cols_str = ", ".join(new_columns_for_insert)
    select_expr_str = ", ".join(old_columns_for_select)

    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{old_table_name}`")
        total_rows = cursor.fetchone()[0]
        if total_rows == 0:
             print(f"Source table '{old_table_name}' is empty. No data to copy.")
             return True

        print(f"Total rows to copy for {old_table_name}: {total_rows}")
        BATCH_SIZE = 5000 # Smaller batch size might be safer
        rows_copied = 0
        print(f"Starting data copy from '{old_table_name}' to '{new_table_name}'...")
        for offset in range(0, total_rows, BATCH_SIZE):
            # Ensure connection is still valid before executing batch
            if not conn.is_connected():
                 messagebox.showerror("Connection Error", "Database connection lost during data copy.")
                 return False

            copy_query = f"INSERT INTO `{new_table_name}` ({new_cols_str}) SELECT {select_expr_str} FROM `{old_table_name}` ORDER BY 1 LIMIT {BATCH_SIZE} OFFSET {offset};"
            try:
                cursor.execute(copy_query)
                batch_rows = cursor.rowcount
                rows_copied += batch_rows
                # Check for warnings
                cursor.execute("SHOW WARNINGS")
                warnings = cursor.fetchall()
                if warnings:
                    warning_messages = "\n".join([f"  - L{w[0]} C{w[1]}: {w[2]}" for w in warnings[:5]]) # Show first 5 warnings
                    print(f"Warnings during batch insert (Offset {offset}, {batch_rows} rows):\n{warning_messages}")
                    if len(warnings) > 5: print("  ...")
                conn.commit()
                print(f"  Copied batch: {rows_copied}/{total_rows} rows...")
            except mysql.connector.Error as batch_err:
                 conn.rollback()
                 messagebox.showerror("Data Copy Error",
                                    f"Error copying data batch for {new_table_name} (offset {offset}). Rolled back.\n"
                                    f"Error: {batch_err}\nQuery:\n{copy_query}\n")
                 return False
        print(f"Finished copying data for {new_table_name}. Total rows affected: {rows_copied}")
        if rows_copied != total_rows:
             print(f"Warning: Rows copied ({rows_copied}) != source count ({total_rows}).")
        return True
    except mysql.connector.Error as err:
        messagebox.showerror("Data Copy Error", f"Failed during data copy prep for {new_table_name}: {err}")
        return False


def generate_create_statement(target_table_name, new_schema_definition):
    """Generates the CREATE TABLE statement from the parsed schema definition."""
    column_definitions = [f"  `{col_name}` {definition}"
                          for col_name, definition in new_schema_definition.items()]
    # Needs more sophisticated parsing for separate PK, FK, Index definitions
    statement = f"CREATE TABLE `{target_table_name}` (\n"
    statement += ",\n".join(column_definitions)
    statement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    return statement


def create_new_table_and_copy_data():
    """Handles the process of creating the new table and copying data after checks and confirmations."""
    if not check_constraints(): # Check manual confirmation gate
         messagebox.showwarning("Confirmation Needed", "Please check all boxes in 'Manual Confirmation Gate' first.")
         return

    selected_indices = tables_listbox_old.curselection()
    if not selected_indices:
        messagebox.showerror("Error", "Please select a table from the 'Old Tables' list.")
        return
    selected_table_db = tables_listbox_old.get(selected_indices)

    # Get Schemas and Mappings
    db_schema = get_db_schema(selected_table_db)
    if not db_schema: return
    new_schema_def = get_schema_file_definition(selected_table_db)
    if not new_schema_def:
        messagebox.showerror("Error", f"No schema definition found for the target of '{selected_table_db}'. Map table first.")
        return

    mapping_info = table_mappings.get(selected_table_db, {})
    target_schema_name = mapping_info.get("new_table_name_schema", selected_table_db)
    new_db_table_name = f"{target_schema_name}_new"

    # Check for Existing Target Table
    try:
        cursor.execute(f"SHOW TABLES LIKE '{new_db_table_name}'")
        if cursor.fetchone():
            messagebox.showwarning("Warning", f"Target table '{new_db_table_name}' already exists. Drop manually to recreate.")
            return
    except mysql.connector.Error as err:
         messagebox.showerror("DB Error", f"Error checking for table '{new_db_table_name}': {err}")
         return

    # Analyze Column Changes (checks for unsafe conversions internally)
    copy_pairs_analysis = get_copy_column_pairs(selected_table_db, target_schema_name, db_schema, new_schema_def, table_mappings)
    if copy_pairs_analysis is None: # Unsafe conversion detected and message shown
        return

    # Check for lossy conversions and prompt user
    lossy_conversions_prompt = [f"- `{p[1]}` ({p[4]} -> {p[5]})" for p in copy_pairs_analysis
                                if get_base_type(p[4]) != get_base_type(p[5]) and is_conversion_safe(p[4], p[5]) == 'lossy']
    if lossy_conversions_prompt:
        message = "Potential data loss/truncation for conversions:\n\n" + "\n".join(lossy_conversions_prompt) + "\n\nProceed anyway?"
        if not messagebox.askyesno("Potential Data Loss Warning", message):
            messagebox.showinfo("Aborted", "Table creation aborted by user.")
            return

    # Generate and Execute CREATE TABLE
    create_statement = generate_create_statement(new_db_table_name, new_schema_def)
    try:
        print(f"Executing CREATE TABLE statement for {new_db_table_name}...")
        cursor.execute(create_statement)
        conn.commit()
        print(f"Table '{new_db_table_name}' created successfully.")
    except mysql.connector.Error as err:
        conn.rollback()
        messagebox.showerror("Create Table Error", f"Failed to create table '{new_db_table_name}':\n{err}\n\nSQL:\n{create_statement}")
        return

    # Copy Data
    copy_successful = copy_data_in_batches(selected_table_db, new_db_table_name, copy_pairs_analysis, cursor, conn)

    if copy_successful:
        messagebox.showinfo("Success", f"Table '{new_db_table_name}' created and data copied successfully!")
    else:
        messagebox.showerror("Error", f"Table '{new_db_table_name}' created, but data copy FAILED. Check logs/table state.")
        # Consider adding option to drop the partially created/filled table here

    get_tables() # Refresh table lists regardless of copy success/failure
    # Reset Checkboxes
    if 'constraint_vars' in globals() and constraint_vars:
        for var in constraint_vars: var.set(False)
    check_constraints()


# --- Data Viewing ---

def safe_serializer(obj):
    """JSON serializer for types not handled by default encoder."""
    if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj) # Use string for exact Decimal representation
    elif isinstance(obj, bytes):
        try:
            return obj.decode('utf-8', errors='replace') # Try UTF-8 first
        except UnicodeDecodeError:
            return obj.hex() # Fallback to hex if decode fails
    # Let the default encoder handle built-in types and raise TypeError for others
    raise TypeError(f"Type {type(obj)} not serializable for JSON")


def view_data(table_name, columns, data, data_window):
    """General function to display data in a Treeview and add download options."""
    frame = ttk.Frame(data_window)
    frame.pack(fill=tk.BOTH, expand=True)

    tree_scroll_y = ttk.Scrollbar(frame, orient="vertical")
    tree_scroll_x = ttk.Scrollbar(frame, orient="horizontal")
    data_tree = ttk.Treeview(frame, columns=columns, show="headings",
                             yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
    tree_scroll_y.config(command=data_tree.yview)
    tree_scroll_x.config(command=data_tree.xview)

    tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
    tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
    data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    for col in columns:
        data_tree.heading(col, text=col, anchor='w')
        data_tree.column(col, width=120, anchor='w', stretch=tk.NO)

    MAX_ROWS_DISPLAY = 5000 # Limit direct display for performance
    display_data = data[:MAX_ROWS_DISPLAY]
    for i, row in enumerate(display_data):
        display_row = []
        for v in row:
            if isinstance(v, bytes):
                 try: display_row.append(v.decode('utf-8', errors='replace'))
                 except: display_row.append(f"<{len(v)} bytes>") # Show placeholder for un-decodable bytes
            elif v is None: display_row.append('NULL')
            else: display_row.append(str(v))
        data_tree.insert("", tk.END, values=display_row)

    if len(data) > MAX_ROWS_DISPLAY:
         data_tree.insert("", tk.END, values=(f"... (displaying first {MAX_ROWS_DISPLAY} of {len(data)} rows) ...",))

    # --- Download Functions ---
    def download_csv():
        initial_filename = re.sub(r'[^\w\-]+', '_', table_name)
        file_path = filedialog.asksaveasfilename(
            title="Save as CSV", initialfile=f"{initial_filename}_data.csv", defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")], parent=data_window )
        if file_path:
            try:
                with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                    csv_writer.writerow(columns) # Header
                    for row_data in data: # Write ALL data
                         csv_row = []
                         for v in row_data:
                              if v is None: csv_row.append('')
                              elif isinstance(v, bytes):
                                   try: csv_row.append(v.decode('utf-8', errors='replace'))
                                   except: csv_row.append(f"<bytes:{len(v)}>") # CSV placeholder
                              else: csv_row.append(str(v))
                         csv_writer.writerow(csv_row)
                messagebox.showinfo("Download Successful", f"Full data ({len(data)} rows) downloaded to\n{file_path}", parent=data_window)
            except Exception as e:
                messagebox.showerror("Download Error", f"Failed to download CSV data: {e}", parent=data_window)

    def download_json():
        initial_filename = re.sub(r'[^\w\-]+', '_', table_name)
        file_path = filedialog.asksaveasfilename(
            title="Save as JSON", initialfile=f"{initial_filename}_data.json", defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")], parent=data_window )
        if file_path:
            try:
                json_data = [dict(zip(columns, row_data)) for row_data in data] # Write ALL data
                with open(file_path, 'w', encoding='utf-8') as jsonfile:
                    json.dump(json_data, jsonfile, indent=2, default=safe_serializer) # Use safe_serializer
                messagebox.showinfo("Download Successful", f"Full data ({len(data)} rows) downloaded to\n{file_path}", parent=data_window)
            except TypeError as e:
                 messagebox.showerror("JSON Error", f"Failed to serialize data to JSON.\nError: {e}\nCheck for unsupported data types.", parent=data_window)
            except Exception as e:
                messagebox.showerror("Download Error", f"Failed to download JSON data: {e}", parent=data_window)

    # --- Download Buttons ---
    download_frame = ttk.Frame(data_window)
    download_frame.pack(pady=10)
    ttk.Label(download_frame, text="Download Full Data:").pack(side=tk.LEFT, padx=5)
    ttk.Button(download_frame, text="CSV", command=download_csv).pack(side=tk.LEFT, padx=5)
    ttk.Button(download_frame, text="JSON", command=download_json).pack(side=tk.LEFT, padx=5)


def view_old_data():
    """Fetches and displays data from the selected 'Old Table'."""
    selected_index = tables_listbox_old.curselection()
    if not selected_index:
        messagebox.showerror("Error", "Please select a table from 'Old Tables' list.")
        return
    selected_table = tables_listbox_old.get(selected_index)
    try:
        if not conn or not conn.is_connected():
            messagebox.showerror("Error", "Database connection lost.")
            return
        print(f"Fetching data for OLD table: {selected_table}...")
        cursor.execute(f"SELECT * FROM `{selected_table}`")
        data = cursor.fetchall()
        print(f"Fetched {len(data)} rows.")
        if not data and cursor.rowcount == 0:
             messagebox.showinfo("No Data", f"Table '{selected_table}' is empty.")
             return

        columns = [desc[0] for desc in cursor.description]
        data_window = Toplevel(root)
        data_window.title(f"Data from OLD: {selected_table} ({len(data)} rows)")
        center_window(data_window, 800, 500)
        view_data(selected_table, columns, data, data_window)

    except mysql.connector.Error as err:
        messagebox.showerror("DB Error", f"Error fetching data for '{selected_table}': {err}")
    except Exception as e:
        messagebox.showerror("Error", f"Unexpected error viewing old data: {e}")


def view_new_data():
    """Fetches and displays data from the selected 'New Table' or the one corresponding to the selected Old Table."""
    selected_table = None
    source_listbox = None

    # Prioritize selection from the _new listbox
    selected_index_new = tables_listbox_new.curselection()
    if selected_index_new:
        selected_table = tables_listbox_new.get(selected_index_new)
        source_listbox = "New Tables"
    else:
        # Check old table list selection as fallback
        old_selected_index = tables_listbox_old.curselection()
        if old_selected_index:
             selected_old_table = tables_listbox_old.get(old_selected_index)
             mapping_info = table_mappings.get(selected_old_table, {})
             target_schema_name = mapping_info.get("new_table_name_schema", selected_old_table)
             potential_new_table = f"{target_schema_name}_new"
             source_listbox = f"Old '{selected_old_table}' ->"
             # Verify _new table exists
             try:
                 if not conn or not conn.is_connected(): raise mysql.connector.Error("Not connected")
                 cursor.execute(f"SHOW TABLES LIKE '{potential_new_table}'")
                 if cursor.fetchone(): selected_table = potential_new_table
                 else: messagebox.showerror("Error", f"Corresponding table '{potential_new_table}' not found."); return
             except mysql.connector.Error as err: messagebox.showerror("Error", f"DB error checking for '{potential_new_table}': {err}"); return
        else:
             messagebox.showerror("Error", "Select table from 'New Tables' list or a processed table from 'Old Tables'."); return

    if selected_table:
        try:
            if not conn or not conn.is_connected(): messagebox.showerror("Error", "Database connection lost."); return
            print(f"Fetching data for NEW table: {selected_table}...")
            cursor.execute(f"SELECT * FROM `{selected_table}`")
            data = cursor.fetchall()
            print(f"Fetched {len(data)} rows.")
            if not data and cursor.rowcount == 0: messagebox.showinfo("No Data", f"Table '{selected_table}' is empty."); return

            columns = [desc[0] for desc in cursor.description]
            data_window = Toplevel(root)
            data_window.title(f"Data from NEW ({source_listbox} {selected_table}) - {len(data)} rows")
            center_window(data_window, 800, 500)
            view_data(selected_table, columns, data, data_window)

        except mysql.connector.Error as err: messagebox.showerror("DB Error", f"Error fetching data for '{selected_table}': {err}")
        except Exception as e: messagebox.showerror("Error", f"Unexpected error viewing new data: {e}")


# --- Mapping Logic ---

def auto_map_tables_and_columns(parsed_schema):
    """Automatically maps tables and columns with identical names on initial load."""
    global table_mappings
    if not conn or not conn.is_connected() or not parsed_schema: return

    try:
        cursor.execute("SHOW TABLES")
        all_tables_db = [table[0] for table in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Warning: Could not fetch tables during auto-mapping: {e}")
        return

    made_changes = False
    print("Starting auto-mapping...")
    for table_db in all_tables_db:
        if table_db.endswith("_new"): continue

        # Auto map table if name matches schema and not already mapped
        if table_db in parsed_schema and table_db not in table_mappings:
            print(f"- Auto-mapping table: '{table_db}'")
            table_mappings[table_db] = {"new_table_name_schema": table_db, "column_mappings": {}}
            made_changes = True

        # Auto map columns for tables (newly or previously mapped)
        if table_db in table_mappings:
            mapping_info = table_mappings[table_db]
            if not isinstance(mapping_info, dict) or "new_table_name_schema" not in mapping_info: continue

            target_schema_name = mapping_info["new_table_name_schema"]
            if target_schema_name in parsed_schema:
                db_schema = get_db_schema(table_db)
                if db_schema:
                     new_schema_cols = parsed_schema[target_schema_name]
                     if "column_mappings" not in mapping_info: mapping_info["column_mappings"] = {}
                     current_col_mappings = mapping_info["column_mappings"]
                     for db_col_name in db_schema.keys():
                         if (db_col_name in new_schema_cols and
                             db_col_name not in current_col_mappings and
                             db_col_name not in current_col_mappings.values()):
                             print(f"  - Auto-mapping column: {table_db}.{db_col_name}")
                             current_col_mappings[db_col_name] = db_col_name
                             made_changes = True

    if made_changes:
        print("Saving updated auto-mappings...")
        save_mappings(table_mappings)
    else:
        print("No new auto-mappings found.")


def browse_file():
    """Opens file dialog to select schema file."""
    current_dir = os.path.dirname(schema_file_entry.get())
    if not os.path.isdir(current_dir): current_dir = os.path.expanduser("~")

    file_path = filedialog.askopenfilename(
        title="Select Schema Definition File", initialdir=current_dir,
        filetypes=[("Schema files", "*.txt;*.sql"), ("All files", "*.*")] )
    if file_path:
        schema_file_entry.delete(0, tk.END)
        schema_file_entry.insert(0, file_path)


def map_tables():
    """Opens a window to manually map an old DB table to a schema table name."""
    selected_indices = tables_listbox_old.curselection()
    if not selected_indices:
        messagebox.showerror("Error", "Select a table from 'Old Tables' list to map.")
        return
    selected_table_db = tables_listbox_old.get(selected_indices)

    parsed_schema = parse_schema_file(schema_file_path)
    if not parsed_schema:
         messagebox.showerror("Error", "Cannot map tables, schema file empty or invalid.")
         return

    schema_table_names = sorted(list(parsed_schema.keys()))
    # Find targets already mapped from OTHER tables
    currently_mapped_targets = {m_data.get("new_table_name_schema") for old_t, m_data in table_mappings.items()
                                if old_t != selected_table_db and isinstance(m_data, dict)}
    available_schema_tables = [name for name in schema_table_names if name not in currently_mapped_targets]

    current_target = table_mappings.get(selected_table_db, {}).get("new_table_name_schema")
    # Ensure current target is in the list if it exists
    if current_target and current_target in schema_table_names and current_target not in available_schema_tables:
         available_schema_tables.insert(0, current_target)

    if not available_schema_tables:
         messagebox.showinfo("No Tables to Map", "All tables in the schema file seem mapped from other tables.")
         return

    # --- Mapping Window ---
    map_win = Toplevel(root)
    map_win.title(f"Map Table '{selected_table_db}'")
    center_window(map_win, 400, 200)
    map_win.transient(root)
    map_win.grab_set()

    ttk.Label(map_win, text=f"Map DB table '{selected_table_db}' to\nwhich table name in schema file?").pack(pady=10)
    new_table_var = tk.StringVar(map_win)
    if current_target and current_target in available_schema_tables: new_table_var.set(current_target)
    elif available_schema_tables: new_table_var.set(available_schema_tables[0])

    dropdown = ttk.Combobox(map_win, textvariable=new_table_var, values=available_schema_tables, state="readonly", width=35)
    dropdown.pack(pady=5)

    def confirm_mapping():
        chosen_schema_table = new_table_var.get()
        if not chosen_schema_table: messagebox.showerror("Error", "Select a schema table name.", parent=map_win); return

        if selected_table_db not in table_mappings or not isinstance(table_mappings[selected_table_db], dict):
             table_mappings[selected_table_db] = {"column_mappings": {}}
        table_mappings[selected_table_db]["new_table_name_schema"] = chosen_schema_table

        # Attempt column auto-map after table map
        target_schema_def = parsed_schema.get(chosen_schema_table, {})
        db_schema = get_db_schema(selected_table_db)
        if db_schema and target_schema_def:
             if "column_mappings" not in table_mappings[selected_table_db]: table_mappings[selected_table_db]["column_mappings"] = {}
             current_col_maps = table_mappings[selected_table_db]["column_mappings"]
             print(f"Attempting column auto-map after table map '{selected_table_db}' -> '{chosen_schema_table}'")
             newly_mapped_cols = 0
             for db_col in db_schema:
                  if db_col in target_schema_def and db_col not in current_col_maps and db_col not in current_col_maps.values():
                       print(f"  - Auto-mapping column: {selected_table_db}.{db_col}")
                       current_col_maps[db_col] = db_col; newly_mapped_cols += 1
             if newly_mapped_cols > 0: print(f"  Auto-mapped {newly_mapped_cols} columns.")

        save_mappings(table_mappings)
        map_win.destroy()
        get_tables(); show_schema()

    def unmap_table():
         if selected_table_db in table_mappings:
              if messagebox.askyesno("Confirm Unmap", f"Remove mapping for table '{selected_table_db}'?", parent=map_win):
                  del table_mappings[selected_table_db]
                  save_mappings(table_mappings)
                  messagebox.showinfo("Unmapped", f"Table '{selected_table_db}' unmapped.", parent=map_win)
                  map_win.destroy(); get_tables(); show_schema()
         else: messagebox.showinfo("Info", "Table not currently mapped.", parent=map_win)

    button_frame = ttk.Frame(map_win)
    button_frame.pack(pady=15)
    ttk.Button(button_frame, text="Confirm Map", command=confirm_mapping, width=15).pack(side=tk.LEFT, padx=10)
    ttk.Button(button_frame, text="Unmap Table", command=unmap_table, width=15).pack(side=tk.LEFT, padx=10)
    map_win.wait_window()


def map_columns():
    """Opens a window to manually map columns between the selected old table and its mapped new schema."""
    selected_indices_old = tables_listbox_old.curselection()
    if not selected_indices_old: messagebox.showerror("Error", "Select a table from 'Old Tables' list first."); return
    selected_table_db = tables_listbox_old.get(selected_indices_old)

    mapping_info = table_mappings.get(selected_table_db)
    if not isinstance(mapping_info, dict) or "new_table_name_schema" not in mapping_info:
        messagebox.showerror("Error", f"Table '{selected_table_db}' is not mapped. Use 'Map Tables...' first."); return

    target_schema_name = mapping_info["new_table_name_schema"]
    if "column_mappings" not in mapping_info: mapping_info["column_mappings"] = {}
    current_col_mappings = mapping_info["column_mappings"]

    db_schema = get_db_schema(selected_table_db)
    if not db_schema: return
    new_schema_def = get_schema_file_definition(selected_table_db)
    if not new_schema_def: messagebox.showerror("Error", f"Schema definition for '{target_schema_name}' not found."); return

    db_cols = set(db_schema.keys())
    new_cols = set(new_schema_def.keys())
    mapped_db_cols = set(current_col_mappings.keys())
    mapped_new_cols = set(current_col_mappings.values())
    unmapped_db_cols = sorted(list(db_cols - mapped_db_cols))
    unmapped_new_cols = sorted(list(new_cols - mapped_new_cols))

    # Create display lists with types
    unmapped_db_cols_display = [f"{c} ({get_base_type(db_schema[c][1])})" for c in unmapped_db_cols]
    unmapped_new_cols_display = [f"{c} ({get_base_type(new_schema_def[c].split()[0])})" for c in unmapped_new_cols]

    if not unmapped_db_cols and not unmapped_new_cols and current_col_mappings: pass # Allow viewing/unmapping
    elif not unmapped_db_cols or not unmapped_new_cols: messagebox.showinfo("No Pairs to Map", "No unmapped columns available in both tables."); # Still show window if maps exist

    # --- Mapping Window ---
    col_map_win = Toplevel(root)
    col_map_win.title(f"Map Columns: {selected_table_db} -> {target_schema_name}")
    center_window(col_map_win, 700, 450)
    col_map_win.transient(root); col_map_win.grab_set()

    main_frame = ttk.Frame(col_map_win, padding=10)
    main_frame.pack(fill=tk.BOTH, expand=True)
    main_frame.columnconfigure(0, weight=1); main_frame.columnconfigure(2, weight=1)
    main_frame.rowconfigure(2, weight=1) # Allow map display to expand

    # Dropdowns for unmapped columns
    left_frame = ttk.Frame(main_frame); left_frame.grid(row=0, column=0, padx=10, pady=5, sticky="nsew")
    ttk.Label(left_frame, text=f"Unmapped DB Columns ({selected_table_db}):").pack(anchor='w')
    db_col_var = tk.StringVar(col_map_win)
    db_col_combo = ttk.Combobox(left_frame, textvariable=db_col_var, values=unmapped_db_cols_display, state="readonly", width=35)
    db_col_combo.pack(pady=5, fill='x');
    if unmapped_db_cols_display: db_col_combo.current(0)

    ttk.Label(main_frame, text="->").grid(row=0, column=1, padx=10, pady=25, sticky='n')

    right_frame = ttk.Frame(main_frame); right_frame.grid(row=0, column=2, padx=10, pady=5, sticky="nsew")
    ttk.Label(right_frame, text=f"Unmapped Schema Columns ({target_schema_name}):").pack(anchor='w')
    new_col_var = tk.StringVar(col_map_win)
    new_col_combo = ttk.Combobox(right_frame, textvariable=new_col_var, values=unmapped_new_cols_display, state="readonly", width=35)
    new_col_combo.pack(pady=5, fill='x');
    if unmapped_new_cols_display: new_col_combo.current(0)

    # --- Actions ---
    def get_col_name_from_display(display_str): return display_str.split(' (')[0] if display_str else None

    def confirm_col_mapping():
        selected_old = get_col_name_from_display(db_col_var.get())
        selected_new = get_col_name_from_display(new_col_var.get())
        if not selected_old or not selected_new: messagebox.showerror("Error", "Select one column from each list.", parent=col_map_win); return

        # Check conflicts
        if selected_old in current_col_mappings: messagebox.showerror("Error", f"DB Column '{selected_old}' already mapped.", parent=col_map_win); return
        if selected_new in current_col_mappings.values(): messagebox.showerror("Error", f"Schema Column '{selected_new}' already mapped.", parent=col_map_win); return

        table_mappings[selected_table_db]["column_mappings"][selected_old] = selected_new
        save_mappings(table_mappings)
        messagebox.showinfo("Mapping Saved", f"Mapped: {selected_old} -> {selected_new}", parent=col_map_win)
        col_map_win.destroy(); show_schema()

    def unmap_selected_col():
        selected_old = get_col_name_from_display(db_col_var.get())
        selected_new = get_col_name_from_display(new_col_var.get())
        col_to_unmap_old, col_to_report = None, None

        # Find mapping pair based on selections
        for old_c, new_c in current_col_mappings.items():
            if old_c == selected_old and new_c == selected_new: col_to_unmap_old, col_to_report = old_c, f"{old_c} -> {new_c}"; break
            elif old_c == selected_old: col_to_unmap_old, col_to_report = old_c, f"{old_c} -> {new_c}"; break
            elif new_c == selected_new: col_to_unmap_old, col_to_report = old_c, f"{old_c} -> {new_c}"; break

        if col_to_unmap_old:
             if messagebox.askyesno("Confirm Unmap", f"Remove mapping:\n{col_to_report}?", parent=col_map_win):
                  del table_mappings[selected_table_db]["column_mappings"][col_to_unmap_old]
                  save_mappings(table_mappings)
                  messagebox.showinfo("Mapping Removed", f"Removed mapping: {col_to_report}", parent=col_map_win)
                  col_map_win.destroy(); show_schema()
        else: messagebox.showerror("Error", "No matching mapping found for selection.", parent=col_map_win)

    # --- Buttons ---
    button_frame = ttk.Frame(main_frame); button_frame.grid(row=1, column=0, columnspan=3, pady=15)
    map_button = ttk.Button(button_frame, text="Confirm Column Map", command=confirm_col_mapping, width=20)
    map_button.pack(side=tk.LEFT, padx=10); map_button.config(state=tk.DISABLED if not unmapped_db_cols or not unmapped_new_cols else tk.NORMAL)
    unmap_button = ttk.Button(button_frame, text="Unmap Selected Column", command=unmap_selected_col, width=20)
    unmap_button.pack(side=tk.LEFT, padx=10); unmap_button.config(state=tk.DISABLED if not current_col_mappings else tk.NORMAL)
    ttk.Button(button_frame, text="Cancel", command=col_map_win.destroy, width=10).pack(side=tk.LEFT, padx=10)

    # --- Existing Mappings Display ---
    existing_map_frame = ttk.LabelFrame(main_frame, text="Existing Column Mappings"); existing_map_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")
    map_text_frame = ttk.Frame(existing_map_frame); map_text_frame.pack(fill=tk.BOTH, expand=True); map_text_frame.rowconfigure(0, weight=1); map_text_frame.columnconfigure(0, weight=1)
    map_text = tk.Text(map_text_frame, height=6, wrap=tk.NONE, bd=0, font=("Courier New", 9)); map_scroll_y = ttk.Scrollbar(map_text_frame, orient=tk.VERTICAL, command=map_text.yview); map_scroll_x = ttk.Scrollbar(map_text_frame, orient=tk.HORIZONTAL, command=map_text.xview); map_text.config(yscrollcommand=map_scroll_y.set, xscrollcommand=map_scroll_x.set)
    map_text.grid(row=0, column=0, sticky='nsew', padx=(5,0), pady=(5,0)); map_scroll_y.grid(row=0, column=1, sticky='ns', padx=(0,5), pady=(5,0)); map_scroll_x.grid(row=1, column=0, sticky='ew', padx=(5,0), pady=(0,5))
    if current_col_mappings:
         max_old_len = max(len(k) for k in current_col_mappings.keys()) if current_col_mappings else 0
         for old_c, new_c in sorted(current_col_mappings.items()):
              old_type = get_base_type(db_schema.get(old_c, ['','?'])[1])
              new_type = get_base_type(new_schema_def.get(new_c, '?').split()[0])
              map_text.insert(tk.END, f"{old_c:<{max_old_len}} ({old_type:<10}) -> {new_c} ({new_type})\n")
    else: map_text.insert(tk.END, "No specific column mappings defined.")
    map_text.config(state=tk.DISABLED)
    col_map_win.wait_window()


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

    root = tk.Tk()
    root.title("Database Schema Migration Tool")
    center_window(root, 1350, 750)
    root.minsize(1200, 650)

    current_db_name = None
    table_mappings = {}
    schema_file_path = ""

    style = ttk.Style(); # style.theme_use('clam') # Optionally set theme

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
    tables_frame.columnconfigure(1, weight=4, minsize=400)
    tables_frame.columnconfigure(0, weight=1, minsize=200)
    tables_frame.columnconfigure(2, weight=1, minsize=200)
    tables_frame.rowconfigure(0, weight=1)

    # --- Left Frame (Table Lists) ---
    left_frame = ttk.Frame(tables_frame); left_frame.grid(row=0, column=0, padx=(10,5), pady=10, sticky="nsew"); left_frame.rowconfigure(1, weight=1); left_frame.rowconfigure(3, weight=1); left_frame.columnconfigure(0, weight=1)
    ttk.Label(left_frame, text="DB Tables (Old/Source):").grid(row=0, column=0, sticky="w", pady=(0,2))
    listbox_old_frame = ttk.Frame(left_frame); listbox_old_frame.grid(row=1, column=0, sticky="nsew"); listbox_old_frame.rowconfigure(0, weight=1); listbox_old_frame.columnconfigure(0, weight=1)
    tables_listbox_old = tk.Listbox(listbox_old_frame, width=35, exportselection=False, font=("Courier New", 9)); tables_listbox_old.grid(row=0, column=0, sticky="nsew"); scrollbar_old_y = ttk.Scrollbar(listbox_old_frame, orient=tk.VERTICAL, command=tables_listbox_old.yview); scrollbar_old_y.grid(row=0, column=1, sticky="ns"); tables_listbox_old.config(yscrollcommand=scrollbar_old_y.set); tables_listbox_old.bind("<<ListboxSelect>>", show_schema)
    ttk.Label(left_frame, text="DB Tables (_new/Generated):").grid(row=2, column=0, sticky="w", pady=(5,2))
    listbox_new_frame = ttk.Frame(left_frame); listbox_new_frame.grid(row=3, column=0, sticky="nsew"); listbox_new_frame.rowconfigure(0, weight=1); listbox_new_frame.columnconfigure(0, weight=1)
    tables_listbox_new = tk.Listbox(listbox_new_frame, width=35, exportselection=False, font=("Courier New", 9)); tables_listbox_new.grid(row=0, column=0, sticky="nsew"); scrollbar_new_y = ttk.Scrollbar(listbox_new_frame, orient=tk.VERTICAL, command=tables_listbox_new.yview); scrollbar_new_y.grid(row=0, column=1, sticky="ns"); tables_listbox_new.config(yscrollcommand=scrollbar_new_y.set); tables_listbox_new.bind("<<ListboxSelect>>", lambda e: view_new_data())

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
    ttk.Button(action_frame, text="Map Tables...", command=map_tables).pack(pady=5, fill=tk.X)
    ttk.Button(action_frame, text="Map Columns...", command=map_columns).pack(pady=5, fill=tk.X)
    ttk.Button(action_frame, text="View Old Table Data", command=view_old_data).pack(pady=5, fill=tk.X)
    ttk.Button(action_frame, text="View New Table Data", command=view_new_data).pack(pady=5, fill=tk.X)
    ttk.Button(action_frame, text="Refresh Tables & Schema", command=select_database).pack(pady=5, fill=tk.X) # Changed text
    ttk.Button(action_frame, text="Help", command=show_help).pack(pady=(10,5), fill=tk.X)
    constraint_frame = ttk.LabelFrame(right_frame, text="Manual Confirmation Gate", padding=10); constraint_frame.pack(pady=10, fill=tk.X)
    constraints = ["Compared Schemas?", "Checked Data Types?", "Verified Mappings?", "Database Backed Up?", "Aware of Potential Loss?", "Proceed with Create?"]
    constraint_vars = [tk.BooleanVar() for _ in constraints]
    for i, constraint in enumerate(constraints): cb = ttk.Checkbutton(constraint_frame, text=constraint, variable=constraint_vars[i], command=check_constraints); cb.grid(row=i, column=0, sticky="w", pady=2, padx=5)
    ttk.Separator(right_frame, orient=tk.HORIZONTAL).pack(pady=10, fill=tk.X)
    create_button = ttk.Button(right_frame, text="CREATE New Table & Copy Data", command=create_new_table_and_copy_data, state=tk.DISABLED); create_button.pack(pady=10, ipady=5, fill=tk.X)

    root.protocol("WM_DELETE_WINDOW", lambda: close_app(root))
    populate_db_combobox() # Populate DB list on startup
    root.mainloop()


# --- Login Window Setup ---

login_window = tk.Tk()
login_window.title("Database Login")
center_window(login_window, 300, 200)
login_window.resizable(False, False)

ttk.Label(login_window, text="MySQL Login", font=("Arial", 12, "bold")).pack(pady=(10, 5))
ttk.Label(login_window, text="Username:").pack(pady=(5,0))
username_entry = ttk.Entry(login_window, width=30); username_entry.pack(pady=2); username_entry.insert(0, "root")
ttk.Label(login_window, text="Password:").pack(pady=(5,0))
password_entry = ttk.Entry(login_window, show="*", width=30); password_entry.pack(pady=2)
connect_button_login = ttk.Button(login_window, text="Connect", command=connect_db); connect_button_login.pack(pady=15)
password_entry.bind("<Return>", lambda event=None: connect_button_login.invoke())
username_entry.focus()

# --- Global Placeholders ---
conn, cursor, root = None, None, None
tables_listbox_old, tables_listbox_new = None, None
schema_tree_old, schema_tree_new = None, None
db_combobox, schema_file_entry = None, None
db_label, tables_frame, db_select_frame = None, None, None
create_button = None
schema_file_path, current_db_name = "", None
table_mappings = {}
constraint_vars = []

login_window.mainloop()