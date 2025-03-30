import tkinter as tk
import csv
from tkinter import ttk, messagebox, filedialog, Toplevel, Scrollbar
import mysql.connector
import re

def center_window(window, width=600, height=400):
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    window.geometry(f"{width}x{height}+{x}+{y}")

def connect_db():
    global conn, cursor
    username = username_entry.get()
    password = password_entry.get()
    
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            charset="utf8"
        )
        cursor = conn.cursor()
        login_window.destroy()
        init_main_window()
    except mysql.connector.Error as err:
        messagebox.showerror("Connection Error", f"Failed to connect: {err}")

def populate_db_combobox():
    try:
        global cursor, db_combobox
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        db_combobox['values'] = databases
        if databases:
            db_combobox.current(0)
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Failed to fetch databases: {err}")

def select_database():
    global conn, cursor, schema_file_path
    
    dbname = db_combobox.get()
    schema_file_path = schema_file_entry.get()
    
    if not dbname:
        messagebox.showerror("Error", "Please select a database")
        return
    
    if not schema_file_path:
        messagebox.showerror("Error", "Please select a schema file")
        return
    
    try:
        # Use the selected database
        cursor.execute(f"USE {dbname}")
        conn.commit()
        
        # Update UI to show connected database
        db_label.config(text=f"Connected To: {dbname}")
        
        # Enable UI elements
        tables_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Get tables
        get_tables()
        
    except mysql.connector.Error as err:
        messagebox.showerror("Database Error", f"Failed to select database: {err}")

def get_tables():
    try:
        global cursor, conn, tables_listbox_old, tables_listbox_new, schema_file_path
        if not conn.is_connected():
            messagebox.showerror("Error", "Database connection lost.")
            return

        cursor = conn.cursor()

        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]

        tables_listbox_old.delete(0, tk.END)  # Clear previous entries
        tables_listbox_new.delete(0, tk.END)  # Clear previous entries

        parsed_schema = parse_schema_file(schema_file_path)
        old_tables = []
        new_tables = []

        for table in all_tables:
            if table.endswith("_new"):
                new_tables.append(table)
            else:
                old_tables.append(table)

        if old_tables:
            for table in old_tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}_new'")
                new_table_exists = cursor.fetchone() is not None
                index = tables_listbox_old.size()
                tables_listbox_old.insert(tk.END, table)

                if table not in parsed_schema:
                    tables_listbox_old.itemconfig(index, {'fg': 'red'})
                elif not new_table_exists:
                    tables_listbox_old.itemconfig(index)
                else:
                    tables_listbox_old.itemconfig(index, {'fg': 'blue'})

        if new_tables:
            tables_to_remove = []  # Store tables to remove after iteration
            for table in new_tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                new_table_still_exists = cursor.fetchone() is not None
                if new_table_still_exists:
                    index = tables_listbox_new.size()
                    tables_listbox_new.insert(tk.END, table)
                    tables_listbox_new.itemconfig(index)
                else:
                    tables_to_remove.append(table)

            for table in tables_to_remove:
                new_tables.remove(table)  # remove deleted tables from the new table list.

        root.update_idletasks()

    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error fetching tables: {err}")
        print(f"Database Error in get_tables(): {err}")
    except Exception as e:
        print(f"general error in get_tables: {e}")

def show_schema(event):
    selected_indices = tables_listbox_old.curselection()
    if not selected_indices:
        return  # Exit the function if no item is selected

    selected_table = tables_listbox_old.get(selected_indices)

    # Clear previous schema display
    for row in schema_tree_old.get_children():
        schema_tree_old.delete(row)
    for row in schema_tree_new.get_children():
        schema_tree_new.delete(row)

    old_schema = []
    # Fetch schema from MySQL (Old Schema)
    try:
        global cursor, conn, schema_file_path
        if not conn.is_connected():
            messagebox.showerror("Error", "Database connection lost.")
            return

        cursor.execute(f"DESCRIBE {selected_table}")
        schema = cursor.fetchall()
        for column in schema:
            schema_tree_old.insert("", tk.END, values=column)
            old_schema.append(column)
    except mysql.connector.Error:
        messagebox.showerror("Error", f"Error fetching schema for {selected_table}")

    new_schema = parse_schema_file(schema_file_path)
    new_schema_entries = []
    if selected_table in new_schema:
        for col, definition in new_schema[selected_table].items():
            parts = str(definition).split() # convert to string, in case of excel.
            field_type = parts[0] if len(parts) > 0 else ""
            null_value = "NO" if "NOT NULL" in parts else "YES"
            key_value = ""
            default_value = "None"
            extra = ""

            # Handling different key constraints
            if "PRIMARY KEY" in str(definition):
                key_value = "PRI"
            elif "UNIQUE" in str(definition):
                key_value = "UNI"
            elif "CHECK" in str(definition):
                key_value = "CHK"
            elif "FOREIGN KEY" in str(definition):
                key_value = "FK"

            # Extract AUTO_INCREMENT
            if "AUTO_INCREMENT" in str(definition):
                extra = "auto_increment"

            # Extract DEFAULT value (supports numbers, strings, and functions)
            default_match = re.search(r"DEFAULT\s+([\w\'\"]+)", str(definition), re.IGNORECASE)
            if default_match:
                default_value = default_match.group(1).strip("'\"")  # Remove quotes if present

            # Extract CHECK constraints (captures condition inside parentheses)
            check_match = re.search(r"CHECK\s*\((.*?)\)", str(definition), re.IGNORECASE)
            if check_match:
                extra = f"CHECK({check_match.group(1)})"

            # Extract FOREIGN KEY references
            fk_match = re.search(r"REFERENCES\s+(\w+)\((\w+)\)", str(definition), re.IGNORECASE)
            if fk_match:
                extra = f"FK → {fk_match.group(1)}({fk_match.group(2)})"

            row_data = (col, field_type, null_value, key_value, default_value, extra)
            schema_tree_new.insert("", tk.END, values=row_data)
            new_schema_entries.append(row_data)
        highlight_differences(schema_tree_old, schema_tree_new, old_schema, new_schema_entries)

def highlight_differences(tree_old, tree_new, old_schema, new_schema):
    """ Highlight differences in schema trees with improved color coding, including matching columns """

    old_data_dict = {row[0]: row for row in old_schema}  # {column_name: row_data}
    new_data_dict = {row[0]: row for row in new_schema}

    # Highlight differences in old schema tree
    for item in tree_old.get_children():
        values = tree_old.item(item, "values")
        col_name = values[0]

        if col_name in new_data_dict:
            if values != new_data_dict[col_name]:  
                tree_old.item(item, tags=("changed",))  # Yellow for modified values
            else:
                tree_old.item(item, tags=("matching",))  # Gray for matching values
        else:
            tree_old.item(item, tags=("removed",))  # Red for removed columns

    # Highlight differences in new schema tree
    for item in tree_new.get_children():
        values = tree_new.item(item, "values")
        col_name = values[0]

        if col_name in old_data_dict:
            if values != old_data_dict[col_name]:  
                tree_new.item(item, tags=("changed",))  # Yellow for modified values
            else:
                tree_new.item(item, tags=("matching",))  # Gray for matching values
        else:
            tree_new.item(item, tags=("added",))  # Green for newly added columns

    # Define tag styles
    tree_old.tag_configure("changed", background="#FFFACD")  # Light yellow (modified)
    tree_old.tag_configure("removed", background="#FF7276")  # Light red (removed)
    tree_old.tag_configure("matching", background="#D3D3D3")  # Light gray (unchanged)

    tree_new.tag_configure("changed", background="#FFFACD")  # Light yellow (modified)
    tree_new.tag_configure("added", background="#90EE90")  # Light green (newly added)
    tree_new.tag_configure("matching", background="#D3D3D3")  # Light gray (unchanged)

def parse_schema_file(file_path):
    """Parses schema.txt and returns a dictionary {table_name: {column_name: definition}}"""
    schema_dict = {}
    current_table = None

    try:
        with open(file_path, "r") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue  # Skip empty lines

                # Detect table name
                if line.lower().startswith("table:"):
                    current_table = line.split()[1] if " " in line else None
                    schema_dict[current_table] = {}
                    continue

                if current_table:
                    # Capture column definitions, including CHECK and FOREIGN KEY
                    match = re.match(r"([\w_]+)\s+(.+)", line)
                    if match:
                        column_name = match.group(1)
                        attributes = match.group(2)
                        schema_dict[current_table][column_name] = attributes
    except Exception as e:
        messagebox.showerror("Error", f"Error parsing schema file: {e}")
                    
    return schema_dict

def check_constraints():
    """Checks the state of all constraints and enables/disables the create button."""
    all_checked = all(var.get() for var in constraint_vars)
    create_button.config(state=tk.NORMAL if all_checked else tk.DISABLED)

def view_data():
    """Opens a new window to display the selected table's data and adds a download button."""
    selected_index = tables_listbox_old.curselection()
    if not selected_index:
        messagebox.showerror("Error", "Please select a table to view data.")
        return

    selected_table = tables_listbox_old.get(selected_index)

    try:
        global cursor, conn
        if not conn.is_connected():
            messagebox.showerror("Error", "Database connection lost.")
            return

        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {selected_table}")
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        data_window = Toplevel(root)
        data_window.title(f"Data from {selected_table}")

        data_tree = ttk.Treeview(data_window, columns=columns, show="headings")
        for col in columns:
            data_tree.heading(col, text=col)
            data_tree.column(col, width=100)

        for row in data:
            data_tree.insert("", tk.END, values=row)

        data_tree.pack(fill=tk.BOTH, expand=True)

        def download_data():
            """Downloads the data to a CSV file."""
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                try:
                    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                        csv_writer = csv.writer(csvfile)
                        csv_writer.writerow(columns)  # Write header
                        csv_writer.writerows(data)  # Write data
                    messagebox.showinfo("Download Successful", f"Data downloaded to {file_path}")
                except Exception as e:
                    messagebox.showerror("Download Error", f"Failed to download data: {e}")

        download_button = ttk.Button(data_window, text="Download Data", command=download_data)
        download_button.pack(pady=10)

    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Database error: {err}")
    except Exception as e:
        print(f"general error in view data: {e}")

def get_existing_table_schema(table_name, cursor):
    """Retrieve the existing schema of a table from MySQL."""
    cursor.execute(f"DESCRIBE {table_name}")
    existing_schema = {}
    for row in cursor.fetchall():
        column_name, data_type, _, _, _, _ = row
        existing_schema[column_name] = data_type
    return existing_schema

def generate_create_statement(table_name, new_columns):
    """Generates a CREATE TABLE statement ensuring new columns have DEFAULT NULL."""
    new_table_name = f"{table_name}_new"

    # Get existing schema from MySQL
    old_schema = get_existing_table_schema(table_name, cursor)

    column_definitions = []
    for column, datatype in new_columns.items():
        if column in old_schema:
            column_definitions.append(f"`{column}` {datatype}")
        else:
            column_definitions.append(f"`{column}` {datatype} DEFAULT NULL")  # Assign DEFAULT NULL for new columns

    create_statement = f"CREATE TABLE `{new_table_name}` ({', '.join(column_definitions)})"
    return create_statement

def get_common_columns(old_schema, new_schema):
    """Find common columns between old and new schema for data migration."""
    return [col for col in old_schema if col in new_schema]

def copy_data_in_batches(table_name, old_schema, new_schema, cursor, conn):
    """Copy data from old table to new table in batches."""
    new_table_name = f"{table_name}_new"
    common_columns = get_common_columns(old_schema, new_schema)

    if not common_columns:
        print(f"No common columns found, skipping data migration for {table_name}")
        return

    old_columns_str = ", ".join(common_columns)
    new_columns_str = ", ".join(common_columns)

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]

    BATCH_SIZE = 10000  # Define batch size for copying
    for offset in range(0, total_rows, BATCH_SIZE):
        copy_query = f"""
        INSERT INTO {new_table_name} ({new_columns_str})
        SELECT {new_columns_str} FROM {table_name} LIMIT {BATCH_SIZE} OFFSET {offset};
        """
        cursor.execute(copy_query)
        conn.commit()
        #print(f"Copied {offset + BATCH_SIZE if offset + BATCH_SIZE < total_rows else total_rows}/{total_rows} rows from {table_name}")

def create_new_table_and_copy_data():
    """Creates a new table with '_new' suffix and copies data from the old table."""
    selected_index = tables_listbox_old.curselection()

    if not selected_index:
        messagebox.showerror("Error", "Please select a valid table before creating a new one.")
        return

    selected_table = tables_listbox_old.get(selected_index)

    if selected_table.startswith("===") or not selected_table.strip():
        messagebox.showerror("Error", "Please select a valid table (not a heading).")
        return

    new_table_name = f"{selected_table}_new"

    # Check if the new table already exists
    cursor.execute(f"SHOW TABLES LIKE '{new_table_name}'")
    if cursor.fetchone():
        messagebox.showwarning("Warning", f"Table '{new_table_name}' already exists!")
        return

    try:
        # Parse schema file
        new_schema = parse_schema_file(schema_file_path)

        if selected_table not in new_schema:
            messagebox.showerror("Error", f"Table '{selected_table}' not found in schema file.")
            return

        new_columns = new_schema[selected_table]

        # Generate and execute CREATE TABLE statement
        create_statement = generate_create_statement(selected_table, new_columns)
        cursor.execute(create_statement)
        conn.commit()
        print(f"Created new table: {new_table_name}")

        # Get existing schema from MySQL
        old_schema = get_existing_table_schema(selected_table, cursor)

        # Copy data from old table to new table in batches
        copy_data_in_batches(selected_table, old_schema, new_columns, cursor, conn)
        messagebox.showinfo("Success", f"Table '{new_table_name}' created and data copied successfully!")

        # Refresh table list
        get_tables()

    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Database error: {err}")

def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
    if file_path:
        schema_file_entry.delete(0, tk.END)  # Clear previous entry
        schema_file_entry.insert(0, file_path) # Insert the new path

def init_main_window():
    global root, tables_listbox_old, tables_listbox_new, schema_tree_old, schema_tree_new
    global create_button, constraint_vars, db_combobox, schema_file_entry, schema_file_path
    global db_label, tables_frame, db_select_frame
    
    root = tk.Tk()
    root.title("Database Schema Viewer")
    center_window(root, 1200, 600)
    
    # Database selection frame
    db_select_frame = ttk.Frame(root, padding=10)
    db_select_frame.pack(fill=tk.X, padx=10, pady=10)
    
    # Horizontal layout for database selection controls
    db_controls_frame = ttk.Frame(db_select_frame)
    db_controls_frame.pack(fill=tk.X, expand=True)
    
    # First row - Database selection
    ttk.Label(db_controls_frame, text="Database:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
    db_combobox = ttk.Combobox(db_controls_frame, width=30)
    db_combobox.grid(row=0, column=1, sticky="w", padx=5, pady=5)
    
    # Populate database combobox
    populate_db_combobox()
    
    # Second row - Schema file selection
    ttk.Label(db_controls_frame, text="Schema File:").grid(row=0, column=2, sticky="w", padx=5, pady=5)
    schema_file_entry = ttk.Entry(db_controls_frame, width=30)
    schema_file_entry.grid(row=0, column=3, sticky="w", padx=5, pady=5)
    
    browse_button = ttk.Button(db_controls_frame, text="Browse", command=browse_file)
    browse_button.grid(row=0, column=4, padx=5, pady=5)
    
    connect_button = ttk.Button(db_controls_frame, text="Select Database", command=select_database)
    connect_button.grid(row=0, column=5, padx=5, pady=5)
    
    # Configure row and column weights for the db_controls_frame
    for i in range(6):
        db_controls_frame.columnconfigure(i, weight=1)
    
    # Database label
    db_label = ttk.Label(root, text="Not connected to any database", font=("Arial", 12, "bold"))
    db_label.pack(pady=5)
    
    # Main content frame
    tables_frame = ttk.Frame(root)
    
    # Left Frame (Old Tables)
    left_frame = ttk.LabelFrame(tables_frame, text="Old Tables", padding=(10, 10))
    left_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

    tables_listbox_old = tk.Listbox(left_frame, width=30, height=15)
    tables_listbox_old.grid(row=0, column=0, sticky="nsew")
    tables_listbox_old.bind("<<ListboxSelect>>", show_schema)

    scrollbar_old = ttk.Scrollbar(left_frame, command=tables_listbox_old.yview)
    scrollbar_old.grid(row=0, column=1, sticky="ns")
    tables_listbox_old.config(yscrollcommand=scrollbar_old.set)

    ttk.Label(left_frame, text="New Tables").grid(row=2, column=0)

    tables_listbox_new = tk.Listbox(left_frame, width=30, height=15)
    tables_listbox_new.grid(row=3, column=0, sticky="nsew")
    
    scrollbar_new = ttk.Scrollbar(left_frame, command=tables_listbox_new.yview)
    scrollbar_new.grid(row=3, column=1, sticky="ns")
    tables_listbox_new.config(yscrollcommand=scrollbar_new.set)

    # Middle Frame (Schemas)
    middle_frame = ttk.Frame(tables_frame)
    middle_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")

    schema_frame_old = ttk.LabelFrame(middle_frame, text="Original Schema")
    schema_frame_old.grid(row=0, column=0, sticky="nsew", pady=5)

    columns = ("Field", "Type", "Null", "Key", "Default", "Extra")
    schema_tree_old = ttk.Treeview(schema_frame_old, columns=columns, show="headings", height=10)

    for col in columns:
        schema_tree_old.heading(col, text=col)
        schema_tree_old.column(col, width=120, anchor="center")

    schema_tree_old.grid(row=0, column=0, sticky="nsew")
    
    tree_scroll_old = ttk.Scrollbar(schema_frame_old, orient="vertical", command=schema_tree_old.yview)
    tree_scroll_old.grid(row=0, column=1, sticky="ns")
    schema_tree_old.configure(yscrollcommand=tree_scroll_old.set)

    view_data_button = ttk.Button(middle_frame, text="View Data", command=view_data)
    view_data_button.grid(row=1, column=0, pady=5)

    schema_frame_new = ttk.LabelFrame(middle_frame, text="New Schema")
    schema_frame_new.grid(row=2, column=0, sticky="nsew", pady=5)

    schema_tree_new = ttk.Treeview(schema_frame_new, columns=columns, show="headings", height=10)

    for col in columns:
        schema_tree_new.heading(col, text=col)
        schema_tree_new.column(col, width=120, anchor="center")

    schema_tree_new.grid(row=0, column=0, sticky="nsew")
    
    tree_scroll_new = ttk.Scrollbar(schema_frame_new, orient="vertical", command=schema_tree_new.yview)
    tree_scroll_new.grid(row=0, column=1, sticky="ns")
    schema_tree_new.configure(yscrollcommand=tree_scroll_new.set)

    # Right Frame (Constraints)
    right_frame = ttk.LabelFrame(tables_frame, text="Constraints", padding=(10, 10))
    right_frame.grid(row=0, column=2, padx=10, pady=10, sticky="nsew")

    constraints = ["Table Name", "Columns", "Primary Key", "Foreign Key", "Unique Values", "Null Values", "Added Column", "Deleted Column"]
    constraint_vars = [tk.BooleanVar() for _ in constraints]

    for i, constraint in enumerate(constraints):
        ttk.Checkbutton(right_frame, text=constraint, variable=constraint_vars[i], command=check_constraints).grid(row=i, column=0, sticky="w")

    # Create button
    create_button = ttk.Button(tables_frame, text="Create New Table", command=create_new_table_and_copy_data, state=tk.DISABLED)
    create_button.grid(row=1, column=1, pady=10)

    # Refresh Tables Button
    refresh_button = ttk.Button(tables_frame, text="Refresh Tables", command=get_tables)
    refresh_button.grid(row=1, column=0, pady=10)

    # Configure grid weights
    tables_frame.columnconfigure(1, weight=1)
    tables_frame.rowconfigure(0, weight=1)
    
    root.protocol("WM_DELETE_WINDOW", lambda: close_app(root))
    root.mainloop()

def close_app(window):
    try:
        if 'conn' in globals() and conn.is_connected():
            cursor.close()
            conn.close()
    except:
        pass
    finally:
        window.destroy()

# Login Window
login_window = tk.Tk()
login_window.title("Database Login")
center_window(login_window, 300, 200)

ttk.Label(login_window, text="MySQL Database Login", font=("Arial", 12, "bold")).pack(pady=10)

ttk.Label(login_window, text="Username:").pack()
username_entry = ttk.Entry(login_window)
username_entry.pack(pady=5)

ttk.Label(login_window, text="Password:").pack()
password_entry = ttk.Entry(login_window, show="*")
password_entry.pack(pady=5)

ttk.Button(login_window, text="Connect", command=connect_db).pack(pady=10)

# Initialize global schema_file_path
schema_file_path = ""

login_window.mainloop()