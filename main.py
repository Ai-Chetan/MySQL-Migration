import tkinter as tk
from tkinter import ttk, messagebox
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
    database = dbname_entry.get()
    
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            database=database,
            charset="utf8"
        )
        cursor = conn.cursor()
        login_window.destroy()
        init_main_window(database)
    except mysql.connector.Error as err:
        messagebox.showerror("Connection Error", f"Failed to connect: {err}")

def get_tables():
    try:
        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]
        tables_listbox_old.delete(0, tk.END)  # Clear previous entries
        tables_listbox_new.delete(0, tk.END)  # Clear previous entries
        parsed_schema = parse_schema_file("schema.txt")

        old_tables = []
        new_tables = []

        # Separate old and new tables
        for table in all_tables:
            if table.endswith("_new"):
                new_tables.append(table)
            else:
                old_tables.append(table)

        # Display Old Tables
        if old_tables:
            for table in old_tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}_new'")
                new_table_exists = cursor.fetchone() is not None

                index = tables_listbox_old.size()
                tables_listbox_old.insert(tk.END, table)

                if table not in parsed_schema:
                    tables_listbox_old.itemconfig(index, {'fg': 'red'})  # Not in schema.txt
                elif not new_table_exists:
                    tables_listbox_old.itemconfig(index)  # In schema.txt, but _new not created
                else:
                    tables_listbox_old.itemconfig(index, {'fg': 'blue'})  # _new table exists

        # Display New Tables
        if new_tables:
            for table in new_tables:
                index = tables_listbox_new.size()
                tables_listbox_new.insert(tk.END, table)
                tables_listbox_new.itemconfig(index)  # _new tables

        # Disable create button for new tables
        selected_index = tables_listbox_old.curselection()
        if selected_index:
            selected_table = tables_listbox_old.get(selected_index)
            cursor.execute(f"SHOW TABLES LIKE '{selected_table}_new'")
            if cursor.fetchone():
                create_button.config(state=tk.DISABLED)
            else:
                create_button.config(state=tk.NORMAL)

    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error fetching tables: {err}")

def show_schema(event):
    # Check if an item is selected
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
        cursor.execute(f"DESCRIBE {selected_table}")
        schema = cursor.fetchall()
        for column in schema:
            schema_tree_old.insert("", tk.END, values=column)
            old_schema.append(column)  
    except mysql.connector.Error:
        messagebox.showerror("Error", f"Error fetching schema for {selected_table}")

    new_schema = parse_schema_file("schema.txt")
    new_schema_entries = []
    if selected_table in new_schema:
        for col, definition in new_schema[selected_table].items():
            parts = definition.split()
            field_type = parts[0] if len(parts) > 0 else ""
            null_value = "NO" if "NOT NULL" in parts else "YES"
            key_value = ""
            default_value = ""
            extra = ""

            # Handling different key constraints
            if "PRIMARY KEY" in definition:
                key_value = "PRI"
            elif "UNIQUE" in definition:
                key_value = "UNI"
            elif "CHECK" in definition:
                key_value = "CHK"
            elif "FOREIGN KEY" in definition:
                key_value = "FK"

            # Extract AUTO_INCREMENT
            if "AUTO_INCREMENT" in definition:
                extra = "AUTO_INCREMENT"

            # Extract DEFAULT value (supports numbers, strings, and functions)
            default_match = re.search(r"DEFAULT\s+([\w\'\"]+)", definition, re.IGNORECASE)
            if default_match:
                default_value = default_match.group(1).strip("'\"")  # Remove quotes if present

            # Extract CHECK constraints (captures condition inside parentheses)
            check_match = re.search(r"CHECK\s*\((.*?)\)", definition, re.IGNORECASE)
            if check_match:
                extra = f"CHECK({check_match.group(1)})"

            # Extract FOREIGN KEY references
            fk_match = re.search(r"REFERENCES\s+(\w+)\((\w+)\)", definition, re.IGNORECASE)
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

    print(schema_dict)
    return schema_dict


def get_existing_table_schema(table_name, cursor):
    """Retrieve the existing schema of a table from MySQL."""
    cursor.execute(f"DESCRIBE {table_name}")
    existing_schema = {}
    for row in cursor.fetchall():
        column_name, data_type, _, _, _, _ = row
        existing_schema[column_name] = data_type
    return existing_schema

def generate_create_statement(table_name, columns):
    """Generate CREATE TABLE SQL statement with `_new` suffix."""
    new_table_name = f"{table_name}_new"
    columns_sql = ",\n    ".join([f"{col} {definition}" for col, definition in columns.items()])
    return f"CREATE TABLE {new_table_name} (\n    {columns_sql}\n);"

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
        print(f"Copied {offset + BATCH_SIZE if offset + BATCH_SIZE < total_rows else total_rows}/{total_rows} rows from {table_name}")

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
        new_schema = parse_schema_file("schema.txt")

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

def init_main_window(db_name):
    global tables_listbox_old, tables_listbox_new, schema_tree_old, schema_tree_new, root  # Make root global
    root = tk.Tk()  # Define root inside this function
    root.title("Database Schema Viewer")
    center_window(root, 800, 600)
    
    ttk.Label(root, text=f"Connected To: {db_name}", font=("Arial", 14, "bold")).pack(pady=10)
    
    # Frame for Old Tables
    old_frame = ttk.LabelFrame(root, text="Old Tables", padding=(10, 10))
    old_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    tables_listbox_old = tk.Listbox(old_frame, width=30, height=15)
    tables_listbox_old.pack(side=tk.LEFT, padx=10, fill=tk.Y)
    tables_listbox_old.bind("<<ListboxSelect>>", show_schema)
    
    scrollbar_old = ttk.Scrollbar(old_frame, command=tables_listbox_old.yview)
    scrollbar_old.pack(side=tk.LEFT, fill=tk.Y)
    tables_listbox_old.config(yscrollcommand=scrollbar_old.set)

    schema_frame_old = ttk.Frame(old_frame)
    schema_frame_old.pack(side=tk.LEFT, padx=10, fill=tk.BOTH, expand=True)
    
    columns = ("Field", "Type", "Null", "Key", "Default", "Extra")
    schema_tree_old = ttk.Treeview(schema_frame_old, columns=columns, show="headings")
    
    for col in columns:
        schema_tree_old.heading(col, text=col)
        schema_tree_old.column(col, width=120, anchor="center")
    
    schema_tree_old.pack(fill=tk.BOTH, expand=True)

    # Frame for New Tables
    new_frame = ttk.LabelFrame(root, text="New Tables", padding=(10, 10))
    new_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    tables_listbox_new = tk.Listbox(new_frame, width=30, height=15)
    tables_listbox_new.pack(side=tk.LEFT, padx=10, fill=tk.Y)
    
    scrollbar_new = ttk.Scrollbar(new_frame, command=tables_listbox_new.yview)
    scrollbar_new.pack(side=tk.LEFT, fill=tk.Y)
    tables_listbox_new.config(yscrollcommand=scrollbar_new.set)

    schema_frame_new = ttk.Frame(new_frame)
    schema_frame_new.pack(side=tk.LEFT, padx=10, fill=tk.BOTH, expand=True)
    
    schema_tree_new = ttk.Treeview(schema_frame_new, columns=columns, show="headings")
    
    for col in columns:
        schema_tree_new.heading(col, text=col)
        schema_tree_new.column(col, width=120, anchor="center")
    
    schema_tree_new.pack(fill=tk.BOTH, expand=True)

    # Create button
    global create_button
    create_button = ttk.Button(root, text="Create New Table", command=create_new_table_and_copy_data)
    create_button.pack(pady=10)

    get_tables()
    root.mainloop()  # Start the GUI loop
    cursor.close()
    conn.close()

# Login Window
login_window = tk.Tk()
login_window.title("Database Login")
center_window(login_window, 400, 300)

ttk.Label(login_window, text="MySQL Database Login", font=("Arial", 12, "bold")).pack(pady=10)

ttk.Label(login_window, text="Username:").pack()
username_entry = ttk.Entry(login_window)
username_entry.pack(pady=5)

ttk.Label(login_window, text="Password:").pack()
password_entry = ttk.Entry(login_window, show="*")
password_entry.pack(pady=5)

ttk.Label(login_window, text="Database Name:").pack()
dbname_entry = ttk.Entry(login_window)
dbname_entry.pack(pady=5)

ttk.Button(login_window, text="Connect", command=connect_db).pack(pady=10)
login_window.mainloop()