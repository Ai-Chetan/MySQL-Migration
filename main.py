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
        tables = [table[0] for table in cursor.fetchall()]
        tables_listbox.delete(0, tk.END)
        for table in tables:
            tables_listbox.insert(tk.END, table)
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error fetching tables: {err}")

def parse_schema_file(file_path):
    """Parse the schema file and extract table structures."""
    tables = {}
    current_table = None

    try:
        with open(file_path, "r") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue

                table_match = re.match(r"Table:\s*(\w+)", line)
                if table_match:
                    current_table = table_match.group(1)
                    tables[current_table] = {}
                elif current_table:
                    parts = line.split()
                    column_name = parts[0]
                    column_definition = " ".join(parts[1:])
                    tables[current_table][column_name] = column_definition
    except FileNotFoundError:
        messagebox.showerror("Error", "Schema file not found!")

    return tables

def show_schema(event):
    selected_table = tables_listbox.get(tables_listbox.curselection())

    # Clear previous schema display
    for row in schema_tree.get_children():
        schema_tree.delete(row)

    # Fetch schema from MySQL
    try:
        cursor.execute(f"DESCRIBE {selected_table}")
        schema = cursor.fetchall()
        for column in schema:
            schema_tree.insert("", tk.END, values=column)
    except mysql.connector.Error:
        messagebox.showerror("Error", f"Error fetching schema for {selected_table}")

    # Fetch schema from schema.txt
    new_schema = parse_schema_file("schema.txt")

    if selected_table in new_schema:  # Fix: Check without "_new"
        schema_tree.insert("", tk.END, values=("-----", "-----", "-----", "-----", "-----", "-----"))
        for col, definition in new_schema[selected_table].items():
            schema_tree.insert("", tk.END, values=(col, definition, "", "", "", ""))

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
    selected_table = tables_listbox.get(tables_listbox.curselection())
    
    if not selected_table:
        messagebox.showerror("Error", "Please select a table first.")
        return

    try:
        # Parse schema file
        new_schema = parse_schema_file("schema.txt")

        if selected_table not in new_schema:
            messagebox.showerror("Error", f"Table {selected_table} not found in schema file.")
            return
        
        new_table_name = f"{selected_table}_new"
        new_columns = new_schema[selected_table]

        # Generate and execute CREATE TABLE statement
        create_statement = generate_create_statement(selected_table, new_columns)
        cursor.execute(create_statement)
        conn.commit()
        print(f"Created new table: {new_table_name}")

        # Get existing schema from MySQL
        old_schema = get_existing_table_schema(selected_table, cursor)
        common_columns = get_common_columns(old_schema, new_columns)

        if not common_columns:
            messagebox.showinfo("Info", f"No common columns found, skipping data migration for {selected_table}")
            return

        # Copy data from old table to new table in batches
        copy_data_in_batches(selected_table, old_schema, new_columns, cursor, conn)
        messagebox.showinfo("Success", f"Table {new_table_name} created and data copied successfully!")

    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Database error: {err}")


def init_main_window(db_name):
    global tables_listbox, schema_tree, root  # Make root global
    root = tk.Tk()  # Define root inside this function
    root.title("Database Schema Viewer")
    center_window(root, 800, 500)
    
    ttk.Label(root, text=f"Connected To: {db_name}", font=("Arial", 14, "bold")).pack(pady=10)
    
    frame = ttk.Frame(root)
    frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    
    tables_listbox = tk.Listbox(frame, width=30, height=15)
    tables_listbox.pack(side=tk.LEFT, padx=10, fill=tk.Y)
    tables_listbox.bind("<<ListboxSelect>>", show_schema)
    
    scrollbar = ttk.Scrollbar(frame, command=tables_listbox.yview)
    scrollbar.pack(side=tk.LEFT, fill=tk.Y)
    tables_listbox.config(yscrollcommand=scrollbar.set)
    
    schema_frame = ttk.Frame(frame)
    schema_frame.pack(side=tk.LEFT, padx=10, fill=tk.BOTH, expand=True)
    
    columns = ("Field", "Type", "Null", "Key", "Default", "Extra")
    schema_tree = ttk.Treeview(schema_frame, columns=columns, show="headings")
    
    for col in columns:
        schema_tree.heading(col, text=col)
        schema_tree.column(col, width=120, anchor="center")
    
    schema_tree.pack(fill=tk.BOTH, expand=True)

    # Move button creation inside the function
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
