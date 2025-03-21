import tkinter as tk
from tkinter import ttk, messagebox
import mysql.connector

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

def show_schema(event):
    selected_table = tables_listbox.get(tables_listbox.curselection())
    try:
        cursor.execute(f"DESCRIBE {selected_table}")
        schema = cursor.fetchall()
        schema_text.delete(1.0, tk.END)
        schema_text.insert(tk.END, f"Schema of {selected_table}:\n\n")
        for column in schema:
            schema_text.insert(tk.END, f"{column[0]} - {column[1]}\n")
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error fetching schema: {err}")

def init_main_window(db_name):
    global tables_listbox, schema_text
    root = tk.Tk()
    root.title("Database Schema Viewer")
    root.geometry("600x400")
    
    ttk.Label(root, text=f"Connected to: {db_name}", font=("Arial", 14, "bold")).pack(pady=10)
    
    frame = ttk.Frame(root)
    frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    
    tables_listbox = tk.Listbox(frame, width=30, height=15)
    tables_listbox.pack(side=tk.LEFT, padx=10, fill=tk.Y)
    tables_listbox.bind("<<ListboxSelect>>", show_schema)
    
    scrollbar = ttk.Scrollbar(frame, command=tables_listbox.yview)
    scrollbar.pack(side=tk.LEFT, fill=tk.Y)
    tables_listbox.config(yscrollcommand=scrollbar.set)
    
    schema_text = tk.Text(frame, width=50, height=15, wrap=tk.WORD)
    schema_text.pack(side=tk.LEFT, padx=10, fill=tk.BOTH, expand=True)
    
    get_tables()
    
    root.mainloop()
    cursor.close()
    conn.close()

# Login Window
login_window = tk.Tk()
login_window.title("Database Login")
login_window.geometry("300x250")

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