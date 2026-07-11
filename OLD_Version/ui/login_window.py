"""
ui/login_window.py
------------------
Login dialog — collects MySQL host, port, username and password, then
establishes a DatabaseManager connection before handing off to the
application controller.

Design Decisions:
    * This window is a plain Tkinter Tk() root (not Toplevel), because it
      is the first window the user sees and the event loop must start here.
    * The window is not resizable to prevent layout issues.
    * "Return" key submits the form for keyboard-first users.
    * Password field echoes "*" and is never logged.
    * Host and port are configurable here (via config.py defaults) so power
      users who need to connect to a remote server can do so without editing
      code.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from config import CONFIG
from core.database import DatabaseManager, DatabaseError
from logger import get_logger
from ui.utils import center_window, set_window_icon

log = get_logger(__name__)

OnConnectCallback = Callable[[DatabaseManager], None]


class LoginWindow:
    """
    Modal login dialog shown at application startup.

    Args:
        on_connect: Callback invoked with the connected DatabaseManager when
                    login succeeds. The callback is responsible for opening
                    the next window and starting a new event loop.
    """

    def __init__(self, on_connect: OnConnectCallback) -> None:
        self._on_connect = on_connect
        self._root = tk.Tk()
        self._root.title(f"{CONFIG.app_name} — Login")
        self._root.resizable(False, False)
        set_window_icon(self._root)

        self._host_var = tk.StringVar(value=CONFIG.db.host)
        self._port_var = tk.StringVar(value=str(CONFIG.db.port))
        self._user_var = tk.StringVar(value="root")
        self._pass_var = tk.StringVar()

        self._build_ui()
        center_window(self._root, 340, 290)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        pad = {"padx": 10, "pady": 4}

        header = ttk.Label(
            self._root,
            text="MySQL Connection",
            font=(CONFIG.ui.font_family, 13, "bold"),
        )
        header.pack(pady=(16, 8))

        form = ttk.Frame(self._root, padding=(16, 0))
        form.pack(fill=tk.X)
        form.columnconfigure(1, weight=1)

        # Host
        ttk.Label(form, text="Host:").grid(row=0, column=0, sticky="w", **pad)
        host_entry = ttk.Entry(form, textvariable=self._host_var, width=22)
        host_entry.grid(row=0, column=1, sticky="ew", **pad)

        # Port
        ttk.Label(form, text="Port:").grid(row=1, column=0, sticky="w", **pad)
        port_entry = ttk.Entry(form, textvariable=self._port_var, width=8)
        port_entry.grid(row=1, column=1, sticky="w", **pad)

        # Username
        ttk.Label(form, text="Username:").grid(row=2, column=0, sticky="w", **pad)
        user_entry = ttk.Entry(form, textvariable=self._user_var, width=22)
        user_entry.grid(row=2, column=1, sticky="ew", **pad)
        user_entry.focus_set()

        # Password
        ttk.Label(form, text="Password:").grid(row=3, column=0, sticky="w", **pad)
        pass_entry = ttk.Entry(form, textvariable=self._pass_var, show="*", width=22)
        pass_entry.grid(row=3, column=1, sticky="ew", **pad)
        pass_entry.bind("<Return>", lambda _: self._on_submit())

        ttk.Separator(self._root, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10, padx=10)

        connect_btn = ttk.Button(
            self._root,
            text="Connect",
            command=self._on_submit,
            width=16,
        )
        connect_btn.pack()
        self._connect_btn = connect_btn

        self._status_var = tk.StringVar()
        status_lbl = ttk.Label(
            self._root,
            textvariable=self._status_var,
            foreground="red",
            wraplength=300,
        )
        status_lbl.pack(pady=(6, 10))

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _on_submit(self) -> None:
        host = self._host_var.get().strip()
        port_str = self._port_var.get().strip()
        user = self._user_var.get().strip()
        password = self._pass_var.get()

        # --- Validation ---
        if not host:
            self._set_status("Host is required.")
            return
        try:
            port = int(port_str)
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            self._set_status("Port must be a number between 1 and 65535.")
            return
        if not user:
            self._set_status("Username is required.")
            return

        self._set_status("Connecting…", color="black")
        self._connect_btn.config(state=tk.DISABLED)
        self._root.update_idletasks()

        db = DatabaseManager(
            host=host,
            port=port,
            user=user,
            password=password,
            charset=CONFIG.db.charset,
            connect_timeout=CONFIG.db.connect_timeout,
        )
        try:
            db.connect()
        except DatabaseError as exc:
            log.warning("Login failed: %s", exc)
            self._set_status(str(exc))
            self._connect_btn.config(state=tk.NORMAL)
            return

        # Success — destroy this window and invoke callback
        log.info("Login successful (user=%s, host=%s:%d).", user, host, port)
        self._root.destroy()
        self._on_connect(db)

    def _set_status(self, msg: str, color: str = "red") -> None:
        self._status_var.set(msg)
        for widget in self._root.pack_slaves():
            if isinstance(widget, ttk.Label) and widget.cget("textvariable") == str(self._status_var):
                widget.config(foreground=color)
                break

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the Tkinter event loop for this window."""
        self._root.mainloop()
