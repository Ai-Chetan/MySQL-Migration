"""
ui/utils.py
-----------
Shared UI utilities: window centering, theming, icon loading, tooltip helpers.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from config import CONFIG
from logger import get_logger

log = get_logger(__name__)


def center_window(window: tk.Wm, width: int, height: int) -> None:
    """
    Centre *window* on the primary screen.

    Args:
        window: A Tk root window or Toplevel.
        width:  Desired window width in pixels.
        height: Desired window height in pixels.
    """
    window.update_idletasks()
    sw = window.winfo_screenwidth()
    sh = window.winfo_screenheight()
    x = max(0, (sw - width) // 2)
    y = max(0, (sh - height) // 2)
    window.geometry(f"{width}x{height}+{x}+{y}")


def set_window_icon(window: tk.Wm) -> None:
    """Attempt to set the application window icon (silently ignores failures)."""
    try:
        import os
        icon_path = os.path.join(os.path.dirname(__file__), "..", "assets", "icon.ico")
        if os.path.exists(icon_path):
            window.iconbitmap(icon_path)
    except Exception:
        pass


def configure_style() -> None:
    """
    Configure a cleaner ttk Style for all windows.

    Called once at application startup.  Uses the platform default theme
    as a base and only overrides specific, impactful settings.
    """
    style = ttk.Style()
    # 'clam' is the most cross-platform theme that supports colour overrides
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass  # Theme not available on this platform

    # Treeview row height
    style.configure("Treeview", rowheight=22, font=(CONFIG.ui.font_family, CONFIG.ui.font_size))
    style.configure("Treeview.Heading", font=(CONFIG.ui.font_family, CONFIG.ui.font_size, "bold"))

    # Buttons with slightly more padding
    style.configure("TButton", padding=(8, 4))
    style.configure("Action.TButton", padding=(10, 6), font=(CONFIG.ui.font_family, CONFIG.ui.font_size, "bold"))

    # Status bar
    style.configure("Status.TLabel", background="#f0f0f0", relief=tk.SUNKEN, padding=3)


class ToolTip:
    """
    Simple hover tooltip for any Tkinter widget.

    Usage::

        btn = ttk.Button(root, text="Click me")
        ToolTip(btn, "This button does something useful.")
    """

    def __init__(self, widget: tk.Widget, text: str, delay_ms: int = 600) -> None:
        self._widget = widget
        self._text = text
        self._delay = delay_ms
        self._tip_window: tk.Toplevel | None = None
        self._after_id: str | None = None

        widget.bind("<Enter>", self._schedule)
        widget.bind("<Leave>", self._cancel)
        widget.bind("<ButtonPress>", self._cancel)

    def _schedule(self, _event: Any) -> None:
        self._cancel(None)
        self._after_id = self._widget.after(self._delay, self._show)

    def _cancel(self, _event: Any) -> None:
        if self._after_id:
            self._widget.after_cancel(self._after_id)
            self._after_id = None
        self._hide()

    def _show(self) -> None:
        if self._tip_window:
            return
        x = self._widget.winfo_rootx() + 20
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
        self._tip_window = tw = tk.Toplevel(self._widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        lbl = tk.Label(
            tw,
            text=self._text,
            justify=tk.LEFT,
            background="#ffffe0",
            relief=tk.SOLID,
            borderwidth=1,
            font=(CONFIG.ui.font_family, CONFIG.ui.font_size - 1),
            wraplength=260,
        )
        lbl.pack(ipadx=4, ipady=2)

    def _hide(self) -> None:
        if self._tip_window:
            self._tip_window.destroy()
            self._tip_window = None


class ProgressDialog:
    """
    Non-blocking progress dialog shown during long-running operations.

    Usage::

        dlg = ProgressDialog(parent, "Copying data…")
        dlg.show()
        # … pass dlg.update as progress_cb …
        dlg.close()
    """

    def __init__(self, parent: tk.Widget, title: str = "Working…") -> None:
        self._parent = parent
        self._title = title
        self._win: tk.Toplevel | None = None
        self._label_var = tk.StringVar(value="")
        self._progress_var = tk.DoubleVar(value=0.0)

    def show(self) -> None:
        self._win = win = tk.Toplevel(self._parent)
        win.title(self._title)
        win.resizable(False, False)
        win.transient(self._parent)
        win.grab_set()
        center_window(win, 380, 130)

        ttk.Label(win, textvariable=self._label_var, wraplength=340).pack(pady=(16, 4), padx=16)
        bar = ttk.Progressbar(
            win,
            variable=self._progress_var,
            maximum=100,
            mode="determinate",
            length=340,
        )
        bar.pack(padx=16, pady=8)
        win.update_idletasks()

    def update(self, message: str, current: int, total: int) -> None:
        self._label_var.set(message)
        if total > 0:
            self._progress_var.set(current / total * 100)
        else:
            self._progress_var.set(0)
        if self._win:
            self._win.update_idletasks()

    def close(self) -> None:
        if self._win:
            self._win.grab_release()
            self._win.destroy()
            self._win = None


def scrolled_listbox(
    parent: tk.Widget,
    **listbox_kwargs: Any,
) -> tuple[tk.Listbox, ttk.Scrollbar]:
    """
    Create a Listbox with a vertical scrollbar packed inside *parent*.

    Returns:
        (Listbox, Scrollbar) tuple — both already grid/packed in *parent*.
    """
    frame = ttk.Frame(parent)
    lb = tk.Listbox(frame, **listbox_kwargs)
    sb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=lb.yview)
    lb.config(yscrollcommand=sb.set)
    lb.grid(row=0, column=0, sticky="nsew")
    sb.grid(row=0, column=1, sticky="ns")
    frame.rowconfigure(0, weight=1)
    frame.columnconfigure(0, weight=1)
    return lb, sb, frame  # type: ignore[return-value]


def scrolled_text(
    parent: tk.Widget,
    **text_kwargs: Any,
) -> tuple[tk.Text, ttk.Scrollbar]:
    """Create a Text widget with vertical + horizontal scrollbars."""
    frame = ttk.Frame(parent)
    text = tk.Text(frame, **text_kwargs)
    sb_y = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=text.yview)
    sb_x = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=text.xview)
    text.config(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
    text.grid(row=0, column=0, sticky="nsew")
    sb_y.grid(row=0, column=1, sticky="ns")
    sb_x.grid(row=1, column=0, sticky="ew")
    frame.rowconfigure(0, weight=1)
    frame.columnconfigure(0, weight=1)
    return text, frame  # type: ignore[return-value]
