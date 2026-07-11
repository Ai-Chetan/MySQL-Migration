"""
ui/dialogs/help_dialog.py
-------------------------
Contextual help window. Content is structured as a list of (text, tag)
pairs so it can be easily updated without touching UI layout code.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from config import CONFIG
from ui.utils import center_window


_HELP_CONTENT = [
    # (text, tag_name_or_tuple)
    ("MySQL Migration Tool — Help\n", "h1"),
    ("\nOverview\n", "h2"),
    ("This tool helps migrate MySQL database schemas by comparing existing tables against a new schema definition file, creating target tables, and copying data with type-safe transformations.\n\n", ""),

    ("Quick Start\n", "h2"),
    ("1. Login — connect to MySQL.\n", ("bold", "li")),
    ("2. Select a Database and Schema File, then click 'Load DB & Schema'.\n", "li"),
    ("3. Review the colour-coded Source Tables list (see Colours section).\n", "li"),
    ("4. Map tables using the buttons on the right panel.\n", "li"),
    ("5. Tick all safety checklist items and click 'CREATE Table(s) & Copy Data'.\n\n", "li"),

    ("Mapping Types\n", "h2"),
    ("Single:  ", "bold"), ("One source table → one target table.\n", ""),
    ("Split:   ", "bold"), ("One source table → multiple target tables.\n", ""),
    ("Merge:   ", "bold"), ("Multiple source tables → one target table via SQL JOIN.\n\n", ""),

    ("Source Table Colours\n", "h2"),
    ("Red       ", "colour_red"),    ("Not mapped, name not in schema file.\n", "li"),
    ("Orange    ", "colour_orange"), ("Not mapped, name found in schema file.\n", "li"),
    ("Purple    ", "colour_purple"), ("Mapped, but target name missing in schema file.\n", "li"),
    ("Blue      ", "colour_blue"),   ("Mapped (single), target exists, _new table not yet created.\n", "li"),
    ("Dark Cyan ", "colour_cyan"),   ("Mapped (split), targets exist, _new tables not all created.\n", "li"),
    ("Dark Green", "colour_green"),  ("Merge mapping entry.\n", "li"),
    ("Black     ", "bold"),          ("Fully migrated — _new table(s) exist.\n\n", "li"),

    ("Schema File Format\n", "h2"),
    ("Table: MyTable\n  id    INT AUTO_INCREMENT PRIMARY KEY\n  name  VARCHAR(100) NOT NULL\n\n", "code"),
    ("Lines starting with # or -- are treated as comments.\n\n", ""),

    ("Data Type Safety\n", "h2"),
    ("SAFE   — Conversion will succeed without data loss (e.g. INT → BIGINT).\n", "li"),
    ("LOSSY  — May truncate or lose precision (e.g. FLOAT → INT). Tool asks for confirmation.\n", "li"),
    ("UNSAFE — Likely to fail or corrupt data (e.g. TEXT → INT). Migration is blocked.\n\n", "li"),

    ("Manual Scripts\n", "h2"),
    ("Select a source table and click 'Generate Manual Script…' to create a standalone\n"
     "Python script template. Open the file, fill in the # TODO: sections, and run it\n"
     "directly from the command line (python <script>.py).\n\n", ""),

    ("⚠  Always back up your database before running migrations.\n", "warning"),
]


class HelpDialog:
    """Read-only help window."""

    def __init__(self, parent: tk.Widget) -> None:
        win = tk.Toplevel(parent)
        win.title("Help — MySQL Migration Tool")
        win.transient(parent)
        center_window(win, 760, 620)

        # Scrollable text area
        frame = ttk.Frame(win)
        frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        text = tk.Text(
            frame,
            wrap=tk.WORD,
            padx=12, pady=10,
            bd=0,
            relief=tk.FLAT,
            font=(CONFIG.ui.font_family, CONFIG.ui.font_size),
        )
        sb = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=text.yview)
        text.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        text.pack(fill=tk.BOTH, expand=True)

        # Configure tags
        tag_cfg = {
            "h1": {"font": (CONFIG.ui.font_family, 14, "bold"), "spacing3": 6},
            "h2": {"font": (CONFIG.ui.font_family, 11, "bold"), "spacing1": 10, "spacing3": 3},
            "bold": {"font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "li": {"lmargin1": 14, "lmargin2": 28},
            "code": {
                "font": (CONFIG.ui.mono_font, 9),
                "background": "#f4f4f4",
                "lmargin1": 16, "lmargin2": 16,
            },
            "warning": {"foreground": "#cc0000", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_red": {"foreground": "red", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_orange": {"foreground": "darkorange", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_purple": {"foreground": "purple", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_blue": {"foreground": "royalblue", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_cyan": {"foreground": "darkcyan", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
            "colour_green": {"foreground": "darkgreen", "font": (CONFIG.ui.font_family, CONFIG.ui.font_size, "bold")},
        }
        for name, cfg in tag_cfg.items():
            text.tag_configure(name, **cfg)

        for content, tags in _HELP_CONTENT:
            text.insert(tk.END, content, tags)

        text.config(state=tk.DISABLED)

        ttk.Button(win, text="Close", command=win.destroy, width=10).pack(pady=(0, 8))
