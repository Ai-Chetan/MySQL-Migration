"""
main.py -- Entry point for the MySQL Migration Tool.

Usage:
    python main.py
"""
from __future__ import annotations

import sys


def _check_python_version() -> None:
    if sys.version_info < (3, 10):
        sys.exit(
            f"Python 3.10 or newer is required. "
            f"You are running {sys.version}. Aborting."
        )


if __name__ == "__main__":
    _check_python_version()

    from ui.app import AppController

    AppController().run()
