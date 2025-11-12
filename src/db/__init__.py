"""SQLite helper utilities for Flask application."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from flask import current_app, g


def get_database_path() -> Path:
    """Return configured SQLite database path as Path instance."""
    db_path = current_app.config.get('DATABASE')
    if not db_path:
        raise RuntimeError('DATABASE configuration value is missing.')
    return Path(db_path)


def get_db() -> sqlite3.Connection:
    """Return a SQLite connection stored on the application context."""
    if 'db' not in g:
        db_file = get_database_path()
        db_file.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            db_file,
            timeout=current_app.config.get('DATABASE_TIMEOUT', 30),
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


def close_db(_: Any = None) -> None:
    """Close the SQLite connection stored on the application context."""
    conn = g.pop('db', None)
    if conn is not None:
        conn.close()


def execute_script(statements: Iterable[str]) -> None:
    """Execute multiple SQL statements within a single transaction."""
    conn = get_db()
    with conn:
        for sql in statements:
            conn.execute(sql)


def init_app(app) -> None:
    """Register database teardown with the Flask application."""
    app.teardown_appcontext(close_db)


def ensure_database(app) -> None:
    """Ensure the configured SQLite database file exists."""
    db_path = Path(app.config.get('DATABASE', ''))
    if not db_path:
        raise RuntimeError('DATABASE configuration value is missing.')
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.close()
