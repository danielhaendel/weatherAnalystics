"""Schema helpers for auth tables."""

from __future__ import annotations

import datetime as dt

from werkzeug.security import generate_password_hash

from ...db import execute_script, get_db

USER_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS users
    (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        username      TEXT UNIQUE NOT NULL,
        password_hash TEXT        NOT NULL,
        is_admin      INTEGER     NOT NULL DEFAULT 0,
        created_at    TEXT        NOT NULL,
        updated_at    TEXT        NOT NULL
    )
    """,
)

API_KEY_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS api_keys
    (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        name       TEXT    NOT NULL,
        api_key    TEXT    NOT NULL,
        created_at TEXT    NOT NULL,
        expires_at TEXT    NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys (user_id)
    """,
)

USER_REQUIRED_COLUMNS = (
    ('is_admin', 'INTEGER NOT NULL DEFAULT 0'),
)


def ensure_user_columns():
    conn = get_db()
    existing = {row['name'] for row in conn.execute('PRAGMA table_info(users)')}
    missing = [(column, col_type) for column, col_type in USER_REQUIRED_COLUMNS if column not in existing]
    for column, col_type in missing:
        conn.execute(f'ALTER TABLE users ADD COLUMN {column} {col_type}')
    if missing:
        conn.commit()


def ensure_default_admin():
    conn = get_db()
    now = dt.datetime.utcnow().isoformat(timespec='seconds')
    with conn:
        row = conn.execute(
            'SELECT id FROM users WHERE username = ? LIMIT 1',
            ('admin',),
        ).fetchone()
        if row is None:
            conn.execute(
                'INSERT INTO users (username, password_hash, is_admin, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
                ('admin', generate_password_hash('admin'), 1, now, now),
            )
        else:
            conn.execute(
                'UPDATE users SET is_admin = 1 WHERE username = ? AND (is_admin IS NULL OR is_admin = 0)',
                ('admin',),
            )


def initialize_auth_schema():
    execute_script((*USER_SCHEMA, *API_KEY_SCHEMA))
    ensure_user_columns()
    ensure_default_admin()


__all__ = [
    'API_KEY_SCHEMA',
    'USER_SCHEMA',
    'USER_REQUIRED_COLUMNS',
    'ensure_default_admin',
    'ensure_user_columns',
    'initialize_auth_schema',
]
