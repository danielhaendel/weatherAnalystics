"""API-key management helpers."""

from __future__ import annotations

import datetime as dt
import secrets
from typing import List

from ...db import get_db


def list_api_keys(user_id: int):
    conn = get_db()
    return conn.execute(
        '''
        SELECT id, name, api_key, created_at, expires_at
        FROM api_keys
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        ''',
        (user_id,),
    ).fetchall()


def create_api_key(user_id: int, name: str, expires_in_days: int = 90) -> str:
    conn = get_db()
    now = dt.datetime.utcnow()
    expires = now + dt.timedelta(days=expires_in_days)
    key_value = secrets.token_urlsafe(32)
    with conn:
        conn.execute(
            '''
            INSERT INTO api_keys (user_id, name, api_key, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (
                user_id,
                name,
                key_value,
                now.isoformat(timespec='seconds'),
                expires.isoformat(timespec='seconds'),
            ),
        )
    return key_value


def delete_api_key(user_id: int, key_id: int) -> bool:
    conn = get_db()
    with conn:
        cur = conn.execute(
            'DELETE FROM api_keys WHERE id = ? AND user_id = ?',
            (key_id, user_id),
        )
    return cur.rowcount > 0


__all__ = ['create_api_key', 'delete_api_key', 'list_api_keys']
