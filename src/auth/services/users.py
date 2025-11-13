"""User-related helpers and repository functions."""

from __future__ import annotations

import datetime as dt
from typing import Optional
from urllib.parse import urljoin, urlparse

from flask import current_app, request
from flask_login import UserMixin, current_user
from werkzeug.security import check_password_hash, generate_password_hash

from ...db import get_db


class User(UserMixin):
    """Lightweight user wrapper that works with Flask-Login."""

    def __init__(self, user_id: int, username: str, password_hash: str, is_admin: int) -> None:
        self.id = str(user_id)
        self.username = username
        self.password_hash = password_hash
        self.is_admin = bool(is_admin)

    @classmethod
    def from_row(cls, row) -> Optional['User']:
        if row is None:
            return None
        is_admin = _row_is_admin(row)
        return cls(row['id'], row['username'], row['password_hash'], is_admin)

    @classmethod
    def get_by_id(cls, user_id: str) -> Optional['User']:
        conn = get_db()
        row = conn.execute(
            'SELECT id, username, password_hash, is_admin FROM users WHERE id = ?',
            (user_id,),
        ).fetchone()
        return cls.from_row(row)

    @classmethod
    def get_by_username(cls, username: str) -> Optional['User']:
        conn = get_db()
        row = conn.execute(
            'SELECT id, username, password_hash, is_admin FROM users WHERE username = ?',
            (username,),
        ).fetchone()
        return cls.from_row(row)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def update_password(self, password: str) -> None:
        conn = get_db()
        now = dt.datetime.utcnow().isoformat(timespec='seconds')
        with conn:
            conn.execute(
                'UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?',
                (generate_password_hash(password), now, self.id),
            )


def authenticate_user(username: str, password: str) -> Optional[User]:
    user = User.get_by_username(username)
    if user and user.check_password(password):
        return user
    return None


def create_user_account(username: str, password: str, *, is_admin: bool = False) -> User:
    conn = get_db()
    now = dt.datetime.utcnow().isoformat(timespec='seconds')
    with conn:
        conn.execute(
            'INSERT INTO users (username, password_hash, is_admin, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
            (username, generate_password_hash(password), int(is_admin), now, now),
        )
    user = User.get_by_username(username)
    if not user:
        raise RuntimeError('Failed to create user.')
    return user


def load_user(user_id: str) -> Optional[User]:
    return User.get_by_id(user_id)


def is_current_user_admin() -> bool:
    try:
        user_id = int(getattr(current_user, 'id', 0))
    except (TypeError, ValueError):
        return False
    conn = get_db()
    row = conn.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,)).fetchone()
    return bool(row and _row_is_admin(row))


def is_safe_redirect(target: Optional[str]) -> bool:
    if not target:
        return False
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return (
        test_url.scheme in {'http', 'https'}
        and ref_url.netloc == test_url.netloc
    )


def _row_is_admin(row) -> int:
    value = None
    try:
        value = row['is_admin']
    except (KeyError, IndexError, TypeError):
        pass
    if value in (None, ''):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


__all__ = [
    'User',
    'authenticate_user',
    'create_user_account',
    'is_current_user_admin',
    'is_safe_redirect',
    'load_user',
]
