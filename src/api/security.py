"""API access control backed by stored API keys."""

from __future__ import annotations

import datetime as dt

from flask import current_app, jsonify, request

from ..db import get_db
from .blueprint import api_bp

ALLOWED_PATHS = {
    '/reports/aggregate',
    '/data/coverage',
    '/stations/nearest',
    '/stations_in_radius',
    '/reverse_geocode',
    '/sync_stations',
}
API_KEY_HEADER = 'X-API-Key'


def _resolve_relative_path() -> str:
    path = request.path or '/'
    if path.startswith('/api'):
        path = path[4:] or '/'
    if not path.startswith('/'):
        path = '/' + path
    if len(path) > 1:
        path = path.rstrip('/')
    return path or '/'


def _parse_iso_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        result = dt.datetime.fromisoformat(value)
    except ValueError:
        try:
            result = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=dt.timezone.utc)
    return result


def _is_api_key_valid(token: str | None) -> bool:
    if not token:
        return False
    public_key = current_app.config.get('PUBLIC_API_KEY')
    if public_key and token == public_key:
        return True
    conn = get_db()
    row = conn.execute(
        'SELECT api_key, expires_at FROM api_keys WHERE api_key = ? LIMIT 1',
        (token,),
    ).fetchone()
    if not row:
        return False
    expires_text = row['expires_at']
    expires_at = _parse_iso_datetime(expires_text)
    now = dt.datetime.now(dt.timezone.utc)
    if expires_at is None:
        return True
    return expires_at > now


@api_bp.before_request
def enforce_api_key():
    if request.method == 'OPTIONS':
        return None

    rel_path = _resolve_relative_path()
    if rel_path not in ALLOWED_PATHS:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    provided_key = request.headers.get(API_KEY_HEADER) or request.args.get('api_key')
    if not _is_api_key_valid(provided_key):
        return jsonify({'ok': False, 'error': 'invalid_api_key'}), 401
    return None


__all__ = ['API_KEY_HEADER']
