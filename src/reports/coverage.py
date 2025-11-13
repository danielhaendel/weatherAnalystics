"""Helpers for determining available data coverage."""

from __future__ import annotations

from typing import Dict


def get_coverage(conn) -> Dict[str, str] | None:
    row = conn.execute('SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM daily_kl').fetchone()
    if not row or not row['min_date'] or not row['max_date']:
        return None
    return {'min_date': row['min_date'], 'max_date': row['max_date']}


__all__ = ['get_coverage']
