"""Reporting helper functions for the API layer."""

from __future__ import annotations

from ...db import get_db
from ...reports import ReportError, generate_report, get_coverage


def fetch_data_coverage() -> dict:
    conn = get_db()
    coverage = get_coverage(conn)
    if not coverage:
        raise ReportError('no_data')
    return coverage


def build_aggregate_report(lat: float, lon: float, radius: float, start_date: str, end_date: str, granularity: str) -> dict:
    conn = get_db()
    return generate_report(conn, lat, lon, radius, start_date, end_date, granularity)


__all__ = ['fetch_data_coverage', 'build_aggregate_report']
