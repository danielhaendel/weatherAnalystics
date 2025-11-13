"""Main entry point for assembling aggregate reports."""

from __future__ import annotations

from datetime import date
from typing import Dict

from .aggregations import _build_aggregate_query, _round_or_none, _station_period_breakdown
from .coverage import get_coverage
from .errors import ReportError
from .geo import stations_within_radius


def _parse_date(value: str) -> date:
    try:
        parts = [int(x) for x in value.split('-')]
        return date(parts[0], parts[1], parts[2])
    except Exception as exc:  # pragma: no cover - defensive
        raise ReportError('invalid_dates') from exc


def generate_report(conn, lat: float, lon: float, radius: float,
                    start_date: str, end_date: str, granularity: str) -> Dict:
    coverage = get_coverage(conn)
    if not coverage:
        raise ReportError('no_data')

    start_obj = _parse_date(start_date)
    end_obj = _parse_date(end_date)
    if start_obj > end_obj:
        raise ReportError('invalid_range')

    if start_date < coverage['min_date'] or end_date > coverage['max_date']:
        raise ReportError('out_of_bounds')

    stations = stations_within_radius(conn, lat, lon, radius)
    if not stations:
        raise ReportError('no_stations')

    query = _build_aggregate_query(granularity, len(stations))
    params = [*(station['station_id'] for station in stations), start_date, end_date]
    rows = conn.execute(query, params).fetchall()
    if not rows:
        raise ReportError('no_data')

    periods = []
    for row in rows:
        periods.append({
            'period': row['period'],
            'period_raw': row['period'],
            'temp_avg': _round_or_none(row['temp_avg']),
            'temp_max': _round_or_none(row['temp_max']),
            'temp_min': _round_or_none(row['temp_min']),
            'precipitation': _round_or_none(row['precipitation']),
            'sunshine': _round_or_none(row['sunshine']),
            'sample_count': row['sample_count'],
            'distinct_days': row['distinct_days'],
            'stations': [],
        })

    breakdown = _station_period_breakdown(conn, stations, start_date, end_date, granularity)
    used_station_ids = set()
    for row in periods:
        station_details = breakdown.get(row['period_raw'], [])
        row['stations'] = station_details
        for detail in station_details:
            used_station_ids.add(detail['station_id'])

    for station in stations:
        station['has_data'] = station['station_id'] in used_station_ids

    return {
        'params': {
            'lat': lat,
            'lon': lon,
            'radius': radius,
            'start_date': start_date,
            'end_date': end_date,
            'granularity': granularity,
        },
        'coverage': coverage,
        'stations': stations,
        'periods': periods,
        'granularity': granularity,
        'used_station_ids': sorted(used_station_ids),
        'used_station_count': len(used_station_ids),
    }


__all__ = ['generate_report']
