"""Shared helpers for building weather reports (aggregations, coverage, stations)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import atan2, cos, radians, sin, sqrt
from typing import Dict, Iterable, List, Tuple


class ReportError(Exception):
    """Raised when a report cannot be generated."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


def get_coverage(conn) -> Dict[str, str] | None:
    row = conn.execute('SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM daily_kl').fetchone()
    if not row or not row['min_date'] or not row['max_date']:
        return None
    return {'min_date': row['min_date'], 'max_date': row['max_date']}


def _stations_within_radius(conn, lat: float, lon: float, radius_km: float, limit: int = 12) -> List[Dict]:
    radius_km = max(0.5, float(radius_km))
    lat_delta = radius_km / 111.0
    lon_step = 111.0 * max(0.1, abs(cos(radians(lat))))
    lon_delta = radius_km / lon_step if lon_step else radius_km / 111.0

    rows = conn.execute(
        '''
        SELECT station_id, station_name, state, latitude, longitude
        FROM stations
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
        ''',
        (lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta),
    ).fetchall()

    matches: List[Tuple[Dict, float]] = []
    for row in rows:
        distance = haversine_km(lat, lon, row['latitude'], row['longitude'])
        if distance <= radius_km:
            matches.append((row, distance))

    if not matches:
        nearest = conn.execute(
            '''
            SELECT station_id, station_name, state, latitude, longitude
            FROM stations
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY ((latitude - ?)*(latitude - ?) + (longitude - ?)*(longitude - ?))
            LIMIT 1
            ''',
            (lat, lat, lon, lon),
        ).fetchone()
        if nearest:
            matches.append((nearest, haversine_km(lat, lon, nearest['latitude'], nearest['longitude'])))

    matches.sort(key=lambda item: item[1])
    payload: List[Dict] = []
    for row, distance in matches[:limit]:
        payload.append({
            'station_id': row['station_id'],
            'name': row['station_name'],
            'state': row['state'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'distance_km': round(distance, 2),
        })
    return payload


def _build_aggregate_query(granularity: str, station_count: int) -> str:
    if granularity == 'day':
        group_expr = 'date'
        order_expr = 'date'
    elif granularity == 'month':
        group_expr = 'substr(date, 1, 7)'
        order_expr = 'substr(date, 1, 7)'
    elif granularity == 'year':
        group_expr = 'substr(date, 1, 4)'
        order_expr = 'substr(date, 1, 4)'
    else:
        raise ReportError('invalid_granularity')

    placeholders = ','.join('?' for _ in range(station_count))
    return f"""
        SELECT
            {group_expr} AS period,
            AVG(tmk) AS temp_avg,
            MAX(txk) AS temp_max,
            MIN(tnk) AS temp_min,
            SUM(rsk) AS precipitation,
            SUM(sdk) AS sunshine,
            COUNT(*) AS sample_count,
            COUNT(DISTINCT date) AS distinct_days
        FROM daily_kl
        WHERE station_id IN ({placeholders})
          AND date BETWEEN ? AND ?
        GROUP BY {group_expr}
        HAVING sample_count > 0
        ORDER BY {order_expr}
    """


def _round_or_none(value):
    if value is None:
        return None
    return round(float(value), 2)


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

    stations = _stations_within_radius(conn, lat, lon, radius)
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
            'temp_avg': _round_or_none(row['temp_avg']),
            'temp_max': _round_or_none(row['temp_max']),
            'temp_min': _round_or_none(row['temp_min']),
            'precipitation': _round_or_none(row['precipitation']),
            'sunshine': _round_or_none(row['sunshine']),
            'sample_count': row['sample_count'],
            'distinct_days': row['distinct_days'],
        })

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
    }
