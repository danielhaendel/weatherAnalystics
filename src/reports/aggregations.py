"""SQL aggregation helpers for report building."""

from __future__ import annotations

from typing import Dict, List

from .errors import ReportError


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


def _build_breakdown_group_expr(granularity: str) -> str:
    if granularity == 'day':
        return 'dk.date'
    if granularity == 'month':
        return 'substr(dk.date, 1, 7)'
    if granularity == 'year':
        return 'substr(dk.date, 1, 4)'
    raise ReportError('invalid_granularity')


def _station_period_breakdown(conn, stations: List[Dict], start_date: str,
                              end_date: str, granularity: str) -> Dict[str, List[Dict]]:
    if not stations:
        return {}

    group_expr = _build_breakdown_group_expr(granularity)
    placeholders = ','.join('?' for _ in stations)
    has_value_case = '(CASE WHEN dk.tmk IS NOT NULL OR dk.txk IS NOT NULL OR dk.tnk IS NOT NULL OR dk.rsk IS NOT NULL OR dk.sdk IS NOT NULL THEN 1 ELSE 0 END)'

    query = f"""
        SELECT
            {group_expr} AS period,
            dk.station_id,
            AVG(dk.tmk) AS temp_avg,
            MAX(dk.txk) AS temp_max,
            MIN(dk.tnk) AS temp_min,
            SUM(dk.rsk) AS precipitation,
            SUM(dk.sdk) AS sunshine,
            COUNT(*) AS sample_count,
            COUNT(DISTINCT dk.date) AS distinct_days
        FROM daily_kl AS dk
        WHERE dk.station_id IN ({placeholders})
          AND dk.date BETWEEN ? AND ?
        GROUP BY period, dk.station_id
        HAVING SUM({has_value_case}) > 0
        ORDER BY period ASC, dk.station_id ASC
    """

    params = [*(station['station_id'] for station in stations), start_date, end_date]
    rows = conn.execute(query, params).fetchall()
    station_lookup = {station['station_id']: station for station in stations}
    breakdown: Dict[str, List[Dict]] = {}
    for row in rows:
        station_meta = station_lookup.get(row['station_id'])
        if not station_meta:
            continue
        detail = {
            'station_id': row['station_id'],
            'station_name': station_meta.get('name'),
            'state': station_meta.get('state'),
            'distance_km': station_meta.get('distance_km'),
            'temp_avg': _round_or_none(row['temp_avg']),
            'temp_min': _round_or_none(row['temp_min']),
            'temp_max': _round_or_none(row['temp_max']),
            'precipitation': _round_or_none(row['precipitation']),
            'sunshine': _round_or_none(row['sunshine']),
            'sample_count': row['sample_count'],
            'distinct_days': row['distinct_days'],
        }
        breakdown.setdefault(row['period'], []).append(detail)
    for details in breakdown.values():
        details.sort(key=lambda item: (
            item['distance_km'] is None,
            item['distance_km'] if item['distance_km'] is not None else float('inf'),
            item['station_name'] or '',
            item['station_id'],
        ))
    return breakdown


def _round_or_none(value):
    if value is None:
        return None
    return round(float(value), 2)


__all__ = ['_build_aggregate_query', '_station_period_breakdown', '_round_or_none']
