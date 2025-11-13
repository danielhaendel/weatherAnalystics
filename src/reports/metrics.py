"""Additional report metrics and sampling helpers."""

from __future__ import annotations

from typing import Dict, List

from .aggregations import _round_or_none
from .geo import _stations_within_radius


def temp_durchschnitt_auswertung(conn, lat: float, lon: float,
                                 start_date: str, end_date: str,
                                 radius: float) -> float:
    stations = _stations_within_radius(conn, lat, lon, radius)
    if not stations:
        return 0.0

    placeholders = ','.join('?' for _ in stations)
    query = f'''
        SELECT AVG(tmk) AS avg_temp
        FROM daily_kl
        WHERE station_id IN ({placeholders})
          AND date BETWEEN ? AND ?
          AND tmk IS NOT NULL
    '''
    params = [*(station['station_id'] for station in stations), start_date, end_date]
    row = conn.execute(query, params).fetchone()
    if not row or row['avg_temp'] is None:
        return 0.0
    return float(row['avg_temp'])


def temperature_samples(conn, lat: float, lon: float,
                        start_date: str, end_date: str,
                        radius: float, limit: int = 500) -> List[Dict]:
    stations = _stations_within_radius(conn, lat, lon, radius)
    if not stations:
        return []

    placeholders = ','.join('?' for _ in stations)
    query = f'''
        SELECT dk.station_id,
               dk.date,
               dk.tmk,
               s.station_name,
               s.state
        FROM daily_kl AS dk
        LEFT JOIN stations AS s ON s.station_id = dk.station_id
        WHERE dk.station_id IN ({placeholders})
          AND dk.date BETWEEN ? AND ?
          AND dk.tmk IS NOT NULL
        ORDER BY dk.date ASC
        LIMIT ?
    '''
    params = [*(station['station_id'] for station in stations), start_date, end_date, limit]
    rows = conn.execute(query, params).fetchall()
    station_lookup = {station['station_id']: station for station in stations}
    samples: List[Dict] = []
    for row in rows:
        station_meta = station_lookup.get(row['station_id'], {})
        samples.append({
            'station_id': row['station_id'],
            'station_name': row['station_name'] or station_meta.get('name'),
            'state': row['state'] or station_meta.get('state'),
            'date': row['date'],
            'temperature': _round_or_none(row['tmk']),
            'distance_km': station_meta.get('distance_km'),
        })
    return samples


__all__ = ['temp_durchschnitt_auswertung', 'temperature_samples']
