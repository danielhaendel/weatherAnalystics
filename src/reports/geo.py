"""Geospatial helpers used by report services."""

from __future__ import annotations

from math import atan2, cos, radians, sin, sqrt
from typing import Dict, List, Tuple

from .errors import ReportError


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


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


def stations_within_radius(conn, lat: float, lon: float, radius_km: float, limit: int = 12) -> List[Dict]:
    """Public helper to fetch nearby stations with distance metadata."""
    stations = _stations_within_radius(conn, lat, lon, radius_km, limit)
    if not stations:
        raise ReportError('no_stations')
    return stations


__all__ = ['haversine_km', 'stations_within_radius', '_stations_within_radius']
