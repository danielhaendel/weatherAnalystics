"""Station-related helper functions for API routes."""

from __future__ import annotations

from typing import Dict, List, Optional

from flask import current_app

from ...db import get_db
from ...importers import import_station_metadata
from ...reports import haversine_km
from ...reports import geo as reports_geo


class StationServiceError(Exception):
    def __init__(self, code: str, status_code: int = 500, detail: Optional[str] = None) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.detail = detail


def refresh_station_metadata() -> Dict[str, int]:
    app_obj = current_app
    try:
        stats = import_station_metadata(app_obj)
    except Exception as exc:  # pragma: no cover - defensive
        app_obj.logger.exception('Station sync failed: %s', exc)
        raise StationServiceError('station_sync_failed', 500, str(exc)) from exc
    return {
        'inserted': stats.get('inserted', 0),
        'updated': stats.get('updated', 0),
    }


def find_nearest_station(lat: float, lon: float) -> Dict[str, object]:
    conn = get_db()
    rows = conn.execute(
        '''
        SELECT station_id, station_name, state, latitude, longitude, from_date, to_date
        FROM stations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY ((latitude - ?)*(latitude - ?) + (longitude - ?)*(longitude - ?))
        LIMIT 8
        ''',
        (lat, lat, lon, lon),
    ).fetchall()
    if not rows:
        raise StationServiceError('no_station_data', 404)

    best_row = None
    best_distance = None
    for row in rows:
        row_lat = row['latitude']
        row_lon = row['longitude']
        if row_lat is None or row_lon is None:
            continue
        distance = haversine_km(lat, lon, row_lat, row_lon)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_row = row

    if not best_row:
        raise StationServiceError('no_station_data', 404)

    payload = {
        'station_id': best_row['station_id'],
        'name': best_row['station_name'],
        'state': best_row['state'],
        'latitude': best_row['latitude'],
        'longitude': best_row['longitude'],
        'from_date': best_row['from_date'],
        'to_date': best_row['to_date'],
        'distance_km': round(best_distance, 2) if best_distance is not None else None,
    }
    current_app.logger.info(
        'Nearest station lookup: lat=%s lon=%s -> %s (distance=%.2f km)',
        lat, lon, payload['station_id'], best_distance or -1.0,
    )
    return payload


def list_stations_in_radius(lat: float, lon: float, radius: float, limit: int = 40) -> List[Dict[str, object]]:
    conn = get_db()
    stations = reports_geo._stations_within_radius(conn, lat, lon, radius, limit=limit)
    return stations


__all__ = ['StationServiceError', 'refresh_station_metadata', 'find_nearest_station', 'list_stations_in_radius']
