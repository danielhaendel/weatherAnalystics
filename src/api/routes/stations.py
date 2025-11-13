"""Station-related API routes."""

from __future__ import annotations

from flask import jsonify, request

from ..blueprint import api_bp
from ..services.stations import StationServiceError, find_nearest_station, refresh_station_metadata


@api_bp.post('/sync_stations')
def sync_stations():
    """Trigger a refresh of station metadata from the DWD feed."""
    try:
        stats = refresh_station_metadata()
    except StationServiceError as err:
        payload = {'ok': False, 'error': err.code}
        if err.detail:
            payload['detail'] = err.detail
        return jsonify(payload), err.status_code

    rows_processed = stats['inserted'] + stats['updated']
    payload = {
        'ok': True,
        'stations': {
            'downloaded': True,
            'rows_processed': rows_processed,
            'inserted': stats['inserted'],
            'updated': stats['updated'],
            'message': 'downloaded',
        },
    }
    return jsonify(payload), 200


@api_bp.get('/stations/nearest')
def stations_nearest():
    """Return the nearest station for a given coordinate."""
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'invalid_coordinates'}), 400

    try:
        station = find_nearest_station(lat, lon)
    except StationServiceError as err:
        return jsonify({'ok': False, 'error': err.code}), err.status_code

    return jsonify({'ok': True, 'station': station})
