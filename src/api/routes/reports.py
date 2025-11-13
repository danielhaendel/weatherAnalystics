"""Report-related API routes."""

from __future__ import annotations

from flask import jsonify, request

from ..blueprint import api_bp
from ..services.reporting import build_aggregate_report, fetch_data_coverage
from ...reports import ReportError


@api_bp.get('/data/coverage')
def data_coverage():
    """Return date coverage for available measurements."""
    try:
        coverage = fetch_data_coverage()
    except ReportError as err:
        return jsonify({'ok': False, 'error': err.code}), 404
    return jsonify({'ok': True, **coverage})


@api_bp.post('/reports/aggregate')
def aggregate_report():
    """Return aggregated weather report for the provided parameters."""
    payload = request.get_json(force=True) or {}
    try:
        lat = float(payload.get('lat'))
        lon = float(payload.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'invalid_coordinates'}), 400

    try:
        radius = float(payload.get('radius') or 10.0)
    except (TypeError, ValueError):
        radius = 10.0

    granularity = (payload.get('granularity') or 'day').lower()
    start_date = payload.get('start_date')
    end_date = payload.get('end_date')
    if not start_date or not end_date:
        return jsonify({'ok': False, 'error': 'missing_dates'}), 400

    try:
        report = build_aggregate_report(lat, lon, radius, start_date, end_date, granularity)
    except ReportError as exc:
        status = 400 if exc.code in {'invalid_granularity', 'invalid_dates', 'invalid_range', 'out_of_bounds'} else 404
        return jsonify({'ok': False, 'error': exc.code}), status

    return jsonify({'ok': True, **report})
