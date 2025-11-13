"""Geo-related API routes."""

from __future__ import annotations

from flask import jsonify, request

from ..blueprint import api_bp
from ..services.geo import GeoProviderError, fetch_place_suggestions, reverse_geocode_lookup


@api_bp.get('/places')
def places_autocomplete():
    """Return place suggestions for a free-text query."""
    query = (request.args.get('q') or '').strip()
    if not query:
        return jsonify({'items': []})
    country = (request.args.get('country') or 'de').lower()
    lang_value = request.args.get('lang')
    try:
        items = fetch_place_suggestions(query, country, lang_value)
    except GeoProviderError as err:
        return jsonify({'error': err.code}), err.status_code
    return jsonify({'items': items})


@api_bp.get('/reverse_geocode')
def reverse_geocode():
    """Lookup location metadata for the supplied coordinates."""
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'error': 'invalid_coordinates'}), 400

    lang_value = request.args.get('lang')
    try:
        payload = reverse_geocode_lookup(lat, lon, lang_value)
    except GeoProviderError as err:
        return jsonify({'error': err.code}), err.status_code
    return jsonify(payload)
