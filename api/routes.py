# api/routes.py
import os
import time
from datetime import date
from math import radians, sin, cos, sqrt, atan2

import requests
from flask import Blueprint, request, jsonify, abort, current_app

from dwd_sync import sync_dwd_data
from db import get_db

api_bp = Blueprint('api', __name__)

GEOAPIFY_KEY = os.environ.get('GEOAPIFY_KEY', '')


def get_translations():
    return current_app.config.get('APP_TRANSLATIONS', {})


def normalize_lang(lang_value) -> str:
    lang = (lang_value or '').lower()
    translations = get_translations()
    supported = current_app.config.get('APP_SUPPORTED_LANGUAGES')
    default_lang = current_app.config.get('APP_DEFAULT_LANGUAGE')
    if not default_lang:
        default_lang = next(iter(translations.keys()), 'de')

    if supported and lang in supported:
        return lang
    if lang in translations:
        return lang
    return default_lang


def require_api_key() -> None:
    if not GEOAPIFY_KEY:
        abort(500, description='Geo API key missing; set GEOAPIFY_KEY env var.')

@api_bp.get('/places')
def places_autocomplete():
    """Return place suggestions for a free-text query."""
    require_api_key()
    q = (request.args.get('q') or '').strip()
    country = (request.args.get('country') or 'de').lower()
    lang = normalize_lang(request.args.get('lang'))
    if not q:
        return jsonify({'items': []})
    url = 'https://api.geoapify.com/v1/geocode/autocomplete'
    params = {
        'text': q,
        'limit': 7,
        'filter': f'countrycode:{country}',
        'lang': lang,
        'apiKey': GEOAPIFY_KEY,
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    items = []
    for f in data.get('features', []):
        p = f.get('properties', {})
        items.append({
            'label': p.get('formatted') or p.get('name'),
            'lat': p.get('lat'),
            'lon': p.get('lon'),
            'city': p.get('city'),
            'country': p.get('country'),
            'country_code': p.get('country_code'),
            'postcode': p.get('postcode'),
            'street': p.get('street'),
            'housenumber': p.get('housenumber'),
        })

    return jsonify({'items': items})


@api_bp.get('/reverse_geocode')
def reverse_geocode():
    require_api_key()
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'error': 'invalid coordinates'}), 400

    url = 'https://api.geoapify.com/v1/geocode/reverse'
    lang = normalize_lang(request.args.get('lang'))
    params = {
        'lat': lat,
        'lon': lon,
        'limit': 1,
        'lang': lang,
        'apiKey': GEOAPIFY_KEY,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException:
        return jsonify({'error': 'lookup_failed'}), 502

    data = resp.json()
    features = data.get('features', [])
    if not features:
        return jsonify({
            'city': None,
            'town': None,
            'village': None,
            'municipality': None,
            'county': None,
            'state': None,
            'country': None,
            'country_code': None,
        })

    props = features[0].get('properties', {})
    payload = {
        'city': props.get('city'),
        'town': props.get('town'),
        'village': props.get('village'),
        'municipality': props.get('municipality'),
        'county': props.get('county'),
        'state': props.get('state'),
        'country': props.get('country'),
        'country_code': props.get('country_code'),
    }
    return jsonify(payload)


@api_bp.get('/validate_address')
def validate_address():
    """Validate a full address string by forward geocoding once."""
    require_api_key()
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify({'valid': False, 'reason': 'empty input'})
    url = 'https://api.geoapify.com/v1/geocode/search'
    params = {'text': q, 'limit': 1, 'apiKey': GEOAPIFY_KEY}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    feats = data.get('features', [])
    if not feats:
        return jsonify({'valid': False})
    p = feats[0]['properties']
    return jsonify({
        'valid': True,
        'normalized': p.get('formatted'),
        'lat': p.get('lat'),
        'lon': p.get('lon'),
    })

@api_bp.post('/analyze')
def analyze():
    time.sleep(10)
    data = request.get_json(force=True) or {}
    lat = data.get('lat')
    lon = data.get('lon')
    country_code = (data.get('country_code') or '').lower()
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    request_lang = normalize_lang(data.get('lang'))

    # Basic validations
    errors = []
    if lat is None or lon is None:
        errors.append('missing coordinates')
    if country_code != 'de':
        errors.append('address must be in Germany')
    if not start_date or not end_date:
        errors.append('dates missing')
    else:
        try:
            s_parts = [int(x) for x in start_date.split('-')]
            e_parts = [int(x) for x in end_date.split('-')]
            s_d = date(s_parts[0], s_parts[1], s_parts[2])
            e_d = date(e_parts[0], e_parts[1], e_parts[2])
            today = date.today()
            if s_d > e_d:
                errors.append('start after end')
            if not (s_d < today and e_d < today):
                errors.append('dates must be in the past')
        except Exception:
            errors.append('invalid date format')

    if errors:
        return jsonify({'ok': False, 'errors': errors}), 400

    # Replace with real analysis
    translations = get_translations()
    summary_template = translations.get(request_lang, {}).get('messages', {}).get('analysis_summary')
    if not summary_template:
        summary_template = translations.get('de', {}).get('messages', {}).get('analysis_summary')
    if not summary_template:
        summary_template = 'Analysis for coordinates ({lat}, {lon}) in DE from {start} to {end}.'

    result = {
        'ok': True,
        'summary': summary_template.format(lat=lat, lon=lon, start=start_date, end=end_date),
    }
    return jsonify(result), 200


@api_bp.post('/sync_stations')
def sync_stations():
    """Trigger a refresh of station metadata from the DWD feed."""
    current_app.logger.info('API sync_stations called.')
    try:
        result = sync_dwd_data(current_app, include_weather=False, raise_errors=True)
    except Exception as err:  # pragma: no cover - defensive
        current_app.logger.exception('Station sync failed: %s', err)
        return jsonify({'ok': False, 'error': str(err)}), 500

    stations = result.get('stations', {}) if isinstance(result, dict) else {}
    message = stations.get('message') or ('downloaded' if stations.get('downloaded') else 'unknown')
    current_app.logger.info('Station sync result: downloaded=%s rows=%s message=%s',
                            stations.get('downloaded'), stations.get('rows_processed'), message)
    payload = {
        'ok': True,
        'stations': {
            'downloaded': bool(stations.get('downloaded')),
            'rows_processed': stations.get('rows_processed', 0),
            'message': message,
        },
    }
    return jsonify(payload), 200


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


@api_bp.get('/stations/nearest')
def stations_nearest():
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'invalid_coordinates'}), 400

    conn = get_db()
    rows = conn.execute(
        '''
        SELECT station_id, station_name, state, latitude, longitude, from_date, to_date
        FROM stations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY ((latitude - ?)*(latitude - ?) + (longitude - ?)*(longitude - ?))
        LIMIT 8
        '''
        , (lat, lat, lon, lon)
    ).fetchall()

    if not rows:
        return jsonify({'ok': False, 'error': 'no_station_data'}), 404

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

    if best_row is None:
        return jsonify({'ok': False, 'error': 'no_station_data'}), 404

    payload = {
        'ok': True,
        'station': {
            'station_id': best_row['station_id'],
            'name': best_row['station_name'],
            'state': best_row['state'],
            'latitude': best_row['latitude'],
            'longitude': best_row['longitude'],
            'from_date': best_row['from_date'],
            'to_date': best_row['to_date'],
            'distance_km': round(best_distance, 2) if best_distance is not None else None,
        }
    }
    current_app.logger.info(
        'Nearest station lookup: lat=%s lon=%s -> %s (distance=%.2f km)',
        lat, lon, best_row['station_id'], best_distance or -1.0
    )
    return jsonify(payload)
