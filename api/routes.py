# api/routes.py
import os
import requests
from flask import Blueprint, request, jsonify, abort
from datetime import date
import time

api_bp = Blueprint('api', __name__)

GEOAPIFY_KEY = os.environ.get('GEOAPIFY_KEY', '')

def require_api_key() -> None:
    if not GEOAPIFY_KEY:
        abort(500, description='Geo API key missing; set GEOAPIFY_KEY env var.')

@api_bp.get('/places')
def places_autocomplete():
    """Return place suggestions for a free-text query."""
    require_api_key()
    q = (request.args.get('q') or '').strip()
    country = (request.args.get('country') or 'de').lower()
    if not q:
        return jsonify({'items': []})
    url = 'https://api.geoapify.com/v1/geocode/autocomplete'
    params = {
        'text': q,
        'limit': 7,
        'filter': f'countrycode:{country}',
        'apiKey': GEOAPIFY_KEY,
        # Optional: 'lang': 'de'
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
            'country_code': p.get('country_code'),  # <— add this
            'postcode': p.get('postcode'),
            'street': p.get('street'),
            'housenumber': p.get('housenumber'),
        })

    return jsonify({'items': items})

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
    result = {
        'ok': True,
        'summary': f'Auswertung für Koordinaten ({lat}, {lon}) in DE von {start_date} bis {end_date}.'
    }
    return jsonify(result), 200