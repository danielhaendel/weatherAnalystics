"""Geoapify integration helpers used by API routes."""

from __future__ import annotations

import os
from typing import Dict, List

import requests
from flask import current_app

from .language import normalize_lang


class GeoProviderError(Exception):
    """Raised when the external Geo service fails or is misconfigured."""

    def __init__(self, code: str, status_code: int = 500) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


def _get_geoapify_key() -> str:
    return current_app.config.get('GEOAPIFY_KEY') or os.environ.get('GEOAPIFY_KEY', '')


def _build_headers() -> Dict[str, str]:
    return {
        'User-Agent': current_app.config.get('GEOAPIFY_USER_AGENT', 'weather-analytics/geo-client'),
    }


def fetch_place_suggestions(query: str, country: str, lang_value: str | None) -> List[Dict]:
    api_key = _get_geoapify_key()
    if not api_key:
        raise GeoProviderError('geo_api_key_missing', 500)
    lang = normalize_lang(lang_value)
    params = {
        'text': query,
        'limit': 7,
        'filter': f'countrycode:{country.lower()}',
        'lang': lang,
        'apiKey': api_key,
    }
    response = requests.get(
        'https://api.geoapify.com/v1/geocode/autocomplete',
        params=params,
        timeout=10,
        headers=_build_headers(),
    )
    response.raise_for_status()
    data = response.json()
    items = []
    for feature in data.get('features', []):
        props = feature.get('properties', {})
        items.append({
            'label': props.get('formatted') or props.get('name'),
            'lat': props.get('lat'),
            'lon': props.get('lon'),
            'city': props.get('city'),
            'country': props.get('country'),
            'country_code': props.get('country_code'),
            'postcode': props.get('postcode'),
            'street': props.get('street'),
            'housenumber': props.get('housenumber'),
        })
    return items


def reverse_geocode_lookup(lat: float, lon: float, lang_value: str | None) -> Dict[str, str | None]:
    api_key = _get_geoapify_key()
    if not api_key:
        raise GeoProviderError('geo_api_key_missing', 500)
    lang = normalize_lang(lang_value)
    params = {
        'lat': lat,
        'lon': lon,
        'limit': 1,
        'lang': lang,
        'apiKey': api_key,
    }
    try:
        response = requests.get(
            'https://api.geoapify.com/v1/geocode/reverse',
            params=params,
            timeout=10,
            headers=_build_headers(),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise GeoProviderError('lookup_failed', 502) from exc

    data = response.json()
    features = data.get('features', [])
    if not features:
        return {
            'city': None,
            'town': None,
            'village': None,
            'municipality': None,
            'county': None,
            'state': None,
            'country': None,
            'country_code': None,
        }

    props = features[0].get('properties', {})
    return {
        'city': props.get('city'),
        'town': props.get('town'),
        'village': props.get('village'),
        'municipality': props.get('municipality'),
        'county': props.get('county'),
        'state': props.get('state'),
        'country': props.get('country'),
        'country_code': props.get('country_code'),
    }


__all__ = ['GeoProviderError', 'fetch_place_suggestions', 'reverse_geocode_lookup']
