from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from weather_analytics import create_app
from weather_analytics.db import get_db
from weather_analytics.db.schema import ensure_weather_schema


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv('GEOAPIFY_KEY', 'test-key')
    db_path = tmp_path / 'weather.db'
    app = create_app()
    app.config.update({
        'TESTING': True,
        'DATABASE': str(db_path),
        'DATABASE_TIMEOUT': 1,
    })

    with app.app_context():
        ensure_weather_schema(reset=True)
        conn = get_db()
        conn.execute(
            '''
            INSERT INTO stations (station_id, station_name, state, latitude, longitude,
                                  height, from_date, to_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (3056, 'Berlin Test Station', 'Berlin', 52.52, 13.405, 50.0,
             '2000-01-01', '2100-12-31', '2024-01-01T00:00:00'),
        )
        conn.execute(
            '''
            INSERT INTO daily_kl (station_id, date, tmk, txk, tnk, rsk, sdk, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (3056, '2022-07-15', 20.5, 25.0, 15.2, 5.0, 10.0, '2024-01-01T00:00:00'),
        )
        conn.commit()

    yield app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def mock_geoapify(monkeypatch):
    from weather_analytics.api import routes

    class DummyResponse:
        def __init__(self, payload, status=200):
            self._payload = payload
            self.status_code = status

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError('geoapify error')

        def json(self):
            return self._payload

    def fake_get(url, params=None, timeout=10):
        if 'autocomplete' in url:
            payload = {
                'features': [{
                    'properties': {
                        'formatted': 'Bochum, NRW',
                        'name': 'Bochum',
                        'lat': 51.48,
                        'lon': 7.22,
                        'city': 'Bochum',
                        'country': 'Germany',
                        'country_code': 'de',
                        'postcode': '44787',
                        'street': 'Ostring',
                        'housenumber': '25',
                    }
                }]
            }
            return DummyResponse(payload)
        if 'reverse' in url:
            payload = {
                'features': [{
                    'properties': {
                        'city': 'Berlin',
                        'state': 'Berlin',
                        'country': 'Germany',
                        'country_code': 'de',
                    }
                }]
            }
            return DummyResponse(payload)
        if 'geocode/search' in url:
            payload = {
                'features': [{
                    'properties': {
                        'formatted': 'Ostring 25, 44787 Bochum',
                        'lat': 51.48,
                        'lon': 7.22,
                    }
                }]
            }
            return DummyResponse(payload)
        return DummyResponse({'features': []})

    monkeypatch.setattr(routes, 'GEOAPIFY_KEY', 'test-key')
    monkeypatch.setattr(routes.requests, 'get', fake_get)


@pytest.fixture(autouse=True)
def mock_station_import(monkeypatch):
    from weather_analytics.api import routes

    def fake_import(_app):
        return {'inserted': 1, 'updated': 2}

    monkeypatch.setattr(routes, 'import_station_metadata', fake_import)


def test_autocomplete_places(client):
    resp = client.get('/api/places', query_string={'q': 'Bochum', 'country': 'de', 'lang': 'de'})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['items'][0]['city'] == 'Bochum'


def test_reverse_geocode(client):
    resp = client.get('/api/reverse_geocode', query_string={'lat': 52.52, 'lon': 13.405, 'lang': 'de'})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['city'] == 'Berlin'


def test_validate_address(client):
    resp = client.get('/api/validate_address', query_string={'q': 'Ostring 25, 44787 Bochum'})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['valid'] is True
    assert data['lat'] == 51.48


def test_stations_in_radius(client):
    resp = client.get('/api/stations_in_radius', query_string={'lat': 52.52, 'lon': 13.405, 'radius': 30, 'limit': 10})
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['stations']) == 1
    assert data['stations'][0]['station_id'] == 3056


def test_nearest_station(client):
    resp = client.get('/api/stations/nearest', query_string={'lat': 52.52, 'lon': 13.405})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['station']['station_id'] == 3056


def test_data_coverage(client):
    resp = client.get('/api/data/coverage')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['min_date'] == '2022-07-15'
    assert data['max_date'] == '2022-07-15'


def test_reports_aggregate(client):
    payload = {
        'lat': 52.52,
        'lon': 13.405,
        'radius': 30,
        'start_date': '2022-07-15',
        'end_date': '2022-07-15',
        'granularity': 'day',
    }
    resp = client.post('/api/reports/aggregate', json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['ok'] is True
    assert data['periods'][0]['temp_avg'] == 20.5


def test_analyze_endpoint(client):
    payload = {
        'lat': 52.52,
        'lon': 13.405,
        'country_code': 'de',
        'start_date': '2022-07-01',
        'end_date': '2022-07-10',
        'lang': 'de',
    }
    resp = client.post('/api/analyze', json=payload)
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True


def test_sync_stations(client):
    resp = client.post('/api/sync_stations')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['stations']['inserted'] == 1
