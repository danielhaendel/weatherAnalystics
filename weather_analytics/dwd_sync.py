"""Download and import DWD climate data into the SQLite database."""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
import re
import zipfile
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import csv
import requests

LOGGER = logging.getLogger(__name__)

from .db import execute_script, get_db

BASE_URL = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/'
STATIONS_FILENAME = 'KL_Tageswerte_Beschreibung_Stationen.txt'
LISTING_DATE_PATTERN = re.compile(r'(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})')
LISTING_DATE_FORMAT = '%d-%b-%Y %H:%M'

SCHEMA_STATEMENTS = (
    "DROP TABLE IF EXISTS weather_daily;",
    "DROP TABLE IF EXISTS stations;",
    """
    CREATE TABLE stations (
        station_id INTEGER PRIMARY KEY,
        from_date TEXT,
        to_date TEXT,
        station_name TEXT,
        state TEXT,
        latitude REAL,
        longitude REAL,
        height REAL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE weather_daily (
        station_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        data TEXT NOT NULL,
        source_file TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (station_id, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_files (
        filename TEXT PRIMARY KEY,
        last_modified TEXT,
        last_checked TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_weather_daily_date ON weather_daily (date)
    """
)


def sync_dwd_data(app, include_weather: bool = False, raise_errors: bool = False) -> Dict[str, Any]:
    """Download station metadata (and optionally weather data) into SQLite."""
    logger = app.logger or logging.getLogger(__name__)
    logger.info('Starting DWD sync (include_weather=%s)', include_weather)
    try:
        with app.app_context():
            execute_script(SCHEMA_STATEMENTS)
            session = requests.Session()
            listing = fetch_directory_listing(session)
            if not listing:
                logger.warning('DWD sync: directory listing is empty or not reachable.')
                return {'stations': {'downloaded': False, 'rows_processed': 0, 'message': 'listing_empty'}}

            station_result = process_station_file(session, listing, logger)
            weather_result = None
            if include_weather:
                weather_result = process_weather_files(session, listing, logger)
            logger.info('DWD sync finished: stations=%s weather=%s', station_result, weather_result)
            return {'stations': station_result, 'weather': weather_result}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception('DWD sync failed.')
        if raise_errors:
            raise
        return {'error': str(exc) or 'sync_failed'}


def fetch_directory_listing(session: requests.Session) -> Dict[str, Dict[str, Optional[str]]]:
    """Return mapping of filename to metadata extracted from the directory listing."""
    response = session.get(BASE_URL, timeout=60)
    response.raise_for_status()
    entries: Dict[str, Dict[str, Optional[str]]] = {}
    for line in response.text.splitlines():
        href = extract_href(line)
        if not href:
            continue
        if href.endswith('/'):
            continue
        if not (href.endswith('.zip') or href.endswith('.txt')):
            continue
        match = LISTING_DATE_PATTERN.search(line)
        last_modified_iso: Optional[str] = None
        if match:
            try:
                parsed = dt.datetime.strptime(match.group(1), LISTING_DATE_FORMAT)
                last_modified_iso = parsed.replace(tzinfo=dt.timezone.utc).isoformat()
            except ValueError:
                last_modified_iso = None
        entries[href] = {
            'url': urljoin(BASE_URL, href),
            'last_modified': last_modified_iso,
        }
    return entries


def extract_href(line: str) -> Optional[str]:
    """Extract href attribute value from a HTML anchor tag line."""
    match = re.search(r'href=\"([^\"]+)\"', line)
    if not match:
        return None
    return match.group(1)


def process_station_file(session: requests.Session, listing: Dict[str, Dict[str, Optional[str]]], logger: logging.Logger) -> Dict[str, Any]:
    entry = listing.get(STATIONS_FILENAME)
    if not entry:
        logger.warning('DWD sync: stations file missing in listing.')
        return {'downloaded': False, 'rows_processed': 0, 'message': 'missing'}

    # Temporarily disable freshness check to force download on every sync.
    # if not needs_download(entry['url'], entry['last_modified']):
    #     logger.info('DWD sync: stations file up to date, no download required.')
    #     return {'downloaded': False, 'rows_processed': 0, 'message': 'up_to_date'}

    logger.info('DWD sync: downloading station metadata …')
    response = session.get(entry['url'], timeout=120)
    response.raise_for_status()
    # DWD uses ISO-8859-1 encoding for station description files.
    raw_text = response.content.decode('latin-1')
    rows = import_station_data(raw_text)
    record_file_state(STATIONS_FILENAME, entry['last_modified'])
    logger.info('DWD sync: station metadata updated (rows=%s).', rows)
    return {'downloaded': True, 'rows_processed': rows, 'message': 'downloaded'}


def process_weather_files(session: requests.Session, listing: Dict[str, Dict[str, Optional[str]]], logger: logging.Logger) -> Dict[str, Any]:
    zip_entries = sorted(
        ((name, info) for name, info in listing.items() if name.endswith('.zip')),
        key=lambda item: item[0]
    )
    if not zip_entries:
        logger.warning('DWD sync: no ZIP archives discovered in listing.')
        return {'processed': 0, 'skipped': True}

    processed = 0
    for filename, meta in zip_entries:
        # Temporarily disable freshness check to force download on every sync.
        # if not needs_download(meta['url'], meta['last_modified']):
        #     continue
        logger.info('DWD sync: downloading %s …', filename)
        try:
            response = session.get(meta['url'], timeout=300)
            response.raise_for_status()
            import_weather_archive(response.content, filename)
            record_file_state(filename, meta['last_modified'])
            logger.info('DWD sync: processed %s', filename)
            processed += 1
        except requests.RequestException as err:
            logger.warning('DWD sync: failed to download %s (%s)', filename, err)
        except Exception as err:  # pragma: no cover - defensive logging
            logger.exception('DWD sync: failed to process %s (%s)', filename, err)
    return {'processed': processed, 'skipped': processed == 0}


def needs_download(url: str, last_modified: Optional[str]) -> bool:
    """Return True when the given resource requires a fresh download."""
    filename = url.rsplit('/', 1)[-1]
    conn = get_db()
    row = conn.execute(
        'SELECT last_modified FROM dataset_files WHERE filename = ?',
        (filename,)
    ).fetchone()
    if row is None:
        return True
    stored_last_modified = row['last_modified'] or ''
    if not last_modified:
        return False
    return last_modified > stored_last_modified


def record_file_state(filename: str, last_modified: Optional[str]) -> None:
    conn = get_db()
    now_iso = dt.datetime.utcnow().isoformat(timespec='seconds')
    if last_modified is None:
        row = conn.execute(
            'SELECT last_modified FROM dataset_files WHERE filename = ?',
            (filename,)
        ).fetchone()
        if row:
            last_modified = row['last_modified']
    with conn:
        conn.execute(
            """
            INSERT INTO dataset_files (filename, last_modified, last_checked)
            VALUES (?, ?, ?)
            ON CONFLICT(filename) DO UPDATE SET
                last_modified = excluded.last_modified,
                last_checked = excluded.last_checked
            """,
            (filename, last_modified, now_iso)
        )


def import_station_data(stations_raw: str) -> int:
    conn = get_db()
    now_iso = dt.datetime.utcnow().isoformat(timespec='seconds')
    records = list(parse_station_records(stations_raw))

    rows_to_insert: List[tuple] = []
    for data in records:
        station_id_raw = data.get('Stations_id') or data.get('STATIONS_ID')
        station_id = normalize_station_id(station_id_raw, context='station description')
        if station_id is None:
            continue
        rows_to_insert.append(
            (
                station_id,
                normalize_date(data.get('von_datum') or data.get('VON_DATUM')),
                normalize_date(data.get('bis_datum') or data.get('BIS_DATUM')),
                (data.get('Stationsname') or data.get('STATION_NAME') or '').strip() or None,
                (data.get('Bundesland') or data.get('BUNDESLAND') or '').strip() or None,
                parse_float(data.get('geogr. Breite') or data.get('geoBreite') or data.get('Latitude')),
                parse_float(data.get('geogr. Länge') or data.get('geoLaenge') or data.get('Longitude')),
                parse_float(data.get('Stationshoehe') or data.get('Stationshoehe m ue. NN') or data.get('Stationshoehe NN')),
                now_iso,
            )
        )

    if not rows_to_insert:
        return 0

    with conn:
        conn.executemany(
            """
            INSERT INTO stations (
                station_id, from_date, to_date, station_name, state,
                latitude, longitude, height, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id) DO UPDATE SET
                from_date = excluded.from_date,
                to_date = excluded.to_date,
                station_name = excluded.station_name,
                state = excluded.state,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                height = excluded.height,
                updated_at = excluded.updated_at
            """,
            rows_to_insert
        )
    return len(rows_to_insert)


def parse_station_records(stations_raw: str) -> Iterable[Dict[str, str]]:
    """Yield station dictionaries from the raw description file."""
    lines = stations_raw.splitlines()
    cleaned_lines = [line for line in lines if line and not line.startswith('#')]
    if not cleaned_lines:
        return []

    header_line = cleaned_lines[0].lstrip('^')
    cleaned_lines[0] = header_line
    if ';' in header_line:
        reader = csv.reader(io.StringIO('\n'.join(cleaned_lines)), delimiter=';')
        headers: Optional[List[str]] = None
        for row in reader:
            if not row:
                continue
            if headers is None:
                headers = [col.strip().lstrip('\ufeff') for col in row]
                continue
            yield {headers[i]: row[i].strip().lstrip('\ufeff') for i in range(len(headers))}
        return

    # Fallback: whitespace delimited format
    for line in cleaned_lines[1:] if header_line.lower().startswith('stations') else cleaned_lines:
        record = parse_station_line_whitespace(line)
        if record:
            yield record


def parse_station_line_whitespace(line: str) -> Optional[Dict[str, str]]:
    stripped = line.rstrip()
    if not stripped:
        return None
    parts = stripped.split()
    if len(parts) < 7:
        return None

    station_id, von_datum, bis_datum, hoehe, lat, lon, *rest = parts
    abgabe = rest[-1] if rest else ''
    rest = rest[:-1] if rest else []

    bundesland = None
    station_parts = rest
    for i in range(len(rest), 0, -1):
        candidate = ' '.join(rest[i - 1:])
        if candidate in GERMAN_STATE_NAMES:
            bundesland = candidate
            station_parts = rest[:i - 1]
            break
    station_name = ' '.join(station_parts).strip()

    return {
        'Stations_id': station_id,
        'von_datum': von_datum,
        'bis_datum': bis_datum,
        'Stationshoehe': hoehe,
        'geoBreite': lat,
        'geoLaenge': lon,
        'Stationsname': station_name,
        'Bundesland': bundesland,
        'Abgabe': abgabe,
    }


def import_weather_archive(archive_bytes: bytes, source_filename: str) -> None:
    conn = get_db()
    now_iso = dt.datetime.utcnow().isoformat(timespec='seconds')
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        txt_name = next((name for name in zf.namelist() if name.endswith('.txt')), None)
        if not txt_name:
            raise ValueError(f'No data file found inside {source_filename}')
        with zf.open(txt_name) as data_file:
            reader = csv.reader(io.TextIOWrapper(data_file, encoding='utf-8'), delimiter=';')
            headers: Optional[List[str]] = None
            batch: List[tuple] = []
            for row in reader:
                if not row or row[0].startswith('#'):
                    continue
                if headers is None:
                headers = [col.strip() for col in row]
                continue
            data = {headers[i]: row[i].strip() for i in range(len(headers))}
            station_id_raw = data.get('STATIONS_ID')
            date_raw = data.get('MESS_DATUM')
            station_id = normalize_station_id(station_id_raw, context=f'weather row {source_filename}')
            if station_id is None or not date_raw:
                continue
            batch.append(
                (
                    station_id,
                    normalize_date(date_raw),
                        json.dumps(normalize_weather_payload(data), ensure_ascii=False),
                        source_filename,
                        now_iso,
                    )
                )
                if len(batch) >= 1000:
                    persist_weather_batch(conn, batch)
                    batch.clear()
            if batch:
                persist_weather_batch(conn, batch)


def persist_weather_batch(conn, batch: List[tuple]) -> None:
    with conn:
        conn.executemany(
            """
            INSERT INTO weather_daily (station_id, date, data, source_file, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(station_id, date) DO UPDATE SET
                data = excluded.data,
                source_file = excluded.source_file,
                updated_at = excluded.updated_at
            """,
            batch
        )


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value or value == '-999' or value == '-9999':
        return None
    if len(value) == 8 and value.isdigit():
        return f'{value[0:4]}-{value[4:6]}-{value[6:8]}'
    if len(value) == 10 and value.count('-') == 2:
        return value
    try:
        parsed = dt.datetime.strptime(value, '%Y%m%d')
        return parsed.strftime('%Y-%m-%d')
    except ValueError:
        return value


def parse_float(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.replace(',', '.').strip()
    if not value or value in {'-999', '-9999'}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def normalize_weather_payload(payload: Dict[str, str]) -> Dict[str, Optional[str]]:
    normalized: Dict[str, Optional[str]] = {}
    for key, value in payload.items():
        if value in {'-999', '-9999', '-999.0', ''}:
            normalized[key] = None
        elif key == 'MESS_DATUM':
            normalized[key] = normalize_date(value)
        else:
            normalized[key] = value
    return normalized


def normalize_station_id(value: Optional[str], *, context: str = '') -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().lstrip('\ufeff')
    if not text:
        return None
    if not text.isdigit():
        LOGGER.warning('Skipping %s due to non-numeric station_id=%r', context or 'record', value)
        return None
    station_id = int(text)
    if station_id <= 0:
        LOGGER.warning('Skipping %s due to invalid station_id=%r', context or 'record', value)
        return None
    return station_id
GERMAN_STATE_NAMES = {
    'Baden-Württemberg',
    'Bayern',
    'Berlin',
    'Brandenburg',
    'Bremen',
    'Hamburg',
    'Hessen',
    'Mecklenburg-Vorpommern',
    'Niedersachsen',
    'Nordrhein-Westfalen',
    'Rheinland-Pfalz',
    'Saarland',
    'Sachsen',
    'Sachsen-Anhalt',
    'Schleswig-Holstein',
    'Thüringen',
}
