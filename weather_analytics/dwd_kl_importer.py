"""Importer for DWD historical daily climate (KL) data."""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import re
import sqlite3
import tempfile
import time
import zipfile
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry

from .db import get_db
from .schema import ensure_weather_schema


BASE_URL = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/'
STATION_DESCRIPTION_FILE = 'KL_Tageswerte_Beschreibung_Stationen.txt'
ARCHIVE_SUFFIX = '_hist.zip'
SENTINEL_VALUES = {'-999', '-999.0', '-9999', '-9999.0'}
CHUNK_SIZE = 500
SQLITE_LOCK_RETRIES = 5
SQLITE_LOCK_SLEEP = 1.0
SQLITE_BUSY_TIMEOUT_MS = 60_000

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

DAILY_COLUMN_TYPES: Dict[str, str] = {
    'qn_3': 'int',
    'fx': 'float',
    'fm': 'float',
    'qn_4': 'int',
    'rsk': 'float',
    'rskf': 'float',
    'sdk': 'float',
    'shk_tag': 'float',
    'nm': 'float',
    'vpm': 'float',
    'pm': 'float',
    'tmk': 'float',
    'upm': 'float',
    'txk': 'float',
    'tnk': 'float',
    'tgk': 'float',
    'eor': 'text',
}


@dataclass
class StationImportStats:
    inserted: int = 0
    updated: int = 0


@dataclass
class DailyImportStats:
    inserted: int = 0
    updated: int = 0
    archives_processed: int = 0
    archives_failed: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class ImportReport:
    stations: StationImportStats
    daily: DailyImportStats

    def to_dict(self) -> Dict[str, Dict[str, int]]:
        return {
            'stations': {
                'inserted': self.stations.inserted,
                'updated': self.stations.updated,
            },
            'daily': {
                'inserted': self.daily.inserted,
                'updated': self.daily.updated,
                'archives_processed': self.daily.archives_processed,
                'archives_failed': self.daily.archives_failed,
                'errors': list(self.daily.errors),
            },
        }


class DwdKlImporter:
    """Importer encapsulating streaming download, parsing, and upsert logic."""

    def __init__(
        self,
        app=None,
        logger: Optional[logging.Logger] = None,
        session: Optional[Session] = None,
        progress_handler: Optional[Callable[[float, str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.app = app
        self.logger = logger or logging.getLogger(__name__)
        self.session = session or self._build_session()
        self.progress_handler = progress_handler

    # --- public API ------------------------------------------------------------------

    def run_full_refresh(self) -> ImportReport:
        """Import station metadata and all available historical KL daily data."""
        with self._application_context():
            self._ensure_schema()
            self._update_progress(0.0, 'Import wird vorbereitet', {'stage': 'prepare'})
            station_stats = self._import_stations()
            self._update_progress(
                100.0,
                'Stationsdaten importiert',
                {
                    'stage': 'stations',
                    'stations_inserted': station_stats.inserted,
                    'stations_updated': station_stats.updated,
                },
            )
            daily_stats = self._import_daily_archives()
            self._update_progress(
                100.0,
                'Import der Tageswerte abgeschlossen',
                {
                    'stage': 'complete',
                    'stations_inserted': station_stats.inserted,
                    'stations_updated': station_stats.updated,
                    'daily_inserted': daily_stats.inserted,
                    'daily_updated': daily_stats.updated,
                    'archives_processed': daily_stats.archives_processed,
                    'archives_failed': daily_stats.archives_failed,
                },
            )
            return ImportReport(stations=station_stats, daily=daily_stats)

    def run_station_refresh(self) -> StationImportStats:
        """Import only station metadata."""
        with self._application_context():
            self._ensure_schema()
            self._update_progress(0.0, 'Stationsimport wird vorbereitet', {'stage': 'prepare'})
            stats = self._import_stations()
            self._update_progress(
                100.0,
                'Stationsimport abgeschlossen',
                {
                    'stage': 'complete',
                    'stations_inserted': stats.inserted,
                    'stations_updated': stats.updated,
                },
            )
            return stats

    # --- context and schema helpers --------------------------------------------------

    def _update_progress(self, percent: float, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if not self.progress_handler:
            return
        payload = dict(extra or {})
        payload.setdefault('stage', payload.get('stage', ''))
        payload.setdefault('timestamp', dt.datetime.utcnow().isoformat())
        safe_percent = max(0.0, min(100.0, float(percent)))
        self.progress_handler(safe_percent, message, payload)

    def _application_context(self):
        if self.app is not None:
            return self.app.app_context()
        try:
            from flask import current_app
        except RuntimeError:
            return nullcontext()
        return current_app.app_context()

    def _ensure_schema(self) -> None:
        ensure_weather_schema(reset=True)

    # --- station import --------------------------------------------------------------

    def _import_stations(self) -> StationImportStats:
        listing = self._fetch_listing()
        entry = listing.get(STATION_DESCRIPTION_FILE)
        if not entry:
            message = 'Station description file missing from directory listing.'
            self.logger.error(message)
            raise RuntimeError(message)

        response = self._download(entry['url'], timeout=120)
        text = response.content.decode('iso-8859-1')
        response.close()
        parsed_rows = self._parse_station_rows(text)
        if not parsed_rows:
            self.logger.warning('No station rows parsed from metadata file.')
        else:
            self.logger.info('Example station row parsed: %s', parsed_rows[0])

        conn = self._get_connection()
        timestamp = dt.datetime.utcnow().isoformat(timespec='seconds')
        inserted = 0
        updated = 0
        batch: List[Dict[str, Optional[str]]] = []

        for row in parsed_rows:
            batch.append(row)
            if len(batch) >= CHUNK_SIZE:
                inc, upd = self._persist_station_batch(conn, batch, timestamp)
                inserted += inc
                updated += upd
                batch.clear()

        if batch:
            inc, upd = self._persist_station_batch(conn, batch, timestamp)
            inserted += inc
            updated += upd

        self.logger.info('Station import complete: inserted=%s updated=%s', inserted, updated)
        return StationImportStats(inserted=inserted, updated=updated)

    def _persist_station_batch(self, conn, records: List[Dict[str, Optional[str]]], timestamp: str) -> Tuple[int, int]:
        if not records:
            return 0, 0
        station_ids = [record['station_id'] for record in records]
        existing = self._fetch_existing_station_ids(conn, station_ids)
        params = [
            (
                record['station_id'],
                record.get('station_name'),
                record.get('state'),
                record.get('latitude'),
                record.get('longitude'),
                record.get('height'),
                record.get('from_date'),
                record.get('to_date'),
                timestamp,
            )
            for record in records
        ]
        self._executemany_with_retry(
            conn,
            """
            INSERT INTO stations (
                station_id, station_name, state, latitude, longitude, height,
                from_date, to_date, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id) DO UPDATE SET
                station_name = excluded.station_name,
                state = excluded.state,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                height = excluded.height,
                from_date = excluded.from_date,
                to_date = excluded.to_date,
                updated_at = excluded.updated_at
            """,
            params,
            'stations',
        )
        inserted = len(records) - len(existing)
        updated = len(existing)
        return inserted, updated

    def _parse_station_rows(self, text: str) -> List[Dict[str, Optional[str]]]:
        lines = [line for line in text.splitlines() if line.strip() and not line.startswith('#')]
        if not lines:
            return []
        header_line = lines[0].lstrip('\ufeff')
        if ';' in header_line:
            reader = csv.reader(io.StringIO('\n'.join(lines)), delimiter=';')
            headers: Optional[List[str]] = None
            rows: List[Dict[str, Optional[str]]] = []
            for raw_row in reader:
                if not raw_row:
                    continue
                if headers is None:
                    headers = [column.strip().lower().lstrip('\ufeff') for column in raw_row]
                    self.logger.info('Station header columns detected: %s', headers)
                    continue
                payload = {
                    headers[index]: raw_row[index].strip().lstrip('\ufeff')
                    for index in range(min(len(headers), len(raw_row)))
                }
                record = self._build_station_record(payload, context='station row (; delimited)')
                if record:
                    rows.append(record)
            return rows

        # whitespace-delimited fallback
        rows: List[Dict[str, Optional[str]]] = []
        data_lines = lines[1:] if header_line.lower().startswith('stations') else lines
        self.logger.info('Station header columns detected (whitespace format): %s', header_line.split())
        for line in data_lines:
            record = self._parse_station_line_whitespace(line)
            if not record:
                continue
            built = self._build_station_record(record, context='station row (whitespace)')
            if built:
                rows.append(built)
        return rows

    def _build_station_record(self, payload: Dict[str, str], context: str) -> Optional[Dict[str, Optional[str]]]:
        station_id_raw = payload.get('stations_id') or payload.get('stationsid') or payload.get('stations-id')
        station_id = self._normalize_station_id(station_id_raw, context=context)
        if station_id is None:
            return None
        from_date = self._normalize_date(payload.get('von_datum'))
        to_date = self._normalize_date(payload.get('bis_datum'))
        latitude = self._convert_value(
            payload.get('geobreite') or payload.get('geo breite') or payload.get('geogr. breite') or payload.get('geobreite(grad)')
        )
        longitude = self._convert_value(
            payload.get('geolaenge') or payload.get('geo laenge') or payload.get('geogr. länge') or payload.get('geolaenge(grad)')
        )
        height = self._convert_value(
            payload.get('stationshoehe') or payload.get('stationshoehe m ue. nn') or payload.get('stationshoehe nn')
        )
        return {
            'station_id': station_id,
            'station_name': payload.get('stationsname') or payload.get('station_name') or None,
            'state': payload.get('bundesland') or None,
            'latitude': latitude,
            'longitude': longitude,
            'height': height,
            'from_date': from_date,
            'to_date': to_date,
        }

    def _parse_station_line_whitespace(self, line: str) -> Optional[Dict[str, str]]:
        stripped = line.strip()
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
            'stations_id': station_id,
            'von_datum': von_datum,
            'bis_datum': bis_datum,
            'stationshoehe': hoehe,
            'geobreite': lat,
            'geolaenge': lon,
            'stationsname': station_name or None,
            'bundesland': bundesland,
            'abgabe': abgabe,
        }

    def _fetch_existing_station_ids(self, conn, station_ids: Sequence[int]) -> set:
        if not station_ids:
            return set()
        placeholders = ','.join('?' for _ in station_ids)
        rows = conn.execute(
            f'SELECT station_id FROM stations WHERE station_id IN ({placeholders})',
            tuple(station_ids),
        ).fetchall()
        return {int(row['station_id']) for row in rows}

    # --- daily data import ------------------------------------------------------------

    def _import_daily_archives(self) -> DailyImportStats:
        listing = self._fetch_listing()
        archives = [
            (name, meta)
            for name, meta in listing.items()
            if name.endswith(ARCHIVE_SUFFIX)
        ]
        if not archives:
            message = 'No historical archives discovered in directory listing.'
            self.logger.error(message)
            raise RuntimeError(message)

        archives.sort(key=lambda item: item[0])
        total_archives = len(archives)
        conn = self._get_connection()
        stats = DailyImportStats()
        self._update_progress(
            0.0,
            'Dateien werden vorbereitet',
            {
                'stage': 'daily',
                'archives_total': total_archives,
                'archives_processed': 0,
                'archives_failed': 0,
                'daily_inserted': 0,
                'daily_updated': 0,
            },
        )
        for index, (filename, meta) in enumerate(archives, start=1):
            file_inserted = 0
            file_updated = 0
            success = False
            try:
                self.logger.info('Processing archive %s', filename)
                file_stats = self._import_single_archive(conn, meta['url'], filename)
                file_inserted = file_stats.inserted
                file_updated = file_stats.updated
                stats.inserted += file_inserted
                stats.updated += file_updated
                stats.archives_processed += 1
                success = True
            except Exception as exc:  # pragma: no cover - defensive
                stats.archives_failed += 1
                error_message = f'{filename}: {exc}'
                stats.errors.append(error_message)
                self.logger.exception('Failed to process archive %s', filename)
            finally:
                processed_total = stats.archives_processed + stats.archives_failed
                fraction = processed_total / total_archives if total_archives else 1.0
                percent = fraction * 100.0
                detail = {
                    'stage': 'daily',
                    'current_archive': filename,
                    'archives_total': total_archives,
                    'archives_processed': stats.archives_processed,
                    'archives_failed': stats.archives_failed,
                    'daily_inserted': stats.inserted,
                    'daily_updated': stats.updated,
                    'last_archive_inserted': file_inserted,
                    'last_archive_updated': file_updated,
                    'last_archive_success': success,
                }
                message = (
                    f'Verarbeite Datei {processed_total}/{total_archives}'
                    if total_archives
                    else 'Tagesdateien werden verarbeitet'
                )
                self._update_progress(percent, message, detail)
        return stats

    def _import_single_archive(self, conn, url: str, filename: str) -> DailyImportStats:
        response = self._download(url, stream=True, timeout=300)
        try:
            with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024) as tmp_file:
                for chunk in response.iter_content(chunk_size=512 * 1024):
                    if chunk:
                        tmp_file.write(chunk)
                tmp_file.seek(0)
                with zipfile.ZipFile(tmp_file) as archive:
                    members = [name for name in archive.namelist() if name.endswith('.txt')]
                    if not members:
                        raise RuntimeError('Archive does not contain a data file.')
                    target_name = next(
                        (name for name in members if 'produkt_klima_tag_' in name.lower()),
                        members[0],
                    )
                    with archive.open(target_name) as data_file:
                        return self._parse_and_store_daily(conn, data_file, filename)
        finally:
            response.close()

    def _parse_and_store_daily(self, conn, data_file, filename: str) -> DailyImportStats:
        stats = DailyImportStats()
        reader = csv.reader(io.TextIOWrapper(data_file, encoding='iso-8859-1'), delimiter=';')
        headers: Optional[List[str]] = None
        batch: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
        for row in reader:
            if not row:
                continue
            if row[0].startswith('#'):
                continue
            if headers is None:
                headers = [column.strip().lower() for column in row]
                continue
            payload = {headers[index]: row[index].strip() for index in range(min(len(headers), len(row)))}
            normalized = self._normalize_daily_record(payload, filename)
            if not normalized:
                continue
            key = (normalized['station_id'], normalized['date'])
            batch[key] = normalized
            if len(batch) >= CHUNK_SIZE:
                inc, upd = self._persist_daily_batch(conn, batch.values())
                stats.inserted += inc
                stats.updated += upd
                batch.clear()
        if batch:
            inc, upd = self._persist_daily_batch(conn, batch.values())
            stats.inserted += inc
            stats.updated += upd
        return stats

    def _persist_daily_batch(self, conn, records: Iterable[Dict[str, Optional[str]]]) -> Tuple[int, int]:
        records = list(records)
        if not records:
            return 0, 0
        pairs = [(record['station_id'], record['date']) for record in records]
        existing = self._fetch_existing_daily_pairs(conn, pairs)
        timestamp = dt.datetime.utcnow().isoformat(timespec='seconds')
        params = [
            (
                record['station_id'],
                record['date'],
                record.get('qn_3'),
                record.get('fx'),
                record.get('fm'),
                record.get('qn_4'),
                record.get('rsk'),
                record.get('rskf'),
                record.get('sdk'),
                record.get('shk_tag'),
                record.get('nm'),
                record.get('vpm'),
                record.get('pm'),
                record.get('tmk'),
                record.get('upm'),
                record.get('txk'),
                record.get('tnk'),
                record.get('tgk'),
                record.get('eor'),
                record.get('source_filename'),
                timestamp,
            )
            for record in records
        ]
        self._executemany_with_retry(
            conn,
            """
            INSERT INTO daily_kl (
                station_id, date, qn_3, fx, fm, qn_4, rsk, rskf, sdk, shk_tag,
                nm, vpm, pm, tmk, upm, txk, tnk, tgk, eor, source_filename, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, date) DO UPDATE SET
                qn_3 = excluded.qn_3,
                fx = excluded.fx,
                fm = excluded.fm,
                qn_4 = excluded.qn_4,
                rsk = excluded.rsk,
                rskf = excluded.rskf,
                sdk = excluded.sdk,
                shk_tag = excluded.shk_tag,
                nm = excluded.nm,
                vpm = excluded.vpm,
                pm = excluded.pm,
                tmk = excluded.tmk,
                upm = excluded.upm,
                txk = excluded.txk,
                tnk = excluded.tnk,
                tgk = excluded.tgk,
                eor = excluded.eor,
                source_filename = excluded.source_filename,
                updated_at = excluded.updated_at
            """,
            params,
            'daily_kl',
        )
        inserted = len(records) - len(existing)
        updated = len(existing)
        return inserted, updated

    def _fetch_existing_daily_pairs(self, conn, pairs: Sequence[Tuple[int, str]]) -> set:
        if not pairs:
            return set()
        placeholders = ','.join(['(?, ?)'] * len(pairs))
        flattened = [value for pair in pairs for value in pair]
        rows = conn.execute(
            f'SELECT station_id, date FROM daily_kl WHERE (station_id, date) IN ({placeholders})',
            tuple(flattened),
        ).fetchall()
        return {(int(row['station_id']), row['date']) for row in rows}

    def _normalize_daily_record(self, payload: Dict[str, str], filename: str) -> Optional[Dict[str, Optional[str]]]:
        station_id_raw = payload.get('stations_id')
        date_raw = payload.get('mess_datum')
        station_id = self._normalize_station_id(station_id_raw, context=f'daily record from {filename}')
        if station_id is None or not date_raw:
            return None
        normalized: Dict[str, Optional[str]] = {
            'station_id': station_id,
            'date': self._normalize_date(date_raw),
            'source_filename': filename,
        }
        for column, column_type in DAILY_COLUMN_TYPES.items():
            normalized[column] = self._convert_value(payload.get(column), column_type)
        return normalized

    # --- remote listing and parsing ---------------------------------------------------

    def _fetch_listing(self) -> Dict[str, Dict[str, str]]:
        response = self._download(BASE_URL, timeout=60)
        try:
            content = response.text
        finally:
            response.close()
        entries: Dict[str, Dict[str, str]] = {}
        for href, last_modified in self._extract_links(content):
            entries[href] = {
                'url': urljoin(BASE_URL, href),
                'last_modified': last_modified or '',
            }
        return entries

    def _extract_links(self, html: str) -> Iterable[Tuple[str, Optional[str]]]:
        href_pattern = re.compile(r'href=\"([^\"]+)\"')
        date_pattern = re.compile(r'(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})')
        for line in html.splitlines():
            href_match = href_pattern.search(line)
            if not href_match:
                continue
            href = href_match.group(1)
            if href.endswith('/'):
                continue
            if not (href.endswith('.zip') or href.endswith('.txt')):
                continue
            last_modified = None
            date_match = date_pattern.search(line)
            if date_match:
                try:
                    parsed = dt.datetime.strptime(date_match.group(1), '%d-%b-%Y %H:%M')
                    last_modified = parsed.replace(tzinfo=dt.timezone.utc).isoformat()
                except ValueError:
                    last_modified = None
            yield href, last_modified

    def _download(self, url: str, stream: bool = False, timeout: int = 30) -> Response:
        self.logger.debug('Downloading %s', url)
        response = self.session.get(url, stream=stream, timeout=timeout)
        response.raise_for_status()
        return response

    # --- value normalization helpers --------------------------------------------------

    def _convert_value(self, value: Optional[str], column_type: str = 'float'):
        if value is None:
            return None
        value = value.strip()
        if not value or value in SENTINEL_VALUES:
            return None
        value = value.replace(',', '.')
        if column_type == 'int':
            try:
                return int(float(value))
            except ValueError:
                return None
        if column_type == 'float':
            try:
                return float(value)
            except ValueError:
                return None
        return value or None

    def _normalize_station_id(self, value: Optional[str], *, context: str = '') -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip().lstrip('\ufeff')
        if not text:
            return None
        if not text.isdigit():
            self.logger.warning('Non-numeric station_id %r encountered in %s', value, context or 'record')
            return None
        station_id = int(text)
        if station_id <= 0:
            self.logger.warning('Out-of-range station_id %r encountered in %s', value, context or 'record')
            return None
        return station_id

    def _normalize_date(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = value.strip()
        if not value or value in SENTINEL_VALUES:
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

    def _build_session(self) -> Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=1.5,
            status_forcelist=(500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        session.headers.update({'User-Agent': 'weather-analytics-kl-importer/1.0'})
        return session

    def _get_connection(self):
        conn = get_db()
        try:
            conn.execute(f'PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}')
        except sqlite3.OperationalError:
            pass
        return conn

    def _executemany_with_retry(self, conn, sql: str, params: Iterable[Tuple], label: str) -> None:
        params_list = list(params)
        attempts = 0
        while True:
            try:
                with conn:
                    conn.executemany(sql, params_list)
                return
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if 'locked' in message and attempts < SQLITE_LOCK_RETRIES:
                    attempts += 1
                    wait_time = SQLITE_LOCK_SLEEP * attempts
                    self.logger.warning(
                        'SQLite locked while writing to %s (attempt %s/%s). Retrying in %.1fs.',
                        label,
                        attempts,
                        SQLITE_LOCK_RETRIES,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                raise


def import_full_history(app, progress_handler: Optional[Callable[[float, str, Dict[str, Any]], None]] = None) -> Dict[str, Dict[str, int]]:
    """Entry point used by Flask views to trigger the full import."""
    importer = DwdKlImporter(app=app, logger=app.logger, progress_handler=progress_handler)
    report = importer.run_full_refresh()
    return report.to_dict()


def import_station_metadata(app, progress_handler: Optional[Callable[[float, str, Dict[str, Any]], None]] = None) -> Dict[str, int]:
    """Entry point used by other components for station metadata refresh."""
    importer = DwdKlImporter(app=app, logger=app.logger, progress_handler=progress_handler)
    stats = importer.run_station_refresh()
    return {'inserted': stats.inserted, 'updated': stats.updated}
