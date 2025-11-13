"""Core utilities shared by the DWD importer mixins."""

from __future__ import annotations

import datetime as dt
import logging
import re
import sqlite3
import time
from contextlib import nullcontext
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry

from ...db import get_db
from ...db.schema import ensure_weather_schema
from .constants import (
    BASE_URL,
    SENTINEL_VALUES,
    SQLITE_BUSY_TIMEOUT_MS,
    SQLITE_LOCK_RETRIES,
    SQLITE_LOCK_SLEEP,
)


class DwdImporterCore:
    """Base functionality shared across station and daily imports."""

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

    def _ensure_schema(self, *, reset: bool = False) -> None:
        ensure_weather_schema(reset=reset)

    def _fetch_listing(self) -> Dict[str, Dict[str, str]]:
        response = self._download(BASE_URL)
        data = response.text
        response.close()
        listing: Dict[str, Dict[str, str]] = {}
        for href, last_modified in self._extract_links(data):
            url = urljoin(BASE_URL, href)
            listing[href] = {'url': url, 'last_modified': last_modified}
        return listing

    def _extract_links(self, html: str):
        href_pattern = re.compile(r'href="([^"]+)"')
        date_pattern = re.compile(r'Last modified\s+([0-9]{2}-[A-Za-z]{3}-[0-9]{4}\s+[0-9]{2}:[0-9]{2})')
        for match in href_pattern.finditer(html):
            href = match.group(1)
            if not href.lower().endswith(('.zip', '.txt', '/')):
                continue
            if href.endswith('/'):
                continue
            last_modified = None
            surrounding = html[max(0, match.start() - 200):match.end() + 200]
            date_match = date_pattern.search(surrounding)
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

    def _convert_value(self, value: Optional[str], column_type: str = 'float'):
        if value is None:
            return None
        text = value.strip()
        if not text or text in SENTINEL_VALUES:
            return None
        text = text.replace(',', '.')
        if column_type == 'int':
            try:
                return int(float(text))
            except ValueError:
                return None
        if column_type == 'float':
            try:
                return float(text)
            except ValueError:
                return None
        return text or None

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


__all__ = ['DwdImporterCore']
