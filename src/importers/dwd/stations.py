"""Station import mixin for the DWD importer."""

from __future__ import annotations

import csv
import datetime as dt
import io
from typing import Dict, List, Optional, Sequence, Tuple

from .constants import CHUNK_SIZE, GERMAN_STATE_NAMES, STATION_DESCRIPTION_FILE
from .models import StationImportStats


class StationImportMixin:
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

        rows = []
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
            payload.get('geolaenge') or payload.get('geo laenge') or payload.get('geogr. laenge') or payload.get('geolaenge(grad)')
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


__all__ = ['StationImportMixin']
