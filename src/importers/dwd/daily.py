"""Daily archive import mixin."""

from __future__ import annotations

import csv
import datetime as dt
import io
import tempfile
import zipfile
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .constants import ARCHIVE_SUFFIX, CHUNK_SIZE, DAILY_COLUMN_TYPES
from .models import DailyImportStats


class DailyImportMixin:
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
        }
        for column, col_type in DAILY_COLUMN_TYPES.items():
            normalized[column] = self._convert_value(payload.get(column), col_type)
        normalized['source_filename'] = filename
        return normalized


__all__ = ['DailyImportMixin']
