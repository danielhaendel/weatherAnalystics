"""High-level importer combining the mixins."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from .core import DwdImporterCore
from .daily import DailyImportMixin
from .models import ImportReport, StationImportStats
from .stations import StationImportMixin


class DwdKlImporter(StationImportMixin, DailyImportMixin, DwdImporterCore):
    """Importer encapsulating streaming download, parsing, and upsert logic."""

    def run_full_refresh(self) -> ImportReport:
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


__all__ = ['DwdKlImporter', 'import_full_history', 'import_station_metadata']
