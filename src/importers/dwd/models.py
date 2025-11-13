"""Dataclasses describing import progress/state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


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


__all__ = ['DailyImportStats', 'ImportReport', 'StationImportStats']
