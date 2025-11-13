"""Shared constants for the DWD importer."""

from __future__ import annotations

BASE_URL = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/'
STATION_DESCRIPTION_FILE = 'KL_Tageswerte_Beschreibung_Stationen.txt'
ARCHIVE_SUFFIX = '_hist.zip'
SENTINEL_VALUES = {'-999', '-999.0', '-9999', '-9999.0'}
CHUNK_SIZE = 500
SQLITE_LOCK_RETRIES = 5
SQLITE_LOCK_SLEEP = 1.0
SQLITE_BUSY_TIMEOUT_MS = 60_000

GERMAN_STATE_NAMES = {
    'Baden-Wuerttemberg',
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
    'Thueringen',
}

DAILY_COLUMN_TYPES = {
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

__all__ = [
    'ARCHIVE_SUFFIX',
    'BASE_URL',
    'CHUNK_SIZE',
    'DAILY_COLUMN_TYPES',
    'GERMAN_STATE_NAMES',
    'SENTINEL_VALUES',
    'SQLITE_BUSY_TIMEOUT_MS',
    'SQLITE_LOCK_RETRIES',
    'SQLITE_LOCK_SLEEP',
    'STATION_DESCRIPTION_FILE',
]
