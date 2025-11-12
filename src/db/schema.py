"""Database schema helpers for ensuring required SQLite tables exist."""

from __future__ import annotations

from typing import Tuple

from . import execute_script, get_db


STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stations (
    station_id INTEGER PRIMARY KEY,
    station_name TEXT,
    state TEXT,
    latitude REAL,
    longitude REAL,
    height REAL,
    from_date TEXT,
    to_date TEXT,
    updated_at TEXT NOT NULL
)
"""

DAILY_KL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_kl (
    station_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    qn_3 INTEGER,
    fx REAL,
    fm REAL,
    qn_4 INTEGER,
    rsk REAL,
    rskf REAL,
    sdk REAL,
    shk_tag REAL,
    nm REAL,
    vpm REAL,
    pm REAL,
    tmk REAL,
    upm REAL,
    txk REAL,
    tnk REAL,
    tgk REAL,
    eor TEXT,
    source_filename TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (station_id, date)
)
"""

DAILY_KL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_daily_kl_station_date
ON daily_kl (station_id, date)
"""

DROP_TABLE_STATEMENTS = (
    "DROP TABLE IF EXISTS daily_kl;",
    "DROP TABLE IF EXISTS stations;",
)

STATION_REQUIRED_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ('station_name', 'TEXT'),
    ('state', 'TEXT'),
    ('latitude', 'REAL'),
    ('longitude', 'REAL'),
    ('height', 'REAL'),
    ('from_date', 'TEXT'),
    ('to_date', 'TEXT'),
    ('updated_at', 'TEXT'),
)


def ensure_station_columns(conn) -> None:
    """Add missing station columns for older database versions."""
    existing_columns = {row['name'] for row in conn.execute('PRAGMA table_info(stations)')}
    missing = [(column, column_type) for column, column_type in STATION_REQUIRED_COLUMNS if column not in existing_columns]
    for column, column_type in missing:
        conn.execute(f'ALTER TABLE stations ADD COLUMN {column} {column_type}')
    if missing:
        conn.commit()


def ensure_weather_schema(*, reset: bool = False) -> None:
    """Ensure the weather data tables exist and include required columns."""
    statements = []
    if reset:
        statements.extend(DROP_TABLE_STATEMENTS)
    statements.extend([STATIONS_TABLE_SQL, DAILY_KL_TABLE_SQL, DAILY_KL_INDEX_SQL])
    execute_script(statements)
    conn = get_db()
    ensure_station_columns(conn)


__all__ = ['ensure_weather_schema', 'ensure_station_columns']
