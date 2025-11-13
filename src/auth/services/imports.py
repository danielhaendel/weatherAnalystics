"""Helpers for triggering data imports from the auth admin area."""

from __future__ import annotations

from flask import current_app

from ...importers import import_full_history, import_station_metadata
from ...jobs import get_job, start_job


class ImportJobError(Exception):
    def __init__(self, code: str, status_code: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


def launch_background_import(kind: str):
    app_obj = current_app._get_current_object()
    if kind == 'stations':
        job = start_job('stations', import_station_metadata, args=(app_obj,))
    elif kind == 'weather':
        job = start_job('weather', import_full_history, args=(app_obj,))
    else:
        raise ImportJobError('invalid_kind', 400)
    return job


def fetch_import_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise ImportJobError('job_not_found', 404)
    return job.to_dict()


def sync_station_metadata():
    app_obj = current_app
    return import_station_metadata(app_obj)


def sync_weather_history():
    app_obj = current_app
    return import_full_history(app_obj)


__all__ = [
    'ImportJobError',
    'fetch_import_job',
    'launch_background_import',
    'sync_station_metadata',
    'sync_weather_history',
]
