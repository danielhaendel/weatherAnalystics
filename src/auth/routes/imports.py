"""Routes for triggering data imports from the admin UI."""

from __future__ import annotations

from flask import current_app, flash, jsonify, redirect, request, url_for
from flask_login import login_required

from ..blueprint import auth_bp
from ..services import locale as locale_service
from ..services.imports import (
    ImportJobError,
    fetch_import_job,
    launch_background_import,
    sync_station_metadata,
    sync_weather_history,
)
from ..services.users import is_current_user_admin


@auth_bp.post('/admin/import/start')
@login_required
def admin_import_start():
    if not is_current_user_admin():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    payload = request.get_json(silent=True) or {}
    kind = (payload.get('kind') or '').strip().lower()
    try:
        job = launch_background_import(kind)
    except ImportJobError as err:
        return jsonify({'ok': False, 'error': err.code}), err.status_code
    return jsonify({'ok': True, 'job_id': job.job_id})


@auth_bp.get('/admin/import/<job_id>')
@login_required
def admin_import_status(job_id: str):
    if not is_current_user_admin():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    try:
        job = fetch_import_job(job_id)
    except ImportJobError as err:
        return jsonify({'ok': False, 'error': err.code}), err.status_code
    return jsonify({'ok': True, 'job': job})


@auth_bp.post('/admin/sync-stations')
@login_required
def sync_stations_admin():
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    if not is_current_user_admin():
        flash(locale_service.format_message(messages, 'auth_permission_denied', 'Keine Berechtigung.'), 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        stats = sync_station_metadata()
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Station sync failed in admin view: %s', exc)
        flash(
            locale_service.format_message(messages, 'auth_station_sync_failed', 'Stationsdaten konnten nicht aktualisiert werden.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='data'))

    inserted = stats.get('inserted', 0)
    updated = stats.get('updated', 0)
    total = inserted + updated
    flash(
        locale_service.format_message(
            messages,
            'auth_station_sync_success',
            'Stationsdaten aktualisiert (neu: {inserted}, aktualisiert: {updated}, gesamt: {total}).',
            inserted=inserted,
            updated=updated,
            total=total,
        ),
        'success',
    )
    return redirect(url_for('auth.admin', section='data'))


@auth_bp.post('/admin/sync-weather')
@login_required
def sync_weather_admin():
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    if not is_current_user_admin():
        flash(locale_service.format_message(messages, 'auth_permission_denied', 'Keine Berechtigung.'), 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        report = sync_weather_history()
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Weather sync failed in admin view: %s', exc)
        flash(
            locale_service.format_message(messages, 'auth_weather_sync_failed', 'Wetterdaten konnten nicht aktualisiert werden.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='data'))

    stations = report.get('stations', {}) if isinstance(report, dict) else {}
    daily = report.get('daily', {}) if isinstance(report, dict) else {}

    inserted_stations = stations.get('inserted', 0)
    updated_stations = stations.get('updated', 0)
    inserted_daily = daily.get('inserted', 0)
    updated_daily = daily.get('updated', 0)
    archives_failed = daily.get('archives_failed', 0)
    archives_processed = daily.get('archives_processed', 0)

    summary_message = locale_service.format_message(
        messages,
        'auth_weather_sync_summary',
        (
            'Wetterdaten vollstaendig aktualisiert. '
            'Stationen neu: {inserted_stations}, aktualisiert: {updated_stations}. '
            'Tageswerte neu: {inserted_daily}, aktualisiert: {updated_daily}. '
            'ZIP-Dateien verarbeitet: {archives_processed}, fehlgeschlagen: {archives_failed}.'
        ),
        inserted_stations=inserted_stations,
        updated_stations=updated_stations,
        inserted_daily=inserted_daily,
        updated_daily=updated_daily,
        archives_processed=archives_processed,
        archives_failed=archives_failed,
    )

    if archives_failed:
        flash(summary_message, 'warning')
        errors = daily.get('errors') or []
        for error in errors[:3]:
            flash(
                locale_service.format_message(messages, 'auth_weather_sync_error', 'Fehler: {error}', error=error),
                'error',
            )
    else:
        flash(summary_message, 'success')
    return redirect(url_for('auth.admin', section='data'))
