"""API key management routes."""

from __future__ import annotations

from flask import flash, redirect, request, session, url_for
from flask_login import current_user, login_required

from ..blueprint import auth_bp
from ..services import locale as locale_service
from ..services import api_keys as api_key_service
from ..services.users import is_current_user_admin


@auth_bp.post('/settings/api-keys/create')
@login_required
def create_api_key():
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    if is_current_user_admin():
        flash(
            locale_service.format_message(messages, 'auth_api_keys_user_only', 'API-Keys stehen nur Benutzerkonten zur Verfuegung.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    name = (request.form.get('name') or '').strip() or 'API-Key'
    try:
        days = int(request.form.get('expires_in') or 90)
    except (TypeError, ValueError):
        days = 90
    days = max(1, min(360, days))

    key_value = api_key_service.create_api_key(int(current_user.id), name, days)
    session['new_api_key_value'] = key_value
    flash(locale_service.format_message(messages, 'auth_api_key_created', 'API-Key erstellt.'), 'success')
    return redirect(url_for('auth.admin', section='api_keys'))


@auth_bp.post('/settings/api-keys/<int:key_id>/delete')
@login_required
def delete_api_key(key_id: int):
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    if is_current_user_admin():
        flash(
            locale_service.format_message(messages, 'auth_api_keys_user_only', 'API-Keys stehen nur Benutzerkonten zur Verfuegung.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))
    deleted = api_key_service.delete_api_key(int(current_user.id), key_id)
    if deleted:
        flash(locale_service.format_message(messages, 'auth_api_key_deleted', 'API-Key geloescht.'), 'success')
    else:
        flash(
            locale_service.format_message(messages, 'auth_api_key_delete_failed', 'API-Key konnte nicht geloescht werden.'),
            'error',
        )
    return redirect(url_for('auth.admin', section='api_keys'))
