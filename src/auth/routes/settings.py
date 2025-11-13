"""Settings and admin dashboard routes."""

from __future__ import annotations

import datetime as dt

from flask import current_app, flash, make_response, redirect, render_template, request, session, url_for
from flask_login import current_user, login_required

from ..blueprint import auth_bp
from ..services import locale as locale_service
from ..services.api_keys import list_api_keys
from ..services.users import is_current_user_admin


@auth_bp.get('/settings')
@login_required
def admin():
    locale = locale_service.build_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    is_admin = is_current_user_admin()
    sections = ['password']
    if is_admin:
        sections.insert(0, 'data')
    else:
        sections.insert(0, 'api_keys')

    requested = request.args.get('section') or sections[0]
    if requested not in sections:
        requested = sections[0]

    api_keys = []
    api_key_now = None
    if requested == 'api_keys' and not is_admin:
        api_keys = list_api_keys(int(current_user.id))
        api_key_now = dt.datetime.utcnow().isoformat(timespec='seconds')

    new_api_key = session.pop('new_api_key_value', None)
    public_base = (current_app.config.get('PUBLIC_BASE_URL') or '').rstrip('/')
    swagger_url = f'{public_base}/docs' if public_base else url_for('swagger_docs', _external=True)
    public_api_key = current_app.config.get('PUBLIC_API_KEY', '')

    response = make_response(
        render_template(
            'settings.html',
            active_section=requested,
            available_sections=sections,
            is_admin=is_admin,
            api_keys=api_keys,
            api_key_now=api_key_now,
            new_api_key=new_api_key if not is_admin else None,
            swagger_url=swagger_url,
            public_api_key=public_api_key,
            ui=ui_strings,
            js_strings=js_strings,
            i18n_messages=messages,
            lang=locale['lang'],
            current_language=locale['lang'],
        )
    )
    return locale_service.maybe_set_language_cookie(response, locale)


@auth_bp.post('/admin/change-password')
@login_required
def change_password():
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    current_pw = request.form.get('current_password') or ''
    new_pw = request.form.get('new_password') or ''
    confirm_pw = request.form.get('confirm_password') or ''

    if not current_user.check_password(current_pw):
        flash(
            locale_service.format_message(messages, 'auth_password_current_invalid', 'Aktuelles Passwort ist falsch.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    if not new_pw:
        flash(
            locale_service.format_message(messages, 'auth_password_new_required', 'Neues Passwort darf nicht leer sein.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    if new_pw != confirm_pw:
        flash(
            locale_service.format_message(
                messages,
                'auth_password_mismatch',
                'Neues Passwort und Bestaetigung stimmen nicht ueberein.',
            ),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    current_user.update_password(new_pw)
    flash(
        locale_service.format_message(messages, 'auth_password_updated', 'Passwort wurde aktualisiert.'),
        'success',
    )
    return redirect(url_for('auth.admin', section='password'))
