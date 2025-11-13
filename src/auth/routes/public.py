"""Public-facing auth routes (login/registration)."""

from __future__ import annotations

from flask import flash, make_response, redirect, render_template, request, url_for
from flask_login import current_user, login_required, login_user, logout_user

from ..blueprint import auth_bp
from ..services import locale as locale_service
from ..services.users import User, authenticate_user, create_user_account, is_safe_redirect


@auth_bp.route('/login', methods=('GET', 'POST'))
def login():
    if current_user.is_authenticated:
        return redirect(url_for('auth.admin'))

    locale = locale_service.build_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    error = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        user = authenticate_user(username, password)
        if not user:
            error = locale_service.format_message(
                messages,
                'auth_login_invalid',
                'Ungueltige Kombination aus Benutzername und Passwort.',
            )
        else:
            login_user(user)
            next_url = request.args.get('next')
            if not is_safe_redirect(next_url):
                next_url = url_for('auth.admin')
            flash(
                locale_service.format_message(messages, 'auth_login_success', 'Erfolgreich angemeldet.'),
                'success',
            )
            return redirect(next_url)

    response = make_response(
        render_template(
            'login.html',
            error=error,
            ui=ui_strings,
            js_strings=js_strings,
            lang=locale['lang'],
            current_language=locale['lang'],
        )
    )
    return locale_service.maybe_set_language_cookie(response, locale)


@auth_bp.route('/register', methods=('GET', 'POST'))
def register():
    if current_user.is_authenticated:
        return redirect(url_for('auth.admin'))

    locale = locale_service.build_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    error = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        confirm = request.form.get('confirm_password') or ''
        if not username:
            error = locale_service.format_message(
                messages,
                'auth_register_username_required',
                'Benutzername darf nicht leer sein.',
            )
        elif not password:
            error = locale_service.format_message(
                messages,
                'auth_register_password_required',
                'Passwort darf nicht leer sein.',
            )
        elif password != confirm:
            error = locale_service.format_message(
                messages,
                'auth_register_password_mismatch',
                'Passwoerter stimmen nicht ueberein.',
            )
        elif User.get_by_username(username):
            error = locale_service.format_message(
                messages,
                'auth_register_username_taken',
                'Benutzername ist bereits vergeben.',
            )
        else:
            user = create_user_account(username, password)
            login_user(user)
            flash(
                locale_service.format_message(messages, 'auth_register_success', 'Konto erstellt.'),
                'success',
            )
            return redirect(url_for('auth.admin'))

    response = make_response(
        render_template(
            'register.html',
            error=error,
            ui=ui_strings,
            js_strings=js_strings,
            lang=locale['lang'],
            current_language=locale['lang'],
        )
    )
    return locale_service.maybe_set_language_cookie(response, locale)


@auth_bp.post('/logout')
@login_required
def logout():
    locale = locale_service.build_locale_bundle()
    messages = locale['messages']
    logout_user()
    flash(locale_service.format_message(messages, 'auth_logout_success', 'Sie wurden abgemeldet.'), 'info')
    return redirect(url_for('auth.login'))
