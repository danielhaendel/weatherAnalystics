"""Authentication and admin views based on Flask-Login."""

from __future__ import annotations

import datetime as dt
import secrets
from typing import Optional
from urllib.parse import urljoin, urlparse

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash

from .db import execute_script, get_db
from .dwd_kl_importer import import_full_history, import_station_metadata
from .job_manager import get_job, start_job

auth_bp = Blueprint('auth', __name__)

login_manager = LoginManager()

USER_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS users
    (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        username      TEXT UNIQUE NOT NULL,
        password_hash TEXT        NOT NULL,
        is_admin      INTEGER     NOT NULL DEFAULT 0,
        created_at    TEXT        NOT NULL,
        updated_at    TEXT        NOT NULL
    )
    """,
)

API_KEY_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS api_keys
    (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        name       TEXT    NOT NULL,
        api_key    TEXT    NOT NULL,
        created_at TEXT    NOT NULL,
        expires_at TEXT    NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys (user_id)
    """,
)

USER_REQUIRED_COLUMNS = (
    ('is_admin', 'INTEGER NOT NULL DEFAULT 0'),
)


def _get_locale_bundle():
    translations = current_app.config.get('APP_TRANSLATIONS') or {}
    supported = tuple(current_app.config.get('APP_SUPPORTED_LANGUAGES') or ())
    default_lang = current_app.config.get('APP_DEFAULT_LANGUAGE') or 'de'

    query_lang = (request.args.get('lang') or '').strip()
    cookie_lang = request.cookies.get('lang')

    lang = default_lang
    set_cookie = False
    if query_lang in supported:
        lang = query_lang
        set_cookie = True
    elif cookie_lang in supported:
        lang = cookie_lang
    else:
        best = request.accept_languages.best_match(supported) if supported else None
        if best:
            lang = best

    translation = translations.get(lang) or translations.get(default_lang) or {}
    return {
        'lang': lang,
        'set_cookie': set_cookie,
        'ui': translation.get('ui', {}),
        'js': translation.get('js', {}),
        'messages': translation.get('messages', {}),
    }


def _maybe_set_language_cookie(response, locale_bundle):
    if not locale_bundle.get('set_cookie'):
        return response
    response.set_cookie(
        'lang',
        locale_bundle['lang'],
        max_age=60 * 60 * 24 * 365,
        samesite='Lax',
    )
    return response


def _format_message(messages, key, fallback, **params):
    template = messages.get(key, fallback)
    try:
        return template.format(**params)
    except Exception:
        return template


def _localize_login_message(message, **values):
    bundle = _get_locale_bundle()
    messages = bundle.get('messages', {})
    if message == 'login_required':
        return _format_message(messages, 'auth_login_required', 'Bitte melden Sie sich an, um fortzufahren.')
    template = messages.get(message, message)
    try:
        return template.format(**values)
    except Exception:
        return template


login_manager.login_view = 'auth.login'
login_manager.login_message = 'login_required'
login_manager.localize_callback = _localize_login_message


def _row_is_admin(row) -> int:
    if row is None:
        return 0
    try:
        value = row['is_admin']
        if value in (None, ''):
            return 0
        return int(value)
    except (KeyError, IndexError, TypeError, ValueError):
        return 0


def _is_current_user_admin() -> bool:
    try:
        user_id = int(getattr(current_user, 'id', 0))
    except (TypeError, ValueError):
        return False
    row = get_db().execute('SELECT is_admin FROM users WHERE id = ?', (user_id,)).fetchone()
    return bool(row and _row_is_admin(row))


class User(UserMixin):
    """Lightweight user wrapper that works with Flask-Login."""

    def __init__(self, user_id: int, username: str, password_hash: str, is_admin: int) -> None:
        self.id = str(user_id)
        self.username = username
        self.password_hash = password_hash
        self.is_admin = bool(is_admin)

    @classmethod
    def from_row(cls, row) -> Optional['User']:
        if row is None:
            return None
        return cls(row['id'], row['username'], row['password_hash'], _row_is_admin(row))

    @classmethod
    def get_by_id(cls, user_id: str) -> Optional['User']:
        conn = get_db()
        row = conn.execute(
            'SELECT id, username, password_hash FROM users WHERE id = ?',
            (user_id,),
        ).fetchone()
        return cls.from_row(row)

    @classmethod
    def get_by_username(cls, username: str) -> Optional['User']:
        conn = get_db()
        row = conn.execute(
            'SELECT id, username, password_hash FROM users WHERE username = ?',
            (username,),
        ).fetchone()
        return cls.from_row(row)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def update_password(self, password: str) -> None:
        conn = get_db()
        now = dt.datetime.utcnow().isoformat(timespec='seconds')
        with conn:
            conn.execute(
                'UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?',
                (generate_password_hash(password), now, self.id),
            )


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    return User.get_by_id(user_id)


def is_safe_redirect(target: Optional[str]) -> bool:
    if not target:
        return False
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return (
            test_url.scheme in {'http', 'https'} and
            ref_url.netloc == test_url.netloc
    )


def ensure_user_columns() -> None:
    conn = get_db()
    existing = {row['name'] for row in conn.execute('PRAGMA table_info(users)')}
    missing = [(column, col_type) for column, col_type in USER_REQUIRED_COLUMNS if column not in existing]
    for column, col_type in missing:
        conn.execute(f'ALTER TABLE users ADD COLUMN {column} {col_type}')
    if missing:
        conn.commit()


def ensure_default_admin() -> None:
    conn = get_db()
    now = dt.datetime.utcnow().isoformat(timespec='seconds')
    with conn:
        row = conn.execute(
            'SELECT id FROM users WHERE username = ? LIMIT 1',
            ('admin',),
        ).fetchone()
        if row is None:
            conn.execute(
                'INSERT INTO users (username, password_hash, is_admin, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
                ('admin', generate_password_hash('admin'), 1, now, now),
            )
        else:
            conn.execute(
                'UPDATE users SET is_admin = 1 WHERE username = ? AND (is_admin IS NULL OR is_admin = 0)',
                ('admin',),
            )


def get_api_keys_for_user(user_id: int):
    conn = get_db()
    return conn.execute(
        '''
        SELECT id, name, api_key, created_at, expires_at
        FROM api_keys
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        ''',
        (user_id,),
    ).fetchall()


def init_auth(app) -> None:
    login_manager.init_app(app)
    with app.app_context():
        execute_script((*USER_SCHEMA, *API_KEY_SCHEMA))
        ensure_user_columns()
        ensure_default_admin()


@auth_bp.route('/login', methods=('GET', 'POST'))
def login():
    if current_user.is_authenticated:
        return redirect(url_for('auth.admin'))

    locale = _get_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    error: Optional[str] = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        user = User.get_by_username(username)
        if not user or not user.check_password(password):
            error = _format_message(
                messages,
                'auth_login_invalid',
                'Ungültige Kombination aus Benutzername und Passwort.',
            )
        else:
            login_user(user)
            next_url = request.args.get('next')
            if not is_safe_redirect(next_url):
                next_url = url_for('auth.admin')
            flash(
                _format_message(messages, 'auth_login_success', 'Erfolgreich angemeldet.'),
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
    return _maybe_set_language_cookie(response, locale)


@auth_bp.route('/register', methods=('GET', 'POST'))
def register():
    if current_user.is_authenticated:
        return redirect(url_for('auth.admin'))

    locale = _get_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    error: Optional[str] = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        confirm = request.form.get('confirm_password') or ''
        if not username:
            error = _format_message(
                messages,
                'auth_register_username_required',
                'Benutzername darf nicht leer sein.',
            )
        elif not password:
            error = _format_message(
                messages,
                'auth_register_password_required',
                'Passwort darf nicht leer sein.',
            )
        elif password != confirm:
            error = _format_message(
                messages,
                'auth_register_password_mismatch',
                'Passwörter stimmen nicht überein.',
            )
        elif User.get_by_username(username):
            error = _format_message(
                messages,
                'auth_register_username_taken',
                'Benutzername ist bereits vergeben.',
            )
        else:
            conn = get_db()
            now = dt.datetime.utcnow().isoformat(timespec='seconds')
            with conn:
                conn.execute(
                    'INSERT INTO users (username, password_hash, is_admin, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
                    (username, generate_password_hash(password), 0, now, now),
                )
            user = User.get_by_username(username)
            if user:
                login_user(user)
                flash(
                    _format_message(messages, 'auth_register_success', 'Konto erstellt.'),
                    'success',
                )
                return redirect(url_for('auth.admin'))
            error = _format_message(
                messages,
                'auth_register_failure',
                'Registrierung fehlgeschlagen.',
            )

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
    return _maybe_set_language_cookie(response, locale)


@auth_bp.post('/logout')
@login_required
def logout():
    logout_user()
    messages = _get_locale_bundle()['messages']
    flash(_format_message(messages, 'auth_logout_success', 'Sie wurden abgemeldet.'), 'info')
    return redirect(url_for('auth.login'))


@auth_bp.get('/settings')
@login_required
def admin():
    locale = _get_locale_bundle()
    ui_strings = locale['ui']
    js_strings = locale['js']
    messages = locale['messages']

    is_admin = _is_current_user_admin()
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
        api_keys = get_api_keys_for_user(int(current_user.id))
        api_key_now = dt.datetime.utcnow().isoformat(timespec='seconds')

    new_api_key = session.pop('new_api_key_value', None)
    response = make_response(
        render_template(
            'settings.html',
            active_section=requested,
            available_sections=sections,
            is_admin=is_admin,
            api_keys=api_keys,
            api_key_now=api_key_now,
            new_api_key=new_api_key if not is_admin else None,
            ui=ui_strings,
            js_strings=js_strings,
            i18n_messages=messages,
            lang=locale['lang'],
            current_language=locale['lang'],
        )
    )
    return _maybe_set_language_cookie(response, locale)


@auth_bp.post('/admin/import/start')
@login_required
def admin_import_start():
    if not _is_current_user_admin():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    payload = request.get_json(silent=True) or {}
    kind = (payload.get('kind') or '').strip().lower()
    if kind not in {'stations', 'weather'}:
        return jsonify({'ok': False, 'error': 'invalid_kind'}), 400

    app_obj = current_app._get_current_object()
    if kind == 'stations':
        job = start_job('stations', import_station_metadata, args=(app_obj,))
    else:
        job = start_job('weather', import_full_history, args=(app_obj,))
    return jsonify({'ok': True, 'job_id': job.job_id})


@auth_bp.get('/admin/import/<job_id>')
@login_required
def admin_import_status(job_id: str):
    if not _is_current_user_admin():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    job = get_job(job_id)
    if job is None:
        return jsonify({'ok': False, 'error': 'job_not_found'}), 404
    return jsonify({'ok': True, 'job': job.to_dict()})


@auth_bp.post('/admin/change-password')
@login_required
def change_password():
    messages = _get_locale_bundle()['messages']
    current_pw = request.form.get('current_password') or ''
    new_pw = request.form.get('new_password') or ''
    confirm_pw = request.form.get('confirm_password') or ''

    if not current_user.check_password(current_pw):
        flash(
            _format_message(messages, 'auth_password_current_invalid', 'Aktuelles Passwort ist falsch.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    if not new_pw:
        flash(
            _format_message(messages, 'auth_password_new_required', 'Neues Passwort darf nicht leer sein.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))
    if new_pw != confirm_pw:
        flash(
            _format_message(
                messages,
                'auth_password_mismatch',
                'Neues Passwort und Bestätigung stimmen nicht überein.',
            ),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))

    current_user.update_password(new_pw)
    flash(
        _format_message(messages, 'auth_password_updated', 'Passwort wurde aktualisiert.'),
        'success',
    )
    return redirect(url_for('auth.admin', section='password'))


@auth_bp.post('/settings/api-keys/create')
@login_required
def create_api_key():
    messages = _get_locale_bundle()['messages']
    if _is_current_user_admin():
        flash(
            _format_message(messages, 'auth_api_keys_user_only', 'API-Keys stehen nur Benutzerkonten zur Verfügung.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))
    name = (request.form.get('name') or '').strip() or 'API-Key'
    try:
        days = int(request.form.get('expires_in') or 90)
    except (TypeError, ValueError):
        days = 90
    days = max(1, min(360, days))
    now = dt.datetime.utcnow()
    expires_at = (now + dt.timedelta(days=days)).isoformat(timespec='seconds')
    key_value = secrets.token_urlsafe(32)
    conn = get_db()
    with conn:
        conn.execute(
            '''
            INSERT INTO api_keys (user_id, name, api_key, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (int(current_user.id), name, key_value, now.isoformat(timespec='seconds'), expires_at),
        )
    session['new_api_key_value'] = key_value
    flash(_format_message(messages, 'auth_api_key_created', 'API-Key erstellt.'), 'success')
    return redirect(url_for('auth.admin', section='api_keys'))


@auth_bp.post('/settings/api-keys/<int:key_id>/delete')
@login_required
def delete_api_key(key_id: int):
    messages = _get_locale_bundle()['messages']
    if _is_current_user_admin():
        flash(
            _format_message(messages, 'auth_api_keys_user_only', 'API-Keys stehen nur Benutzerkonten zur Verfügung.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='password'))
    conn = get_db()
    with conn:
        cur = conn.execute(
            'DELETE FROM api_keys WHERE id = ? AND user_id = ?',
            (key_id, int(current_user.id)),
        )
    if cur.rowcount:
        flash(_format_message(messages, 'auth_api_key_deleted', 'API-Key gelöscht.'), 'success')
    else:
        flash(
            _format_message(messages, 'auth_api_key_delete_failed', 'API-Key konnte nicht gelöscht werden.'),
            'error',
        )
    return redirect(url_for('auth.admin', section='api_keys'))


@auth_bp.post('/admin/sync-stations')
@login_required
def sync_stations_admin():
    messages = _get_locale_bundle()['messages']
    if not current_user.is_admin:
        flash(_format_message(messages, 'auth_permission_denied', 'Keine Berechtigung.'), 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        stats = import_station_metadata(current_app)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Station sync failed in admin view: %s', exc)
        flash(
            _format_message(messages, 'auth_station_sync_failed', 'Stationsdaten konnten nicht aktualisiert werden.'),
            'error',
        )
        return redirect(url_for('auth.admin', section='data'))

    inserted = stats.get('inserted', 0)
    updated = stats.get('updated', 0)
    total = inserted + updated
    flash(
        _format_message(
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
    messages = _get_locale_bundle()['messages']
    if not current_user.is_admin:
        flash(_format_message(messages, 'auth_permission_denied', 'Keine Berechtigung.'), 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        report = import_full_history(current_app)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Weather sync failed in admin view: %s', exc)
        flash(
            _format_message(messages, 'auth_weather_sync_failed', 'Wetterdaten konnten nicht aktualisiert werden.'),
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

    summary_message = _format_message(
        messages,
        'auth_weather_sync_summary',
        (
            'Wetterdaten vollständig aktualisiert. '
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
                _format_message(messages, 'auth_weather_sync_error', 'Fehler: {error}', error=error),
                'error',
            )
    else:
        flash(summary_message, 'success')
    return redirect(url_for('auth.admin', section='data'))
