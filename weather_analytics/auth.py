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
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Bitte melden Sie sich an, um fortzufahren.'

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

    error: Optional[str] = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        user = User.get_by_username(username)
        if not user or not user.check_password(password):
            error = 'Ungültige Kombination aus Benutzername und Passwort.'
        else:
            login_user(user)
            next_url = request.args.get('next')
            if not is_safe_redirect(next_url):
                next_url = url_for('auth.admin')
            flash('Erfolgreich angemeldet.', 'success')
            return redirect(next_url)

    return render_template('login.html', error=error)


@auth_bp.route('/register', methods=('GET', 'POST'))
def register():
    if current_user.is_authenticated:
        return redirect(url_for('auth.admin'))

    error: Optional[str] = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        confirm = request.form.get('confirm_password') or ''
        if not username:
            error = 'Benutzername darf nicht leer sein.'
        elif not password:
            error = 'Passwort darf nicht leer sein.'
        elif password != confirm:
            error = 'Passwörter stimmen nicht überein.'
        elif User.get_by_username(username):
            error = 'Benutzername ist bereits vergeben.'
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
                flash('Konto erstellt.', 'success')
                return redirect(url_for('auth.admin'))
            error = 'Registrierung fehlgeschlagen.'

    return render_template('register.html', error=error)


@auth_bp.post('/logout')
@login_required
def logout():
    logout_user()
    flash('Sie wurden abgemeldet.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.get('/settings')
@login_required
def admin():
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
    return render_template(
        'settings.html',
        active_section=requested,
        available_sections=sections,
        is_admin=is_admin,
        api_keys=api_keys,
        api_key_now=api_key_now,
        new_api_key=new_api_key if not is_admin else None,
    )


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
    current_pw = request.form.get('current_password') or ''
    new_pw = request.form.get('new_password') or ''
    confirm_pw = request.form.get('confirm_password') or ''

    if not current_user.check_password(current_pw):
        flash('Aktuelles Passwort ist falsch.', 'error')
        return redirect(url_for('auth.admin', section='password'))

    if not new_pw:
        flash('Neues Passwort darf nicht leer sein.', 'error')
        return redirect(url_for('auth.admin', section='password'))
    if new_pw != confirm_pw:
        flash('Neues Passwort und Bestätigung stimmen nicht überein.', 'error')
        return redirect(url_for('auth.admin', section='password'))

    current_user.update_password(new_pw)
    flash('Passwort wurde aktualisiert.', 'success')
    return redirect(url_for('auth.admin', section='password'))


@auth_bp.post('/settings/api-keys/create')
@login_required
def create_api_key():
    if _is_current_user_admin():
        flash('API-Keys stehen nur Benutzerkonten zur Verfügung.', 'error')
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
    flash('API-Key erstellt.', 'success')
    return redirect(url_for('auth.admin', section='api_keys'))


@auth_bp.post('/settings/api-keys/<int:key_id>/delete')
@login_required
def delete_api_key(key_id: int):
    if _is_current_user_admin():
        flash('API-Keys stehen nur Benutzerkonten zur Verfügung.', 'error')
        return redirect(url_for('auth.admin', section='password'))
    conn = get_db()
    with conn:
        cur = conn.execute(
            'DELETE FROM api_keys WHERE id = ? AND user_id = ?',
            (key_id, int(current_user.id)),
        )
    if cur.rowcount:
        flash('API-Key gelöscht.', 'success')
    else:
        flash('API-Key konnte nicht gelöscht werden.', 'error')
    return redirect(url_for('auth.admin', section='api_keys'))


@auth_bp.post('/admin/sync-stations')
@login_required
def sync_stations_admin():
    if not current_user.is_admin:
        flash('Keine Berechtigung.', 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        stats = import_station_metadata(current_app)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Station sync failed in admin view: %s', exc)
        flash('Stationsdaten konnten nicht aktualisiert werden.', 'error')
        return redirect(url_for('auth.admin', section='data'))

    inserted = stats.get('inserted', 0)
    updated = stats.get('updated', 0)
    total = inserted + updated
    flash(
        f'Stationsdaten aktualisiert (neu: {inserted}, aktualisiert: {updated}, gesamt: {total}).',
        'success',
    )
    return redirect(url_for('auth.admin', section='data'))


@auth_bp.post('/admin/sync-weather')
@login_required
def sync_weather_admin():
    if not current_user.is_admin:
        flash('Keine Berechtigung.', 'error')
        return redirect(url_for('auth.admin', section='api_keys'))
    try:
        report = import_full_history(current_app)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Weather sync failed in admin view: %s', exc)
        flash('Wetterdaten konnten nicht aktualisiert werden.', 'error')
        return redirect(url_for('auth.admin', section='data'))

    stations = report.get('stations', {}) if isinstance(report, dict) else {}
    daily = report.get('daily', {}) if isinstance(report, dict) else {}

    inserted_stations = stations.get('inserted', 0)
    updated_stations = stations.get('updated', 0)
    inserted_daily = daily.get('inserted', 0)
    updated_daily = daily.get('updated', 0)
    archives_failed = daily.get('archives_failed', 0)
    archives_processed = daily.get('archives_processed', 0)

    summary_message = (
        'Wetterdaten vollständig aktualisiert. '
        f'Stationen neu: {inserted_stations}, aktualisiert: {updated_stations}. '
        f'Tageswerte neu: {inserted_daily}, aktualisiert: {updated_daily}. '
        f'ZIP-Dateien verarbeitet: {archives_processed}, fehlgeschlagen: {archives_failed}.'
    )

    if archives_failed:
        flash(summary_message, 'warning')
        errors = daily.get('errors') or []
        for error in errors[:3]:
            flash(f'Fehler: {error}', 'error')
    else:
        flash(summary_message, 'success')
    return redirect(url_for('auth.admin', section='data'))
