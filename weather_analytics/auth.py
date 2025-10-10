"""Authentication and admin views based on Flask-Login."""

from __future__ import annotations

import datetime as dt
from typing import Optional
from urllib.parse import urljoin, urlparse

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
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
from .dwd_sync import sync_dwd_data


auth_bp = Blueprint('auth', __name__)

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Bitte melden Sie sich an, um fortzufahren.'


USER_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
)


class User(UserMixin):
    """Lightweight user wrapper that works with Flask-Login."""

    def __init__(self, user_id: int, username: str, password_hash: str) -> None:
        self.id = str(user_id)
        self.username = username
        self.password_hash = password_hash

    @classmethod
    def from_row(cls, row) -> Optional['User']:
        if row is None:
            return None
        return cls(row['id'], row['username'], row['password_hash'])

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
                'INSERT INTO users (username, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?)',
                ('admin', generate_password_hash('admin'), now, now),
            )


def init_auth(app) -> None:
    login_manager.init_app(app)
    with app.app_context():
        execute_script(USER_SCHEMA)
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


@auth_bp.post('/logout')
@login_required
def logout():
    logout_user()
    flash('Sie wurden abgemeldet.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.get('/admin')
@login_required
def admin():
    section = request.args.get('section', 'data')
    if section not in {'data', 'password'}:
        section = 'data'
    return render_template('admin.html', active_section=section)


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


@auth_bp.post('/admin/sync-stations')
@login_required
def sync_stations_admin():
    try:
        result = sync_dwd_data(current_app, include_weather=False, raise_errors=True)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Station sync failed in admin view: %s', exc)
        flash('Stationsdaten konnten nicht aktualisiert werden.', 'error')
        return redirect(url_for('auth.admin', section='data'))

    stations = (result or {}).get('stations') or {}
    if stations.get('downloaded'):
        rows = stations.get('rows_processed', 0)
        flash(f'Stationsdaten aktualisiert ({rows} Einträge).', 'success')
    else:
        message = stations.get('message') or 'keine Aktion ausgeführt'
        flash(f'Stationsdaten: {message}.', 'info')
    return redirect(url_for('auth.admin', section='data'))


@auth_bp.post('/admin/sync-weather')
@login_required
def sync_weather_admin():
    try:
        result = sync_dwd_data(current_app, include_weather=True, raise_errors=True)
    except Exception as exc:  # pragma: no cover - defensive
        current_app.logger.exception('Weather sync failed in admin view: %s', exc)
        flash('Wetterdaten konnten nicht aktualisiert werden.', 'error')
        return redirect(url_for('auth.admin', section='data'))

    weather = (result or {}).get('weather') or {}
    processed = weather.get('processed')
    if processed:
        flash(f'Wetterdaten für {processed} Dateien aktualisiert.', 'success')
    else:
        flash('Es wurden keine neuen Wetterdaten verarbeitet.', 'info')
    return redirect(url_for('auth.admin', section='data'))
