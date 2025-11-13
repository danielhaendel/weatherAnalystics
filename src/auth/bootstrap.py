"""Initialization helpers for the auth blueprint."""

from __future__ import annotations

from .blueprint import login_manager
from .services import locale
from .services.schema import initialize_auth_schema
from .services.users import load_user


def init_auth(app) -> None:
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'login_required'
    login_manager.localize_callback = locale.localize_login_message
    login_manager.user_loader(load_user)  # type: ignore[arg-type]

    with app.app_context():
        initialize_auth_schema()


__all__ = ['init_auth']
