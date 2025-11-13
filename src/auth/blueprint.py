"""Auth blueprint and login-manager instances."""

from flask import Blueprint
from flask_login import LoginManager

auth_bp = Blueprint('auth', __name__)
login_manager = LoginManager()

__all__ = ['auth_bp', 'login_manager']
