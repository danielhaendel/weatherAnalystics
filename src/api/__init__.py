"""API blueprint package."""

from .blueprint import api_bp
from . import routes as _routes  # noqa: F401  # ensure route modules are loaded

__all__ = ['api_bp']
