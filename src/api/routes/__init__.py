"""Collection of API route modules."""

# Import route modules for side effects so that decorators register endpoints.
from . import geo, reports, stations  # noqa: F401

__all__ = ['geo', 'reports', 'stations']
