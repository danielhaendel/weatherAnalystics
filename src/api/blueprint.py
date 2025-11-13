"""Shared API blueprint instance."""

from flask import Blueprint

api_bp = Blueprint('api', __name__)

__all__ = ['api_bp']
