"""Language helper utilities for API routes."""

from __future__ import annotations

from flask import current_app


def get_translations():
    return current_app.config.get('APP_TRANSLATIONS', {}) or {}


def normalize_lang(lang_value: str | None) -> str:
    translations = get_translations()
    supported = tuple(current_app.config.get('APP_SUPPORTED_LANGUAGES') or ())
    default_lang = current_app.config.get('APP_DEFAULT_LANGUAGE') or next(iter(translations.keys()), 'de')
    lang = (lang_value or '').lower()
    if supported and lang in supported:
        return lang
    if lang in translations:
        return lang
    return default_lang


__all__ = ['get_translations', 'normalize_lang']
