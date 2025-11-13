"""Locale helpers for auth routes."""

from __future__ import annotations

from flask import current_app, request


def build_locale_bundle():
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


def maybe_set_language_cookie(response, locale_bundle):
    if not locale_bundle.get('set_cookie'):
        return response
    response.set_cookie(
        'lang',
        locale_bundle['lang'],
        max_age=60 * 60 * 24 * 365,
        samesite='Lax',
    )
    return response


def format_message(messages, key, fallback, **params):
    template = messages.get(key, fallback)
    try:
        return template.format(**params)
    except Exception:
        return template


def localize_login_message(message, **values):
    bundle = build_locale_bundle()
    messages = bundle.get('messages', {})
    if message == 'login_required':
        return format_message(messages, 'auth_login_required', 'Bitte melden Sie sich an, um fortzufahren.')
    template = messages.get(message, message)
    try:
        return template.format(**values)
    except Exception:
        return template


__all__ = ['build_locale_bundle', 'format_message', 'localize_login_message', 'maybe_set_language_cookie']
