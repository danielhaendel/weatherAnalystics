# Flask entrypoint with template rendering and API blueprint registration.

from __future__ import annotations

import json
import logging
from pathlib import Path

from flask import Flask, render_template, request, make_response

from api.routes import api_bp
from db import init_app as init_db_app, ensure_database
from dwd_sync import sync_dwd_data


BASE_DIR = Path(__file__).resolve().parent
TRANSLATIONS_DIR = BASE_DIR / 'translations'
FALLBACK_LANGUAGE = 'de'


def load_translations(directory: Path, preferred_default: str):
    translations = {}
    if directory.is_dir():
        for path in sorted(directory.glob('*.json')):
            with path.open('r', encoding='utf-8') as handle:
                translations[path.stem] = json.load(handle)
    if not translations:
        raise RuntimeError(f'No translation files found in {directory}')

    default_lang = preferred_default if preferred_default in translations else next(iter(translations))
    ordered_codes = [default_lang] + [code for code in translations.keys() if code != default_lang]
    return translations, tuple(ordered_codes), default_lang


TRANSLATIONS, SUPPORTED_LANGUAGES, DEFAULT_LANGUAGE = load_translations(TRANSLATIONS_DIR, FALLBACK_LANGUAGE)


def resolve_language() -> str:
    """Determine the preferred language for the current request."""
    query_lang = request.args.get('lang', type=str)
    cookie_lang = request.cookies.get('lang')

    if query_lang in SUPPORTED_LANGUAGES:
        return query_lang

    if cookie_lang in SUPPORTED_LANGUAGES:
        return cookie_lang

    best = request.accept_languages.best_match(SUPPORTED_LANGUAGES)
    return best or DEFAULT_LANGUAGE


def configure_logging(app: Flask) -> None:
    """Configure application logger with consistent formatting."""
    log_level = app.config.get('LOG_LEVEL', 'INFO')
    app.logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    for handler in app.logger.handlers:
        handler.setFormatter(formatter)


def create_app() -> Flask:
    """Create and configure Flask application."""
    app = Flask(__name__)
    app.config['APP_TRANSLATIONS'] = TRANSLATIONS
    app.config['APP_SUPPORTED_LANGUAGES'] = SUPPORTED_LANGUAGES
    app.config['APP_DEFAULT_LANGUAGE'] = DEFAULT_LANGUAGE
    app.config.setdefault('DATABASE', str((BASE_DIR / 'instance' / 'weather.db')))
    app.config.setdefault('LOG_LEVEL', 'INFO')
    configure_logging(app)
    app.register_blueprint(api_bp, url_prefix='/api')
    init_db_app(app)
    ensure_database(app)
    sync_result = sync_dwd_data(app)
    app.logger.info('Initial station sync result: %s', sync_result)

    language_options = [
        {
            'code': code,
            'label': TRANSLATIONS[code]['label'],
            'flag': TRANSLATIONS[code].get('flag', ''),
        }
        for code in SUPPORTED_LANGUAGES
    ]

    @app.get('/')
    def index():
        """Render index page with localized content."""
        resolved_lang = resolve_language()
        translation = TRANSLATIONS[resolved_lang]
        ui_strings = translation['ui']
        js_strings = translation['js']
        current_option = next(
            (opt for opt in language_options if opt['code'] == resolved_lang),
            language_options[0],
        )

        response = make_response(
            render_template(
                'index.html',
                lang=resolved_lang,
                ui=ui_strings,
                js_strings=js_strings,
                languages=language_options,
                current_language=resolved_lang,
                current_language_option=current_option,
            )
        )

        if request.args.get('lang', type=str) in SUPPORTED_LANGUAGES:
            response.set_cookie(
                'lang',
                resolved_lang,
                max_age=60 * 60 * 24 * 365,
                samesite='Lax',
            )

        return response

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
