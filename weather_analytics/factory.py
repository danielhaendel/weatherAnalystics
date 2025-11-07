"""Application factory and common setup for Weather Analytics."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Tuple

from flask import Flask, make_response, render_template, request

from .api import api_bp
from .auth import auth_bp, init_auth
from .db import ensure_database, get_db, init_app as init_db_app
from .report_service import (
    ReportError,
    generate_report,
    get_coverage as get_report_coverage,
    temp_durchschnitt_auswertung,
    temperature_samples,
)
from .schema import ensure_weather_schema

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent
TRANSLATIONS_DIR = PROJECT_ROOT / 'translations'
TEMPLATES_DIR = PROJECT_ROOT / 'templates'
FALLBACK_LANGUAGE = 'de'


def load_translations(directory: Path, preferred_default: str) -> Tuple[Dict[str, Dict], Tuple[str, ...], str]:
    """Load translation JSON files and return data plus ordering info."""
    translations: Dict[str, Dict] = {}
    if directory.is_dir():
        for path in sorted(directory.glob('*.json')):
            with path.open('r', encoding='utf-8') as handle:
                translations[path.stem] = json.load(handle)
    if not translations:
        raise RuntimeError(f'No translation files found in {directory}')

    default_lang = preferred_default if preferred_default in translations else next(iter(translations))
    ordered_codes = (default_lang,) + tuple(code for code in translations if code != default_lang)
    return translations, ordered_codes, default_lang


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
    app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(PROJECT_ROOT / 'static'))
    app.config['APP_TRANSLATIONS'] = TRANSLATIONS
    app.config['APP_SUPPORTED_LANGUAGES'] = SUPPORTED_LANGUAGES
    app.config['APP_DEFAULT_LANGUAGE'] = DEFAULT_LANGUAGE
    app.config.setdefault('DATABASE', str(PROJECT_ROOT / 'instance' / 'weather.db'))
    app.config.setdefault('LOG_LEVEL', 'INFO')
    app.config['SECRET_KEY'] = app.config.get('SECRET_KEY') or 'change-me'

    configure_logging(app)
    init_db_app(app)
    ensure_database(app)
    with app.app_context():
        ensure_weather_schema()
    init_auth(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(api_bp, url_prefix='/api')

    language_options = [
        {
            'code': code,
            'label': TRANSLATIONS[code]['label'],
            'flag': TRANSLATIONS[code].get('flag', ''),
        }
        for code in SUPPORTED_LANGUAGES
    ]

    def _build_page_context():
        resolved_lang = resolve_language()
        translation = TRANSLATIONS[resolved_lang]
        ui_strings = translation['ui']
        js_strings = translation['js']
        current_option = next(
            (opt for opt in language_options if opt['code'] == resolved_lang),
            language_options[0],
        )
        return resolved_lang, ui_strings, js_strings, current_option

    @app.get('/')
    def index():
        """Render index page with localized content."""
        resolved_lang, ui_strings, js_strings, current_option = _build_page_context()

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
            response.set_cookie('lang', resolved_lang, max_age=60 * 60 * 24 * 365, samesite='Lax')

        return response


    TEMPERATURE_SAMPLE_LIMIT = 500

    @app.get('/reports')
    def reports():
        """Render aggregated weather report."""
        resolved_lang, ui_strings, js_strings, current_option = _build_page_context()
        conn = get_db()
        coverage = get_report_coverage(conn)

        report = None
        chart_data = None
        temperature_average = None
        temp_samples = []
        error_message = ui_strings.get('report_table_placeholder', 'Bitte starte die Auswertung auf der Hauptseite.')

        required = {'lat', 'lon', 'radius', 'start_date', 'end_date', 'granularity'}
        if required.issubset(request.args.keys()):
            try:
                lat = float(request.args.get('lat'))
                lon = float(request.args.get('lon'))
                radius = float(request.args.get('radius') or 10.0)
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                granularity = (request.args.get('granularity') or 'day').lower()
                report = generate_report(conn, lat, lon, radius, start_date, end_date, granularity)
                report['period_count'] = len(report['periods'])
                report['station_count'] = len(report['stations'])
                temperature_average = temp_durchschnitt_auswertung(conn, lat, lon, start_date, end_date, radius)
                temp_samples = temperature_samples(conn, lat, lon, start_date, end_date, radius, TEMPERATURE_SAMPLE_LIMIT)
                chart_data = {
                    'labels': [row['period'] for row in report['periods']],
                    'tempAvg': [row['temp_avg'] for row in report['periods']],
                    'precipitation': [row['precipitation'] for row in report['periods']],
                    'sunshine': [row['sunshine'] for row in report['periods']],
                    'labelsTemp': js_strings.get('reportsChartTemperatureLabel', 'Avg temperature'),
                    'labelsPrecip': js_strings.get('reportsChartPrecipLabel', 'Precipitation'),
                    'labelsSunshine': js_strings.get('reportsChartSunshineLabel', 'Sunshine'),
                }
                error_message = ''
            except (ValueError, ReportError) as exc:
                if isinstance(exc, ReportError):
                    mapping = {
                        'out_of_bounds': js_strings.get('reportsCoverageError'),
                        'no_stations': js_strings.get('reportsNoStations'),
                        'no_data': js_strings.get('reportsNoData'),
                        'invalid_range': js_strings.get('reportsValidationDates'),
                        'invalid_granularity': js_strings.get('reportsValidationDates'),
                        'invalid_dates': js_strings.get('reportsValidationDates'),
                    }
                    error_message = mapping.get(exc.code, js_strings.get('reportsError'))
                else:
                    error_message = js_strings.get('reportsError')

        response = make_response(
            render_template(
                'reports.html',
                lang=resolved_lang,
                ui=ui_strings,
                js_strings=js_strings,
                languages=language_options,
                current_language=resolved_lang,
                current_language_option=current_option,
                coverage=coverage,
                report=report,
                chart_data=chart_data,
                error_message=error_message,
                temperature_average=temperature_average,
                temperature_samples=temp_samples,
                temperature_sample_limit=TEMPERATURE_SAMPLE_LIMIT,
            )
        )
        if request.args.get('lang', type=str) in SUPPORTED_LANGUAGES:
            response.set_cookie('lang', resolved_lang, max_age=60 * 60 * 24 * 365, samesite='Lax')
        return response

    return app
