"""Application factory and common setup for Weather Analytics."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, Tuple

from flask import Flask, abort, make_response, render_template, request, url_for

from .api import api_bp
from .auth import auth_bp, init_auth
from .db import ensure_database, get_db, init_app as init_db_app
from .db.schema import ensure_weather_schema
from .reports import (
    ReportError,
    build_report_xlsx,
    generate_report,
    get_coverage as get_report_coverage,
    temp_durchschnitt_auswertung,
    temperature_samples,
)

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent
TRANSLATIONS_DIR = PROJECT_ROOT / 'translations'
TEMPLATES_DIR = PROJECT_ROOT / 'templates'
FALLBACK_LANGUAGE = 'de'
OPENAPI_SPEC_PATH = PROJECT_ROOT / 'openapi.json'


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


def format_iso_date_de(value: str | None) -> str:
    """Convert ISO date (YYYY-MM-DD) to German format (DD.MM.YYYY)."""
    if not value:
        return ''
    parts = value.split('-')
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        year, month, day = parts
        return f'{day.zfill(2)}.{month.zfill(2)}.{year}'
    return value


def format_period_label(period: str, granularity: str) -> str:
    """Return a localized label for aggregated report periods."""
    if not period:
        return ''
    if granularity == 'day':
        return format_iso_date_de(period)
    if granularity == 'month' and len(period) == 7 and period[4] == '-':
        year, month = period.split('-')
        return f'{month.zfill(2)}.{year}'
    return period


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
    app.config.setdefault('DATABASE_TIMEOUT', 30)
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

    # hier bereite ich mir schon mal alle Sprachoptionen fuer das UI vor, damit die Templates nur noch iterieren
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
        # dem Template liefere ich gleich das passende Flag mit, damit es nichts mehr nachschlagen muss
        current_option = next(
            (opt for opt in language_options if opt['code'] == resolved_lang),
            language_options[0],
        )
        return resolved_lang, ui_strings, js_strings, current_option

    @app.context_processor
    def inject_navigation_context():
        resolved_lang = resolve_language()
        current_option = next(
            (opt for opt in language_options if opt['code'] == resolved_lang),
            language_options[0],
        )
        return {
            'nav_languages': language_options,
            'nav_current_language': resolved_lang,
            'nav_current_language_option': current_option,
        }

    def _parse_report_params(args):
        # alle Parameter kommen als Strings rein, deshalb validiere ich sie einmal zentral an dieser Stelle
        try:
            lat = float(args.get('lat'))
            lon = float(args.get('lon'))
            radius = float(args.get('radius') or 10.0)
        except (TypeError, ValueError):
            raise ValueError('invalid_coordinates')

        start_date = args.get('start_date')
        end_date = args.get('end_date')
        if not start_date or not end_date:
            raise ValueError('missing_dates')

        granularity = (args.get('granularity') or 'day').lower()
        if granularity not in {'day', 'month', 'year'}:
            raise ValueError('invalid_granularity')

        return lat, lon, radius, start_date, end_date, granularity

    TEMPERATURE_SAMPLE_LIMIT = 500

    def _build_report_payload(conn, lat, lon, radius, start_date, end_date, granularity, js_strings):
        report = generate_report(conn, lat, lon, radius, start_date, end_date, granularity)
        report['period_count'] = len(report['periods'])
        report['station_count'] = len(report['stations'])

        params = report['params']
        params['start_date_raw'] = params.get('start_date')
        params['end_date_raw'] = params.get('end_date')
        # fuer die Ausgabe formatiere ich die Werte direkt hier um, damit spaeter niemand mehr am Datumsformat drehen muss
        params['start_date'] = format_iso_date_de(params.get('start_date'))
        params['end_date'] = format_iso_date_de(params.get('end_date'))

        for row in report['periods']:
            row['period'] = format_period_label(row.get('period_raw') or row.get('period'), report['granularity'])

        temperature_average = temp_durchschnitt_auswertung(conn, lat, lon, start_date, end_date, radius)
        temp_samples = temperature_samples(conn, lat, lon, start_date, end_date, radius, TEMPERATURE_SAMPLE_LIMIT)
        # die Samples dupliziere ich leicht, damit Tabelle und Download beide auf die gleiche Struktur zugreifen
        for sample in temp_samples:
            sample['date_raw'] = sample.get('date')
            sample['date'] = format_iso_date_de(sample.get('date'))

        # fuer das Chart baue ich das Array hier zusammen, weil das Template die Rohdaten nur noch durchreicht
        chart_data = {
            'labels': [row['period'] for row in report['periods']],
            'tempAvg': [row['temp_avg'] for row in report['periods']],
            'precipitation': [row['precipitation'] for row in report['periods']],
            'sunshine': [row['sunshine'] for row in report['periods']],
            'labelsTemp': js_strings.get('reportsChartTemperatureLabel', 'Avg temperature'),
            'labelsPrecip': js_strings.get('reportsChartPrecipLabel', 'Precipitation'),
            'labelsSunshine': js_strings.get('reportsChartSunshineLabel', 'Sunshine'),
        }

        return {
            'report': report,
            'temperature_average': temperature_average,
            'temp_samples': temp_samples,
            'chart_data': chart_data,
        }

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

    @app.get('/reports')
    def reports():
        """Render aggregated weather report."""
        resolved_lang, ui_strings, js_strings, current_option = _build_page_context()
        conn = get_db()
        coverage = get_report_coverage(conn)
        export_query_string = request.query_string.decode('utf-8')

        report = None
        chart_data = None
        temperature_average = None
        temp_samples = []
        error_message = ui_strings.get('report_table_placeholder', 'Bitte starte die Auswertung auf der Hauptseite.')

        required = {'lat', 'lon', 'radius', 'start_date', 'end_date', 'granularity'}
        # nur wenn wirklich alle Parameter gesetzt sind, loese ich den recht teuren Report-Lauf aus
        if required.issubset(request.args.keys()):
            try:
                lat, lon, radius, start_date, end_date, granularity = _parse_report_params(request.args)
                payload = _build_report_payload(conn, lat, lon, radius, start_date, end_date, granularity, js_strings)
                report = payload['report']
                temperature_average = payload['temperature_average']
                temp_samples = payload['temp_samples']
                chart_data = payload['chart_data']
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
                export_query_string=export_query_string,
            )
        )
        if request.args.get('lang', type=str) in SUPPORTED_LANGUAGES:
            response.set_cookie('lang', resolved_lang, max_age=60 * 60 * 24 * 365, samesite='Lax')
        return response

    @app.get('/reports/export/<fmt>')
    def export_report(fmt: str):
        """Download the current report as XLSX."""
        if fmt != 'xlsx':
            # andere Formate biete ich hier aktuell nicht an, also direkt 404 statt halbgarem Fallback
            abort(404)

        required = {'lat', 'lon', 'radius', 'start_date', 'end_date', 'granularity'}
        if not required.issubset(request.args.keys()):
            return make_response(('Missing report parameters', 400))

        _, ui_strings, js_strings, _ = _build_page_context()
        conn = get_db()
        try:
            lat, lon, radius, start_date, end_date, granularity = _parse_report_params(request.args)
            payload = _build_report_payload(conn, lat, lon, radius, start_date, end_date, granularity, js_strings)
        except (ValueError, ReportError):
            return make_response(('Invalid report parameters', 400))

        report = payload['report']
        temp_samples = payload['temp_samples']
        timestamp = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        base_filename = f'weather_report_{timestamp}'

        content = build_report_xlsx(report, temp_samples, ui_strings)
        response = make_response(content)
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        response.headers['Content-Disposition'] = f'attachment; filename={base_filename}.xlsx'
        return response

    @app.get('/openapi.json')
    def openapi_spec():
        if not OPENAPI_SPEC_PATH.exists():
            abort(404)
        with OPENAPI_SPEC_PATH.open('r', encoding='utf-8') as handle:
            spec_data = json.load(handle)
        return app.response_class(json.dumps(spec_data), mimetype='application/json')

    @app.get('/docs')
    def swagger_docs():
        return render_template('swagger.html', spec_url=url_for('openapi_spec'))

    return app
