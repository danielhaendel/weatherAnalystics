# Flask entrypoint with template rendering and API blueprint registration.

from flask import Flask, render_template
from api.routes import api_bp

def create_app() -> Flask:
    """Create and configure Flask application."""
    app = Flask(__name__)
    app.register_blueprint(api_bp, url_prefix='/api')

    @app.get('/')
    def index():
        """Render index page."""
        return render_template('index.html')

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
