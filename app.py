"""WSGI entrypoint for Weather Analytics."""

from weather_analytics import create_app

app = create_app()


if __name__ == '__main__':
    app.run(debug=True)
