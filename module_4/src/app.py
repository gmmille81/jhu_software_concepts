"""Flask application factory and executable entrypoint."""

from flask import Flask

def create_app():
    """
    Build and configure the Flask application instance.

    Returns
    -------
    flask.Flask
        App with the main blueprint registered.
    """
    app = Flask(__name__)

    # Import and register blueprint
    from pages import bp
    app.register_blueprint(bp)

    return app


def main():
    """Run the Flask development server for local execution."""
    app = create_app()
    app.run(host="127.0.0.1", port=8080, debug=True, use_reloader=False)


if __name__ == "__main__":
    main()
