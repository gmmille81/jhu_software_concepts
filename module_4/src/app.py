# src/app.py
from flask import Flask

def create_app():
    """
    Factory function to create Flask app
    """
    app = Flask(__name__)

    # Import and register blueprint
    from pages import bp
    app.register_blueprint(bp)

    return app


def main():
    app = create_app()
    app.run(host="127.0.0.1", port=8080, debug=True, use_reloader=False)


if __name__ == "__main__":
    main()
