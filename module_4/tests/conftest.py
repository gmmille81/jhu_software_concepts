import pytest
from src.app import create_app
from bs4 import BeautifulSoup
@pytest.fixture()
def app():
    app = create_app()
    app.config['TESTING'] = True
    app.config['LIVESERVER_PORT'] = 8080
    app.config['LIVESERVER_TIMEOUT'] = 10
    yield app

@pytest.fixture()
def client(app):
    return app.test_client()

@pytest.fixture()
def runner(app):
    return app.test_cli_runner()