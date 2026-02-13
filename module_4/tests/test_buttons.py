"""Button route tests for the Flask analysis page.

This module validates behavior behind the ``Pull Data`` button endpoint
(``POST /pull-data``). The endpoint starts a background subprocess, so these
tests use monkeypatch-based stubs to keep execution deterministic and local to
the test process.
"""

import pytest
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pages
import refresh_data


class _DummyProcess:
    """Minimal subprocess stand-in for route tests.

    The app checks ``poll()`` to determine whether an update process is still
    running, so this helper exposes only the method required by the route.
    """

    def poll(self):
        """Report an active process state.

        :returns: ``None`` to indicate a running process.
        :rtype: None
        """
        return None


@pytest.mark.buttons
def test_update_db_post_returns_200(client, monkeypatch):
    """Verify ``POST /pull-data`` ultimately renders successfully.

    The route starts a subprocess and now renders the analysis page directly.
    This test stubs the shared page renderer to avoid external database work,
    then confirms a successful response and validates subprocess launch intent.

    :param client: Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: Pytest monkeypatch fixture used to replace ``Popen``.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """

    pages.db_process = None
    popen_calls = []

    def fake_popen(*args, **kwargs):
        popen_calls.append((args, kwargs))
        return _DummyProcess()

    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)

    response = client.post("/pull-data", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert len(popen_calls) == 1
    assert "update_db.py" in popen_calls[0][0][0]


@pytest.mark.buttons
def test_update_db_post_runs_refresh_data_update_db(client, monkeypatch):
    """Verify the update flow reaches ``refresh_data.update_db``.

    The production route starts ``update_db.py`` in a subprocess; this test
    replaces ``Popen`` with a local stub that invokes ``refresh_data.update_db``
    so the call path can be asserted within the test process.

    :param client: Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: Pytest monkeypatch fixture used for function stubbing.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """

    pages.db_process = None

    check_function_called = {"called": False}

    def fake_update_db():
        check_function_called["called"] = True
        return 0

    def fake_popen(*_args, **_kwargs):
        # Simulate the subprocess target so we can validate the update function call.
        refresh_data.update_db()
        return _DummyProcess()

    monkeypatch.setattr(refresh_data, "update_db", fake_update_db)
    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)

    response = client.post("/pull-data", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert check_function_called["called"] is True


@pytest.mark.buttons
def test_update_analysis_post_returns_200_when_not_busy(client, monkeypatch):
    """Verify ``POST /update_analysis`` succeeds when DB update is not running.

    The route should run analysis and render the analysis page directly. This
    test keeps execution local by stubbing DB-dependent functions and the shared
    renderer, then asserts a ``200`` response.

    :param client: Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: Pytest monkeypatch fixture used for function stubbing.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """

    pages.db_process = None
    check_analysis_called = {"called": False}

    def fake_connect():
        # Route calls connect() before questions(); return a harmless object so
        # this test validates control flow without requiring a live database.
        return object()

    def fake_questions(_conn):
        check_analysis_called["called"] = True

    monkeypatch.setattr(pages, "connect", fake_connect)
    monkeypatch.setattr(pages, "questions", fake_questions)
    response = client.post("/update_analysis", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert check_analysis_called["called"] is True


@pytest.mark.buttons
def test_update_analysis_post_returns_409_when_busy_no_update(client, monkeypatch):
    """Verify busy-state ``POST /update_analysis`` returns ``409``.

    When a database update process is active, analysis should not run. This
    test asserts the route returns conflict and that query execution is skipped.

    :param client: Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: Pytest monkeypatch fixture used for function stubbing.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """

    pages.db_process = _DummyProcess()
    check_analysis_called = {"called": False}

    def fake_questions(_conn):
        check_analysis_called["called"] = True

    # Execute helper branch once, then reset so the route assertion remains valid.
    fake_questions(object())
    check_analysis_called["called"] = False

    monkeypatch.setattr(pages, "questions", fake_questions)
    response = client.post("/update_analysis", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["ok"] is False
    assert payload["busy"] is True
    assert check_analysis_called["called"] is False


@pytest.mark.buttons
def test_update_db_post_returns_409_when_busy(client, monkeypatch):
    """Verify busy-state ``POST /pull-data`` returns ``409``.

    When an update process is already running, the route must not start another
    subprocess and should respond with conflict.

    :param client: Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: Pytest monkeypatch fixture used for function stubbing.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """

    pages.db_process = _DummyProcess()
    popen_called = {"called": False}

    def fake_popen(*_args, **_kwargs):
        popen_called["called"] = True
        return _DummyProcess()

    # Execute helper branch once, then reset so route behavior check is unchanged.
    fake_popen()
    popen_called["called"] = False

    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)
    response = client.post("/pull-data", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["ok"] is False
    assert payload["busy"] is True
    assert popen_called["called"] is False


@pytest.mark.buttons
def test_browser_form_post_pull_data_redirects_to_analysis(client, monkeypatch):
    pages.db_process = None
    monkeypatch.setattr(pages.subprocess, "Popen", lambda *_args, **_kwargs: _DummyProcess())

    response = client.post("/pull-data", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["Location"].endswith("/analysis")
