import pytest
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pages
import refresh_data


@pytest.mark.integration
def test_update_db_with_fake_scraper_returns_multiple_records(
    fake_refresh_pipeline, assert_integration_rows_are_well_formed
):
    """Inject a fake scraper that yields multiple records for DB update."""

    scrape_calls = fake_refresh_pipeline["scrape_calls"]
    inserted_payload = fake_refresh_pipeline["inserted_payload"]

    result = refresh_data.update_db()

    assert result == 0
    assert scrape_calls == [1, 2]
    assert inserted_payload["rows"] is not None
    assert len(inserted_payload["rows"]) == 2
    assert_integration_rows_are_well_formed(inserted_payload["rows"])


@pytest.mark.integration
def test_post_update_db_succeeds_and_adds_rows(
    client, monkeypatch, fake_refresh_pipeline, assert_integration_rows_are_well_formed
):
    """POST /update-db should succeed and add shared fake example rows."""

    class _DummyProcess:
        def poll(self):
            return None

    scrape_calls = fake_refresh_pipeline["scrape_calls"]
    inserted_payload = fake_refresh_pipeline["inserted_payload"]

    def fake_popen(*_args, **_kwargs):
        # Run the updater synchronously so inserts happen during this request.
        refresh_data.update_db()
        return _DummyProcess()

    pages.db_process = None
    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(pages, "_render_analysis_page", lambda: "ok")

    response = client.post("/update-db")

    assert response.status_code == 200
    assert scrape_calls == [1, 2]
    assert inserted_payload["rows"] is not None
    assert len(inserted_payload["rows"]) == 2
    assert_integration_rows_are_well_formed(inserted_payload["rows"])


@pytest.mark.integration
def test_post_update_analysis_succeeds_and_updates_analysis_from_stubbed_db(
    client, monkeypatch, fake_refresh_pipeline
):
    """POST /update_analysis should succeed and render analysis from updated rows."""

    # Step 1: Build the "updated DB" state using the shared fake refresh flow.
    refresh_result = refresh_data.update_db()
    updated_rows = fake_refresh_pipeline["inserted_payload"]["rows"]

    assert refresh_result == 0
    assert updated_rows is not None
    assert len(updated_rows) == 2

    # Step 2: Stub the analysis/read path used by pages.update_analysis_route
    # and pages._render_analysis_page.
    answers_table_rows = []

    class _FakeCursor:
        def execute(self, _query):
            return None

        def fetchall(self):
            return answers_table_rows

        def close(self):
            return None

    class _FakeConnection:
        def cursor(self):
            return _FakeCursor()

        def close(self):
            return None

    def fake_connect():
        return _FakeConnection()

    def fake_questions(_conn):
        # Generate deterministic analysis from the updated in-memory rows.
        total_rows = len(updated_rows)
        accepted_rows = sum(1 for row in updated_rows if row["status"] == "Accepted")
        answers_table_rows[:] = [
            ("How many rows were updated?", str(total_rows)),
            ("How many rows are accepted?", str(accepted_rows)),
        ]

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    monkeypatch.setattr(pages, "connect", fake_connect)
    monkeypatch.setattr(pages, "questions", fake_questions)

    response = client.post("/update_analysis")
    html = response.data.decode("utf-8")

    assert response.status_code == 200
    assert "Analysis complete." in html
    assert "How many rows were updated?" in html
    assert "How many rows are accepted?" in html
    assert html.count("Answer:") >= 2


@pytest.mark.integration
def test_get_analysis_reflects_updated_values_after_post_update_analysis(
    client, monkeypatch, fake_refresh_pipeline
):
    """GET /analysis should show updated answers after POST /update_analysis."""

    refresh_result = refresh_data.update_db()
    updated_rows = fake_refresh_pipeline["inserted_payload"]["rows"]
    assert refresh_result == 0
    assert updated_rows is not None

    # Start with stale values to show they are replaced by update_analysis.
    answers_table_rows = [("Pipeline marker", "stale-value")]

    class _FakeCursor:
        def execute(self, _query):
            return None

        def fetchall(self):
            return answers_table_rows

        def close(self):
            return None

    class _FakeConnection:
        def cursor(self):
            return _FakeCursor()

        def close(self):
            return None

    def fake_connect():
        return _FakeConnection()

    def fake_questions(_conn):
        total_rows = len(updated_rows)
        accepted_rows = sum(1 for row in updated_rows if row["status"] == "Accepted")
        answers_table_rows[:] = [
            ("Pipeline marker", "fresh-value"),
            ("How many rows were updated?", str(total_rows)),
            ("How many rows are accepted?", str(accepted_rows)),
        ]

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    monkeypatch.setattr(pages, "connect", fake_connect)
    monkeypatch.setattr(pages, "questions", fake_questions)

    before_get = client.get("/analysis")
    before_html = before_get.data.decode("utf-8")
    assert before_get.status_code == 200
    assert "stale-value" in before_html

    post_update = client.post("/update_analysis")
    assert post_update.status_code == 200

    after_get = client.get("/analysis")
    after_html = after_get.data.decode("utf-8")
    assert after_get.status_code == 200
    assert "fresh-value" in after_html
    assert "How many rows were updated?" in after_html
    assert "How many rows are accepted?" in after_html
    assert "stale-value" not in after_html
