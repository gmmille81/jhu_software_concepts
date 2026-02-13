import pytest
import sys
import psycopg
import re
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
    """POST /pull-data should succeed and add shared fake example rows."""

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

    response = client.post("/pull-data", headers={"Accept": "application/json"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert scrape_calls == [1, 2]
    assert inserted_payload["rows"] is not None
    assert len(inserted_payload["rows"]) == 2
    assert_integration_rows_are_well_formed(inserted_payload["rows"])
    assert pages.db_process.poll() is None


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

    response = client.post("/update_analysis", headers={"Accept": "application/json"})
    payload = response.get_json()
    render_response = client.get("/analysis")
    html = render_response.data.decode("utf-8")

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert render_response.status_code == 200
    assert "How many rows were updated?" in html
    assert "How many rows are accepted?" in html
    assert html.count("Answer:") >= 2


@pytest.mark.integration
def test_end_to_end_pull_update_render_with_real_db(
    client,
    monkeypatch,
    postgres_connect_kwargs,
    reset_real_applicants_table,
    seeded_answers_table,
    assert_analysis_has_answer_labels,
    assert_analysis_has_two_decimal_numeric_value,
):
    """End-to-end: dataset X and Y should produce different rendered analysis answers."""

    scrape_calls = []
    active_dataset = {"name": "X"}

    def fake_scrape_data(num_records):
        # Real-like scraper shape consumed by the cleaner.
        scrape_calls.append(num_records)
        if active_dataset["name"] == "X" and num_records == 1:
            return {
                0: [
                    "Johns Hopkins University\n",
                    "Computer Science\n\n\n\nMasters\n",
                    "\tJanuary 24, 2026\n",
                    "\tAccepted on Jan 24\n",
                    "",
                    "https://www.thegradcafe.com/result/1002",
                    "American Fall2026 GPA:3.90 GRE329 GREV162 GREAW4.5",
                    "Single latest row",
                ]
            }
        if active_dataset["name"] == "X":
            return {
                0: [
                    "Johns Hopkins University\n",
                    "Computer Science\n\n\n\nMasters\n",
                    "\tJanuary 24, 2026\n",
                    "\tAccepted on Jan 24\n",
                    "",
                    "https://www.thegradcafe.com/result/1002",
                    "American Fall2026 GPA:3.90 GRE329 GREV162 GREAW4.5",
                    "Row one comment",
                ],
                1: [
                    "MIT\n",
                    "Computer Science\n\n\n\nPhD\n",
                    "\tJanuary 23, 2026\n",
                    "\tRejected on Jan 23\n",
                    "",
                    "https://www.thegradcafe.com/result/1001",
                    "International Fall2026 GPA:3.70 GRE325 GREV160 GREAW4.0",
                    "Row two comment",
                ],
            }

        # Dataset Y (three Fall 2026 rows) to force changed query output.
        if num_records == 1:
            return {
                0: [
                    "Stanford University\n",
                    "Computer Science\n\n\n\nPhD\n",
                    "\tJanuary 27, 2026\n",
                    "\tAccepted on Jan 27\n",
                    "",
                    "https://www.thegradcafe.com/result/2003",
                    "International Fall2026 GPA:3.98 GRE334 GREV166 GREAW5.0",
                    "Latest Y row",
                ]
            }
        return {
            0: [
                "Stanford University\n",
                "Computer Science\n\n\n\nPhD\n",
                "\tJanuary 27, 2026\n",
                "\tAccepted on Jan 27\n",
                "",
                "https://www.thegradcafe.com/result/2003",
                "International Fall2026 GPA:3.98 GRE334 GREV166 GREAW5.0",
                "Y row one comment",
            ],
            1: [
                "Carnegie Mellon University\n",
                "Computer Science\n\n\n\nPhD\n",
                "\tJanuary 26, 2026\n",
                "\tAccepted on Jan 26\n",
                "",
                "https://www.thegradcafe.com/result/2002",
                "American Fall2026 GPA:3.85 GRE330 GREV163 GREAW4.5",
                "Y row two comment",
            ],
            2: [
                "MIT\n",
                "Computer Science\n\n\n\nMasters\n",
                "\tJanuary 25, 2026\n",
                "\tAccepted on Jan 25\n",
                "",
                "https://www.thegradcafe.com/result/2001",
                "International Fall2026 GPA:3.75 GRE326 GREV161 GREAW4.0",
                "Y row three comment",
            ],
        }

    def fake_clean_data(scraped):
        cleaned = []
        for raw in scraped.values():
            university = raw[0].strip()
            program_major = raw[1].split("\n\n\n\n")[0].replace("\n", "").strip()
            degree = raw[1].split("\n\n\n\n")[1].split("\n")[0].strip()
            status = raw[3].replace("\t", "").split("on")[0].strip()
            extra = raw[6]
            us_or_int = "American" if "American" in extra else "International"

            cleaned.append(
                {
                    "program": f"{program_major}, {university}",
                    "comments": raw[7],
                    "date_added": raw[2].replace("\t", "").replace("\n", "").strip(),
                    "url": raw[5],
                    "status": status,
                    "term": "Fall 2026",
                    "US/International": us_or_int,
                    "GPA": "3.90" if "result/1002" in raw[5] else "3.70",
                    "GRE Score": "329" if "result/1002" in raw[5] else "325",
                    "GRE V Score": "162" if "result/1002" in raw[5] else "160",
                    "GRE AW": "4.5" if "result/1002" in raw[5] else "4.0",
                    "Degree": degree,
                    "llm-generated-program": "Computer Science",
                    "llm-generated-university": university,
                }
            )
        return cleaned

    def fake_get_newest_p():
        # Make refresh logic request exactly the rows for each dataset.
        return 1000 if active_dataset["name"] == "X" else 2000

    class _DoneProcess:
        # Mark subprocess as completed immediately so db_is_running() is false.
        def poll(self):
            return 0

    def fake_popen(*_args, **_kwargs):
        refresh_data.update_db()
        return _DoneProcess()

    monkeypatch.setattr(refresh_data, "scrape_data", fake_scrape_data)
    monkeypatch.setattr(refresh_data, "clean_data", fake_clean_data)
    monkeypatch.setattr(refresh_data, "get_newest_p", fake_get_newest_p)
    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None

    target_question = "How many entries do you have in your database who have applied for Fall 2026?"

    def extract_answer(html, question):
        pattern = re.compile(
            rf"<h3>\s*{re.escape(question)}\s*</h3>\s*<p class=\"answer\"><strong>Answer:\s*</strong>(.*?)</p>",
            re.DOTALL,
        )
        match = pattern.search(html)
        assert match is not None
        return match.group(1).strip()

    def run_full_cycle_for(dataset_name, expected_records):
        active_dataset["name"] = dataset_name
        scrape_calls.clear()
        pages.db_process = None
        pages.status_message = None
        pages.user_message = None

        with psycopg.connect(**postgres_connect_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE applicants;")
            conn.commit()

        pull_response = client.post("/pull-data", headers={"Accept": "application/json"})
        pull_payload = pull_response.get_json()
        assert pull_response.status_code == 200
        assert pull_payload["ok"] is True
        assert pull_payload["busy"] is False
        assert scrape_calls == [1, expected_records]

        with psycopg.connect(**postgres_connect_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM applicants;")
                applicants_count = cur.fetchone()[0]
        assert applicants_count == expected_records

        update_response = client.post("/update_analysis", headers={"Accept": "application/json"})
        update_payload = update_response.get_json()
        assert update_response.status_code == 200
        assert update_payload["ok"] is True
        assert update_payload["busy"] is False

        render_response = client.get("/analysis")
        assert render_response.status_code == 200
        html = render_response.data.decode("utf-8")
        assert "Answer:" in html
        return html

    # Run dataset X then dataset Y and prove displayed analysis answer changes.
    html_x = run_full_cycle_for("X", 2)
    answer_x = extract_answer(html_x, target_question)

    html_y = run_full_cycle_for("Y", 3)
    answer_y = extract_answer(html_y, target_question)

    assert answer_x == "2"
    assert answer_y == "3"
    assert answer_x != answer_y
    assert_analysis_has_answer_labels(html_y, expected_min_count=2)
    assert_analysis_has_two_decimal_numeric_value(html_y)


@pytest.mark.integration
def test_post_update_db_twice_with_overlapping_data_preserves_uniqueness(
    client,
    monkeypatch,
    postgres_connect_kwargs,
    reset_real_applicants_table,
    seeded_answers_table,
):
    """Two pull runs with overlap should not create duplicate applicant rows."""

    call_index = {"n": 0}

    def make_cleaned_row(p_id, status):
        return {
            "program": "Computer Science, Johns Hopkins University",
            "comments": f"row-{p_id}",
            "date_added": "January 25, 2026",
            "url": f"https://www.thegradcafe.com/result/{p_id}",
            "status": status,
            "term": "Fall 2026",
            "US/International": "American",
            "GPA": "3.90",
            "GRE Score": "329",
            "GRE V Score": "162",
            "GRE AW": "4.5",
            "Degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Johns Hopkins University",
        }

    def fake_scrape_data(_num_records):
        # Call order across two POSTs:
        # 1) probe X newest=3002
        # 2) pull X -> 3002,3001
        # 3) probe Y newest=3003
        # 4) pull Y -> 3003,3002,3001 (overlap with X)
        call_index["n"] += 1
        n = call_index["n"]
        if n == 1:
            return {0: make_cleaned_row(3002, "Accepted")}
        if n == 2:
            return {
                0: make_cleaned_row(3002, "Accepted"),
                1: make_cleaned_row(3001, "Rejected"),
            }
        if n == 3:
            return {0: make_cleaned_row(3003, "Accepted")}
        return {
            0: make_cleaned_row(3003, "Accepted"),
            1: make_cleaned_row(3002, "Accepted"),
            2: make_cleaned_row(3001, "Rejected"),
        }

    def fake_clean_data(scraped):
        # Fake scraper already returns cleaned-entry-like dicts.
        return list(scraped.values())

    def fake_get_newest_p():
        # Keep fixed baseline so second run requests an overlapping pull set.
        return 3000

    class _DoneProcess:
        def poll(self):
            return 0

    def fake_popen(*_args, **_kwargs):
        refresh_data.update_db()
        return _DoneProcess()

    monkeypatch.setattr(refresh_data, "scrape_data", fake_scrape_data)
    monkeypatch.setattr(refresh_data, "clean_data", fake_clean_data)
    monkeypatch.setattr(refresh_data, "get_newest_p", fake_get_newest_p)
    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None

    first_pull = client.post("/pull-data", headers={"Accept": "application/json"})
    first_payload = first_pull.get_json()
    assert first_pull.status_code == 200
    assert first_payload["ok"] is True
    assert first_payload["busy"] is False

    second_pull = client.post("/pull-data", headers={"Accept": "application/json"})
    second_payload = second_pull.get_json()
    assert second_pull.status_code == 200
    assert second_payload["ok"] is True
    assert second_payload["busy"] is False

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT p_id) FROM applicants;")
            total_count, distinct_count = cur.fetchone()
            cur.execute("SELECT p_id FROM applicants ORDER BY p_id;")
            p_ids = [row[0] for row in cur.fetchall()]

    # Uniqueness policy: overlapping pulls do not duplicate existing rows.
    assert total_count == 3
    assert distinct_count == 3
    assert p_ids == [3001, 3002, 3003]


@pytest.mark.integration
def test_pull_data_loader_error_returns_non_200_and_no_partial_writes(
    client,
    monkeypatch,
    postgres_connect_kwargs,
    reset_real_applicants_table,
):
    """If the pull loader errors, request should fail and DB should remain unchanged."""

    def failing_update_db():
        raise RuntimeError("simulated loader failure")

    def failing_popen(*_args, **_kwargs):
        # Match integration behavior where tests execute updater synchronously.
        failing_update_db()

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    monkeypatch.setattr(pages.subprocess, "Popen", failing_popen)
    client.application.config["PROPAGATE_EXCEPTIONS"] = False

    response = client.post("/pull-data", headers={"Accept": "application/json"})
    assert response.status_code != 200

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            row_count = cur.fetchone()[0]

    assert row_count == 0
