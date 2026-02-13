"""Shared pytest fixtures for app, DB, and integration helpers."""

import pytest
import re
import sys
import os
from pathlib import Path
import psycopg
from psycopg import OperationalError

MODULE4_DIR = Path(__file__).resolve().parents[1]
if str(MODULE4_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE4_DIR))

SRC_DIR = MODULE4_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.app import create_app

import refresh_data


@pytest.fixture()
def app():
    """Create a Flask app configured for test execution."""
    app = create_app()
    app.config['TESTING'] = True
    app.config['LIVESERVER_PORT'] = 8080
    app.config['LIVESERVER_TIMEOUT'] = 10
    yield app


@pytest.fixture()
def client(app):
    """Provide a Flask test client bound to the test app."""
    return app.test_client()


@pytest.fixture()
def runner(app):
    """Provide a Flask CLI runner for command-oriented tests."""
    return app.test_cli_runner()


@pytest.fixture()
def integration_expected_keys():
    """Canonical cleaned-row keys expected by insert_applicants..."""
    return {
        "program",
        "comments",
        "date_added",
        "url",
        "status",
        "term",
        "US/International",
        "GPA",
        "GRE Score",
        "GRE V Score",
        "GRE AW",
        "Degree",
        "llm-generated-program",
        "llm-generated-university",
    }


@pytest.fixture()
def assert_integration_rows_are_well_formed(integration_expected_keys):
    """Return a reusable row-schema/format assertion helper for integration tests."""

    def _assert_rows(rows):
        for row in rows:
            assert set(row.keys()) == integration_expected_keys
            assert ", " in row["program"]
            assert row["status"] in {"Accepted", "Rejected", "Waitlisted"}
            assert row["term"] == "Fall 2026"
            assert row["US/International"] in {"American", "International", "Other"}
            assert row["Degree"] in {"Masters", "PhD"}
            assert row["url"].startswith("https://www.thegradcafe.com/result/")
            assert re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4}", row["date_added"])
            assert re.fullmatch(r"\d\.\d{2}", row["GPA"])
            assert re.fullmatch(r"\d{3}", row["GRE Score"])
            assert re.fullmatch(r"\d{3}", row["GRE V Score"])
            assert re.fullmatch(r"\d\.\d", row["GRE AW"])

    return _assert_rows


@pytest.fixture()
def assert_analysis_has_answer_labels():
    """Return helper asserting rendered analysis includes Answer labels."""

    def _assert(html, expected_min_count):
        assert html.count("Answer:") >= expected_min_count

    return _assert


@pytest.fixture()
def assert_analysis_has_two_decimal_numeric_value():
    """Return helper asserting rendered analysis includes a 2-decimal value."""

    def _assert(html):
        assert re.search(r"\b\d+\.\d{2}%?\b", html)

    return _assert


@pytest.fixture()
def fake_refresh_pipeline(monkeypatch):
    """Install a realistic fake scrape->clean->insert pipeline for refresh_data.

    This fixture keeps integration tests deterministic by fully controlling
    external ETL dependencies while preserving the real data-shape contracts.
    """
    scrape_calls = []
    inserted_payload = {"rows": None}

    def fake_scrape_data(num_records):
        # Real-like module_2.scrape shape: dict[int] -> list
        scrape_calls.append(num_records)
        if num_records == 1:
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

    def fake_clean_data(scraped):
        # Real-like module_2.clean output: list[dict] with insert-ready keys.
        cleaned = []
        for raw in scraped.values():
            university = raw[0].strip()
            program_major = raw[1].split("\n\n\n\n")[0].replace("\n", "").strip()
            degree = raw[1].split("\n\n\n\n")[1].split("\n")[0].strip()
            status = raw[3].replace("\t", "").split("on")[0].strip()
            extra = raw[6]
            us_or_int = "American" if "American" in extra else "International"

            row = {
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
            cleaned.append(row)
        return cleaned

    def fake_get_newest_p():
        return 1000

    def fake_insert_applicants_from_json_batch(entries):
        inserted_payload["rows"] = entries
        return 0

    monkeypatch.setattr(refresh_data, "scrape_data", fake_scrape_data)
    monkeypatch.setattr(refresh_data, "clean_data", fake_clean_data)
    monkeypatch.setattr(refresh_data, "get_newest_p", fake_get_newest_p)
    monkeypatch.setattr(
        refresh_data,
        "insert_applicants_from_json_batch",
        fake_insert_applicants_from_json_batch,
    )

    return {"scrape_calls": scrape_calls, "inserted_payload": inserted_payload}


@pytest.fixture()
def postgres_connect_kwargs():
    """Connection settings for integration tests from DATABASE_URL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.fail(
            "Missing required PostgreSQL environment variable: DATABASE_URL"
        )

    return {"conninfo": database_url}


@pytest.fixture()
def real_postgres_ready(postgres_connect_kwargs):
    """Fail fast if a real PostgreSQL instance is not reachable."""
    try:
        with psycopg.connect(connect_timeout=2, **postgres_connect_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
    except OperationalError as exc:
        pytest.fail(f"Real PostgreSQL is required for this test run: {exc}")


@pytest.fixture()
def reset_real_applicants_table(real_postgres_ready, postgres_connect_kwargs):
    """Create/reset the applicants table schema for real DB integration tests."""
    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS applicants (
                    p_id BIGINT PRIMARY KEY,
                    program TEXT,
                    comments TEXT,
                    date_added DATE,
                    url TEXT,
                    status TEXT,
                    term TEXT,
                    us_or_international TEXT,
                    gpa FLOAT,
                    gre FLOAT,
                    gre_v FLOAT,
                    gre_aw FLOAT,
                    degree TEXT,
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
                """
            )
            cur.execute("TRUNCATE TABLE applicants;")
        conn.commit()

    yield

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE applicants;")
        conn.commit()


@pytest.fixture()
def seeded_answers_table(real_postgres_ready, postgres_connect_kwargs):
    """Ensure answers_table exists with at least one row for analysis page tests."""
    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS answers_table (
                    question TEXT,
                    answer TEXT
                );
                """
            )
            cur.execute("TRUNCATE TABLE answers_table;")
            cur.execute(
                """
                INSERT INTO answers_table (question, answer)
                VALUES (%s, %s)
                """,
                ("Seeded question", "Seeded answer"),
            )
        conn.commit()

    yield

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE answers_table;")
        conn.commit()
