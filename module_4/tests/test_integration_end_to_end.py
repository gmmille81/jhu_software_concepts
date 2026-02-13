import pytest
import sys
import re
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pages
import refresh_data


EXPECTED_KEYS = {
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


def _assert_rows_are_well_formed(rows):
    for row in rows:
        assert set(row.keys()) == EXPECTED_KEYS
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


def _install_fake_refresh_pipeline(monkeypatch):
    """Install shared fake scrape/clean/insert pipeline for integration tests."""
    scrape_calls = []
    inserted_payload = {"rows": None}

    def fake_scrape_data(num_records):
        # First call asks for newest record check, second call requests missing
        # rows. Return real-like module_2.scrape shape: dict[int] -> list.
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
        # Convert raw scraped rows to real-like module_2.clean output.
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
        # Simulate DB currently being behind by two records (1001, 1002).
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

    return scrape_calls, inserted_payload


@pytest.mark.integration
def test_update_db_with_fake_scraper_returns_multiple_records(monkeypatch):
    """Inject a fake scraper that yields multiple records for DB update."""

    scrape_calls, inserted_payload = _install_fake_refresh_pipeline(monkeypatch)

    result = refresh_data.update_db()

    assert result == 0
    assert scrape_calls == [1, 2]
    assert inserted_payload["rows"] is not None
    assert len(inserted_payload["rows"]) == 2
    _assert_rows_are_well_formed(inserted_payload["rows"])


@pytest.mark.integration
def test_post_update_db_succeeds_and_adds_rows(client, monkeypatch):
    """POST /update-db should succeed and add shared fake example rows."""

    class _DummyProcess:
        def poll(self):
            return None

    scrape_calls, inserted_payload = _install_fake_refresh_pipeline(monkeypatch)

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
    _assert_rows_are_well_formed(inserted_payload["rows"])
