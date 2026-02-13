import pytest
import sys
import psycopg
from pathlib import Path
from datetime import date

# Ensure `src/` imports resolve the same way as in the application runtime.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import update_data


ALL_SQL_FIELDS = (
    "p_id",
    "program",
    "comments",
    "date_added",
    "url",
    "status",
    "term",
    "us_or_international",
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "degree",
    "llm_generated_program",
    "llm_generated_university",
)

OPTIONAL_FIELDS = {
    "comments",
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "llm_generated_program",
    "llm_generated_university",
}


@pytest.fixture()
def use_real_postgres_for_update_data(monkeypatch, postgres_connect_kwargs):
    """Force update_data DB calls to use the env-driven PostgreSQL instance."""
    original_connect = update_data.psycopg.connect

    def fake_connect(**_kwargs):
        return original_connect(**postgres_connect_kwargs)

    monkeypatch.setattr(update_data.psycopg, "connect", fake_connect)


@pytest.fixture()
def sample_entry():
    """Canonical cleaned entry used across real DB insertion tests."""
    return {
        "program": "Computer Science, Johns Hopkins University",
        "comments": "Real DB row",
        "date_added": "January 25, 2026",
        "url": "https://www.thegradcafe.com/result/7001",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.95",
        "GRE Score": "332",
        "GRE V Score": "165",
        "GRE AW": "4.5",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "Johns Hopkins University",
    }


@pytest.mark.db
def test_insert_applicants_from_json_batch_inserts_required_fields_real_postgres(
    use_real_postgres_for_update_data,
    postgres_connect_kwargs,
    reset_real_applicants_table,
    sample_entry,
):
    """Insert rows into real PostgreSQL and verify required non-null fields."""
    result = update_data.insert_applicants_from_json_batch([sample_entry])
    assert result == 0

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM applicants WHERE p_id = 7001;")
            row = cur.fetchone()

    assert row is not None
    row_by_field = dict(zip(ALL_SQL_FIELDS, row))
    required_non_null_fields = [f for f in ALL_SQL_FIELDS if f not in OPTIONAL_FIELDS]

    for field in required_non_null_fields:
        assert row_by_field[field] is not None

    # Validate key type conversions performed by insert_applicants...
    assert row_by_field["p_id"] == 7001
    assert row_by_field["date_added"] == date(2026, 1, 25)
    assert row_by_field["gpa"] == 3.95
    assert row_by_field["gre"] == 332.0
    assert row_by_field["gre_v"] == 165.0
    assert row_by_field["gre_aw"] == 4.5


@pytest.mark.db
def test_insert_applicants_from_json_batch_ignores_duplicates_real_postgres(
    use_real_postgres_for_update_data,
    postgres_connect_kwargs,
    reset_real_applicants_table,
):
    """Duplicate URLs (same p_id) should result in one stored DB row."""
    duplicate_entries = [
        {
            "program": "Computer Science, MIT",
            "comments": "First copy",
            "date_added": "January 26, 2026",
            "url": "https://www.thegradcafe.com/result/7002",
            "status": "Accepted",
            "term": "Fall 2026",
            "US/International": "International",
            "GPA": "3.80",
            "GRE Score": "328",
            "GRE V Score": "161",
            "GRE AW": "4.0",
            "Degree": "PhD",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "MIT",
        },
        {
            "program": "Computer Science, MIT",
            "comments": "Second copy",
            "date_added": "January 26, 2026",
            "url": "https://www.thegradcafe.com/result/7002",
            "status": "Accepted",
            "term": "Fall 2026",
            "US/International": "International",
            "GPA": "3.80",
            "GRE Score": "328",
            "GRE V Score": "161",
            "GRE AW": "4.0",
            "Degree": "PhD",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "MIT",
        },
    ]

    result = update_data.insert_applicants_from_json_batch(duplicate_entries)
    assert result == 1

    with psycopg.connect(**postgres_connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants WHERE p_id = 7002;")
            count = cur.fetchone()[0]

    assert count == 1


@pytest.mark.db
def test_can_query_inserted_data_and_return_dict_with_required_keys_real_postgres(
    use_real_postgres_for_update_data,
    postgres_connect_kwargs,
    reset_real_applicants_table,
    sample_entry,
):
    """Verify querying real DB row and projecting to dict with full schema keys."""
    result = update_data.insert_applicants_from_json_batch([sample_entry])
    assert result == 0

    def query_applicant_as_dict(p_id):
        with psycopg.connect(**postgres_connect_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM applicants WHERE p_id = %s", (p_id,))
                row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(ALL_SQL_FIELDS, row))

    row_dict = query_applicant_as_dict(7001)
    assert row_dict is not None

    for field in ALL_SQL_FIELDS:
        assert field in row_dict

    # Also validate the helper's None-return path when no row exists.
    assert query_applicant_as_dict(99999999) is None
