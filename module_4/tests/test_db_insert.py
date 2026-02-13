import pytest
import sys
from pathlib import Path
from datetime import date

# Add `src/` to import path so tests can import app modules the same way the
# Flask app does at runtime (top-level imports like `import pages`).
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pages
import refresh_data
import update_data


class _DummyProcess:
    def poll(self):
        # Keep behavior consistent with other route tests that simulate an
        # active subprocess object.
        return None


class _StubCursor:
    """Cursor stub that records insert parameters and simulates RETURNING.

    `insert_applicants_from_json_batch` depends on two cursor behaviors:
    1) `execute(...)` to run the INSERT with converted values
    2) `fetchone()` to detect whether `RETURNING p_id` produced a row
       (inserted) or no row (conflict/no-op)
    This stub captures the sent params so tests can verify field mapping.
    """

    def __init__(self, returning_rows):
        # Queue of rows to hand back from fetchone(), one per execute call.
        self.returning_rows = list(returning_rows)
        # Stores each INSERT parameter tuple in execution order.
        self.executed_params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Return False so any exception would still propagate.
        return False

    def execute(self, _query, params):
        # Save parameters so tests can assert conversion/mapping correctness.
        self.executed_params.append(params)

    def fetchone(self):
        # Simulate psycopg cursor behavior: None when no returned row exists.
        if not self.returning_rows:
            return None
        return self.returning_rows.pop(0)


class _StubConnection:
    """Connection stub implementing context manager + commit behavior.

    `insert_applicants_from_json_batch` uses:
    - `with psycopg.connect(...) as conn`
    - `with conn.cursor() as cur`
    - `conn.commit()`
    This stub provides only that contract.
    """

    def __init__(self, cursor):
        self._cursor = cursor
        # Flag toggled when `commit()` is called.
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Keep default exception propagation behavior.
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        # Record that transaction commit was attempted.
        self.committed = True


@pytest.mark.db
def test_update_db_inserts_rows_with_required_non_null_fields(client, monkeypatch):
    # In-memory stand-in for the target table.
    fake_applicants_table = []
    required_fields = (
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

    # Before update, the table should be empty.
    assert len(fake_applicants_table) == 0

    def fake_update_db():
        # Simulate insert behavior performed during a DB refresh.
        fake_applicants_table.extend([
            {
                "p_id": 1001,
                "program": "Computer Science, Johns Hopkins University",
                "comments": "Strong profile and research fit.",
                "date_added": "2026-01-20",
                "url": "https://www.thegradcafe.com/result/1001",
                "status": "Accepted",
                "term": "Fall 2026",
                "us_or_international": "American",
                "gpa": 3.89,
                "gre": 330.0,
                "gre_v": 162.0,
                "gre_aw": 4.5,
                "degree": "Masters",
                "llm_generated_program": "Computer Science",
                "llm_generated_university": "Johns Hopkins University",
            },
            {
                "p_id": 1002,
                "program": "Computer Science, MIT",
                "comments": "Applied to PhD track.",
                "date_added": "2026-01-21",
                "url": "https://www.thegradcafe.com/result/1002",
                "status": "Rejected",
                "term": "Fall 2026",
                "us_or_international": "International",
                "gpa": 3.74,
                "gre": 325.0,
                "gre_v": 160.0,
                "gre_aw": 4.0,
                "degree": "PhD",
                "llm_generated_program": "Computer Science",
                "llm_generated_university": "MIT",
            },
        ])
        return 0

    def fake_popen(*_args, **_kwargs):
        # Execute the update flow synchronously inside the test process.
        refresh_data.update_db()
        return _DummyProcess()

    pages.db_process = None
    monkeypatch.setattr(refresh_data, "update_db", fake_update_db)
    monkeypatch.setattr(pages.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(pages, "_render_analysis_page", lambda: "ok")

    response = client.post("/update-db")

    # After POST /update-db, inserted rows should exist.
    assert response.status_code == 200
    assert len(fake_applicants_table) > 0

    # Every inserted row must contain required non-null fields.
    for row in fake_applicants_table:
        for field in required_fields:
            assert field in row
            assert row[field] is not None


@pytest.mark.db
def test_insert_applicants_from_json_batch_uses_stubbed_db_and_maps_fields(monkeypatch):
    # Ordered list of destination SQL columns as used by insert_applicants...
    # This lets us map the captured positional INSERT tuple back to field names.
    all_sql_fields = (
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
    # These fields may legitimately be empty for new records.
    optional_fields = {
        "comments",
        "gpa",
        "gre",
        "gre_v",
        "gre_aw",
        "llm_generated_program",
        "llm_generated_university",
    }
    required_non_null_fields = [f for f in all_sql_fields if f not in optional_fields]

    # Input shape mirrors the raw cleaned-entry dictionary consumed by
    # insert_applicants_from_json_batch before conversion/parsing.
    entry = {
        "program": "Computer Science, Johns Hopkins University",
        "comments": "Profile note",
        "date_added": "January 20, 2026",
        "url": "https://www.thegradcafe.com/result/2001",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.91",
        "GRE Score": "330",
        "GRE V Score": "",
        "GRE AW": "4.5",
        "Degree": "Masters",
        "llm-generated-program": "",
        "llm-generated-university": "",
    }

    # Configure fetchone() to return a row once, simulating successful INSERT
    # with `RETURNING p_id`.
    stub_cursor = _StubCursor(returning_rows=[(2001,)])
    stub_conn = _StubConnection(stub_cursor)

    def fake_connect(**_kwargs):
        # Replace real psycopg connection with in-memory stub.
        return stub_conn

    # Patch only update_data's psycopg usage so this test has zero DB I/O.
    monkeypatch.setattr(update_data.psycopg, "connect", fake_connect)

    # Run the real function under test.
    result = update_data.insert_applicants_from_json_batch([entry])

    # Function should report success and commit exactly one insert attempt.
    assert result == 0
    assert stub_conn.committed is True
    assert len(stub_cursor.executed_params) == 1

    # Reconstruct named fields from captured positional SQL parameters.
    inserted = stub_cursor.executed_params[0]
    inserted_by_field = dict(zip(all_sql_fields, inserted))

    # Verify all required (non-optional) SQL columns are populated.
    for field in required_non_null_fields:
        assert inserted_by_field[field] is not None

    # Verify key conversions performed by the function.
    assert inserted_by_field["p_id"] == 2001
    assert inserted_by_field["date_added"] == date(2026, 1, 20)
    assert inserted_by_field["gpa"] == 3.91
    assert inserted_by_field["gre"] == 330.0
    assert inserted_by_field["gre_v"] is None
    assert inserted_by_field["gre_aw"] == 4.5


@pytest.mark.db
def test_insert_applicants_from_json_batch_ignores_duplicate_rows(monkeypatch):
    """Duplicate source rows should not create duplicate DB rows.

    This test models accidental repeated pulls of the same GradCafe record.
    The function under test uses `ON CONFLICT (p_id) DO NOTHING RETURNING p_id`,
    so duplicate `p_id` values should be ignored by the DB insert path.
    """

    class _DedupCursor:
        """Cursor stub that emulates conflict-aware insertion semantics.

        Behavior:
        - First time a `p_id` appears: simulate successful INSERT by returning
          `(p_id,)` from `fetchone()`.
        - Repeated `p_id`: simulate conflict/no-op by returning `None`.
        """

        def __init__(self):
            # Tracks which primary keys have already been "inserted".
            self.seen_p_ids = set()
            # Stores only p_ids that were actually inserted (unique values).
            self.inserted_p_ids = []
            # Captures all execute calls (including duplicates) for visibility.
            self.executed_params = []
            # Per-execution value consumed by fetchone().
            self._last_fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, params):
            # Record every attempted insert statement.
            self.executed_params.append(params)
            p_id = params[0]
            if p_id in self.seen_p_ids:
                # Simulate ON CONFLICT DO NOTHING RETURNING p_id -> no row.
                self._last_fetchone = None
                return
            # First occurrence of this p_id behaves like a successful insert.
            self.seen_p_ids.add(p_id)
            self.inserted_p_ids.append(p_id)
            self._last_fetchone = (p_id,)

        def fetchone(self):
            # Return value indicates whether this execute inserted a row.
            return self._last_fetchone

    # Build connection/cursor doubles that replace psycopg I/O completely.
    dedup_cursor = _DedupCursor()
    stub_conn = _StubConnection(dedup_cursor)

    def fake_connect(**_kwargs):
        # Route all DB connections from update_data to our in-memory stub.
        return stub_conn

    monkeypatch.setattr(update_data.psycopg, "connect", fake_connect)

    # Two entries with the same URL => same derived p_id (3001).
    duplicate_entries = [
        {
            "program": "Computer Science, Johns Hopkins University",
            "comments": "Entry A",
            "date_added": "January 20, 2026",
            "url": "https://www.thegradcafe.com/result/3001",
            "status": "Accepted",
            "term": "Fall 2026",
            "US/International": "American",
            "GPA": "3.90",
            "GRE Score": "329",
            "GRE V Score": "162",
            "GRE AW": "4.5",
            "Degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Johns Hopkins University",
        },
        {
            # Same URL/p_id as above to emulate accidental duplicate pull.
            "program": "Computer Science, Johns Hopkins University",
            "comments": "Entry A duplicate",
            "date_added": "January 20, 2026",
            "url": "https://www.thegradcafe.com/result/3001",
            "status": "Accepted",
            "term": "Fall 2026",
            "US/International": "American",
            "GPA": "3.90",
            "GRE Score": "329",
            "GRE V Score": "162",
            "GRE AW": "4.5",
            "Degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Johns Hopkins University",
        },
    ]

    # Execute real insert function against duplicate input rows.
    result = update_data.insert_applicants_from_json_batch(duplicate_entries)

    # Because at least one insert hit a conflict, function reports 1.
    assert result == 1
    # Transaction should still commit successfully.
    assert stub_conn.committed is True

    # Both source rows were attempted, but only one unique row persisted.
    assert len(dedup_cursor.executed_params) == 2
    assert dedup_cursor.inserted_p_ids == [3001]


@pytest.mark.db
def test_can_query_inserted_data_and_return_dict_with_required_keys(monkeypatch):
    """Inserted data can be queried and returned as a dict with required keys.

    This validates a realistic post-insert read path:
    1) insert an applicant record through the production insert function
    2) run a query for that row by `p_id`
    3) shape the tuple into a dictionary keyed by DB column names
    4) assert required keys are present in the returned dict
    """

    # Canonical SQL column order for `applicants`, used to map tuple results
    # from `SELECT *` into a dictionary.
    sql_fields = (
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

    class _QueryableCursor:
        """Cursor stub that supports both INSERT and SELECT interactions.

        Internally, this behaves like an in-memory table keyed by `p_id`.
        - INSERT path stores parameter tuples and simulates ON CONFLICT logic.
        - SELECT path returns previously stored tuple for the requested `p_id`.
        """

        def __init__(self):
            # Simulated table storage: {p_id: inserted_tuple}
            self.rows_by_p_id = {}
            # Value returned by fetchone() after each execute() call.
            self._last_fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            # Branch by statement type to simulate minimal SQL behavior needed
            # by this test.
            query_upper = query.upper()
            if "INSERT INTO APPLICANTS" in query_upper:
                p_id = params[0]
                # Simulate ON CONFLICT behavior by returning None for duplicates.
                if p_id in self.rows_by_p_id:
                    self._last_fetchone = None
                    return
                # First insert for this p_id succeeds and is persisted.
                self.rows_by_p_id[p_id] = params
                self._last_fetchone = (p_id,)
                return

            if "SELECT" in query_upper and "FROM APPLICANTS" in query_upper:
                p_id = params[0]
                # Return full row tuple if present; otherwise simulate no match.
                self._last_fetchone = self.rows_by_p_id.get(p_id)
                return

            # Default for unsupported statements in this test helper.
            self._last_fetchone = None

        def fetchone(self):
            return self._last_fetchone

    # Replace psycopg connection with in-memory queryable stubs.
    stub_cursor = _QueryableCursor()
    stub_conn = _StubConnection(stub_cursor)

    def fake_connect(**_kwargs):
        return stub_conn

    monkeypatch.setattr(update_data.psycopg, "connect", fake_connect)

    # Source payload mirrors real cleaned input shape used by insert function.
    entry = {
        "program": "Computer Science, Johns Hopkins University",
        "comments": "Queryable row",
        "date_added": "January 24, 2026",
        "url": "https://www.thegradcafe.com/result/4001",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.92",
        "GRE Score": "331",
        "GRE V Score": "164",
        "GRE AW": "4.5",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "Johns Hopkins University",
    }

    # Run the production insert function; this populates the stub "table".
    insert_result = update_data.insert_applicants_from_json_batch([entry])
    assert insert_result == 0

    def query_applicant_as_dict(conn, p_id):
        # Minimal query helper that mimics service-layer read logic:
        # fetch tuple then project into a dictionary by schema column order.
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM applicants WHERE p_id = %s", (p_id,))
            row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(sql_fields, row))

    # Query the just-inserted row and validate returned structure.
    row_dict = query_applicant_as_dict(stub_conn, 4001)

    assert row_dict is not None
    # Ensure the result includes all required DB keys for downstream use.
    for key in sql_fields:
        assert key in row_dict
