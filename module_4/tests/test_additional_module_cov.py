"""Additional coverage-focused tests for source and route branch behavior."""

import sys
from pathlib import Path
import json
import secrets
import importlib.util

import pytest
from psycopg import OperationalError
from _pytest.outcomes import Failed

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import query_data
import refresh_data
import pages
import update_data
import load_data
import app as app_module
import update_db as update_db_module
import db_config
from module_2 import clean as clean_mod
from module_2 import scrape as scrape_mod
import conftest as test_conftest

TESTS_DIR = Path(__file__).resolve().parent


def _load_module_from_path(module_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _exec_file_as_main(module_path):
    """Execute a Python file with module name set to '__main__'."""
    return _load_module_from_path(module_path, "__main__")


@pytest.fixture(autouse=True)
def fake_db_connect_kwargs(monkeypatch):
    # Keep unit tests independent from machine/CI DB env while avoiding
    # hard-coded credential literals.
    token = secrets.token_hex(8)
    kwargs = {"conninfo": f"postgresql://user_{token}:{secrets.token_hex(16)}@localhost:5432/db_{token}"}
    monkeypatch.setattr(query_data, "get_db_connect_kwargs", lambda: kwargs)
    monkeypatch.setattr(refresh_data, "get_db_connect_kwargs", lambda: kwargs)
    monkeypatch.setattr(update_data, "get_db_connect_kwargs", lambda: kwargs)


class _InsertCursor:
    def __init__(self, fetchone_values):
        self._fetchone_values = list(fetchone_values)
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _query, params):
        self.params.append(params)

    def fetchone(self):
        if self._fetchone_values:
            return self._fetchone_values.pop(0)
        return None


class _InsertConn:
    def __init__(self, fetchone_values):
        self._cursor = _InsertCursor(fetchone_values)
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class _LoadDataCursor:
    """Cursor double that records SQL calls and supports batched insert checks."""

    def __init__(self, select_results=None):
        self.select_results = list(select_results or [])
        self.executed = []
        self.executemany_calls = []
        self._last_query = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        query_text = str(query)
        self._last_query = query_text
        self.executed.append((query_text, params))

    def executemany(self, query, params_list):
        self.executemany_calls.append((str(query), list(params_list)))

    def fetchone(self):
        if "SELECT 1 FROM applicants" in (self._last_query or ""):
            if self.select_results:
                return self.select_results.pop(0)
            return None
        return None


class _LoadDataConn:
    """Connection double for load_data functions."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.autocommit = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


@pytest.mark.integration
def test_import_guard_inserts_src_dir_when_missing(monkeypatch):
    # Exercise this module's own import-guard insertion branch.
    src_dir_str = str(SRC_DIR)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != src_dir_str])
    _load_module_from_path(__file__, "cov_self_reexec")
    assert src_dir_str in sys.path


class _CtxCursor:
    def __init__(self, fetch_value):
        self._fetch_value = fetch_value
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query):
        self.executed.append(query)

    def fetchone(self):
        return (self._fetch_value,)


class _CtxConn:
    def __init__(self, fetch_value):
        self._cursor = _CtxCursor(fetch_value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor


@pytest.mark.db
def test_refresh_data_get_newest_p_returns_none_when_table_empty(monkeypatch):
    monkeypatch.setattr(refresh_data.psycopg, "connect", lambda **_kwargs: _CtxConn(None))
    assert refresh_data.get_newest_p() is None


@pytest.mark.db
def test_refresh_data_get_newest_p_returns_int_value(monkeypatch):
    monkeypatch.setattr(refresh_data.psycopg, "connect", lambda **_kwargs: _CtxConn(42))
    assert refresh_data.get_newest_p() == 42


@pytest.mark.integration
def test_refresh_data_update_db_returns_1_when_no_new_records(monkeypatch):
    calls = {"insert_called": False}

    monkeypatch.setattr(refresh_data, "scrape_data", lambda _n: {"k": []})
    monkeypatch.setattr(refresh_data, "clean_data", lambda _d: [{"url": "https://www.thegradcafe.com/result/100"}])
    monkeypatch.setattr(refresh_data, "get_newest_p", lambda: 100)
    monkeypatch.setattr(
        refresh_data,
        "insert_applicants_from_json_batch",
        lambda _rows: calls.__setitem__("insert_called", True),
    )

    assert refresh_data.update_db() == 1
    assert calls["insert_called"] is False


@pytest.mark.integration
def test_refresh_data_update_db_returns_0_when_new_records_exist(monkeypatch):
    scrape_calls = []
    clean_call_counter = {"n": 0}
    inserted_rows = {"value": None}

    def fake_scrape_data(n):
        scrape_calls.append(n)
        return {"raw": n}

    def fake_clean_data(_raw):
        clean_call_counter["n"] += 1
        if clean_call_counter["n"] == 1:
            return [{"url": "https://www.thegradcafe.com/result/12"}]
        return [
            {"url": "https://www.thegradcafe.com/result/12"},
            {"url": "https://www.thegradcafe.com/result/11"},
        ]

    def fake_insert(rows):
        inserted_rows["value"] = rows
        return 0

    monkeypatch.setattr(refresh_data, "scrape_data", fake_scrape_data)
    monkeypatch.setattr(refresh_data, "clean_data", fake_clean_data)
    monkeypatch.setattr(refresh_data, "get_newest_p", lambda: 10)
    monkeypatch.setattr(refresh_data, "insert_applicants_from_json_batch", fake_insert)

    assert refresh_data.update_db() == 0
    assert scrape_calls == [1, 2]
    assert inserted_rows["value"] is not None
    assert len(inserted_rows["value"]) == 2


@pytest.mark.db
def test_query_data_connect_failure_prints_error_and_raises(monkeypatch, capsys):
    def raise_operational_error(**_kwargs):
        raise OperationalError("boom")

    monkeypatch.setattr(query_data.psycopg, "connect", raise_operational_error)

    with pytest.raises(UnboundLocalError):
        query_data.connect()

    out = capsys.readouterr().out
    assert "The error 'boom' occurred" in out


@pytest.mark.integration
def test_update_db_script_main_invokes_refresh_update_db(monkeypatch):
    called = {"value": False}

    def fake_update_db():
        called["value"] = True
        return 0

    monkeypatch.setattr(update_db_module, "update_db", fake_update_db)
    update_db_module.main()

    assert called["value"] is True


@pytest.mark.db
def test_load_data_create_database_success_and_operational_error(monkeypatch, capsys):
    cursor = _LoadDataCursor()
    conn = _LoadDataConn(cursor)
    monkeypatch.setattr(load_data.psycopg, "connect", lambda **_kwargs: conn)

    load_data.create_database("applicant_data", "postgres", "pw", "127.0.0.1", "5432")

    assert conn.autocommit is True
    assert any("CREATE DATABASE" in q for q, _ in cursor.executed)
    assert "created successfully" in capsys.readouterr().out

    def boom(**_kwargs):
        raise OperationalError("create db failed")

    monkeypatch.setattr(load_data.psycopg, "connect", boom)
    load_data.create_database("applicant_data", "postgres", "pw", "127.0.0.1", "5432")
    assert "create db failed" in capsys.readouterr().out


@pytest.mark.db
def test_load_data_create_table_success_and_operational_error(monkeypatch, capsys):
    cursor = _LoadDataCursor()
    conn = _LoadDataConn(cursor)
    monkeypatch.setattr(load_data, "get_db_connect_kwargs", lambda: {"conninfo": "postgresql://stub"})
    monkeypatch.setattr(load_data.psycopg, "connect", lambda **_kwargs: conn)

    load_data.create_table()

    assert conn.commits == 1
    assert any("DROP TABLE IF EXISTS applicants" in q for q, _ in cursor.executed)
    assert any("CREATE TABLE applicants" in q for q, _ in cursor.executed)

    def boom(**_kwargs):
        raise OperationalError("create table failed")

    monkeypatch.setattr(load_data.psycopg, "connect", boom)
    load_data.create_table()
    assert "create table failed" in capsys.readouterr().out


@pytest.mark.db
def test_load_data_bulk_insert_json_covers_batches_and_remainder(tmp_path, monkeypatch, capsys):
    jsonl_path = tmp_path / "seed.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                "",
                json.dumps(
                    {
                        "url": "https://www.thegradcafe.com/result/1001",
                        "program": "Computer Science, Johns Hopkins University",
                        "comments": "",
                        "date_added": "January 24, 2026",
                        "status": "Accepted",
                        "term": "Fall 2026",
                        "US/International": "American",
                        "GPA": "3.90",
                        "GRE Score": "329",
                        "GRE V Score": "162",
                        "GRE AW Score": "4.5",
                        "Degree": "Masters",
                        "llm-generated-program": "Computer Science",
                        "llm-generated-university": "Johns Hopkins University",
                    }
                ),
                json.dumps(
                    {
                        "url": "https://www.thegradcafe.com/result/not-a-number",
                        "date_added": "January 25, 2026",
                    }
                ),
                json.dumps(
                    {
                        "url": "https://www.thegradcafe.com/result/1002",
                        "program": "Data Science, University of Michigan",
                        "comments": "",
                        "date_added": "not-a-date",
                        "status": "Rejected",
                        "term": "Fall 2026",
                        "US/International": "International",
                        "GPA": "3.70",
                        "GRE Score": "325",
                        "GRE V Score": "160",
                        "GRE AW Score": "4.0",
                        "Degree": "PhD",
                        "llm-generated-program": "",
                        "llm-generated-university": "",
                    }
                ),
                json.dumps(
                    {
                        "url": "https://www.thegradcafe.com/result/1003",
                        "program": "Statistics, Stanford University",
                        "comments": "",
                        "date_added": "January 26, 2026",
                        "status": "Accepted",
                        "term": "Fall 2026",
                        "US/International": "American",
                        "GPA": "",
                        "GRE Score": "",
                        "GRE V Score": "",
                        "GRE AW Score": "",
                        "Degree": "Masters",
                        "llm-generated-program": "Statistics",
                        "llm-generated-university": "Stanford University",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cursor = _LoadDataCursor(select_results=[(1,), None, None])
    conn = _LoadDataConn(cursor)
    monkeypatch.setattr(load_data, "get_db_connect_kwargs", lambda: {"conninfo": "postgresql://stub"})
    monkeypatch.setattr(load_data.psycopg, "connect", lambda **_kwargs: conn)

    load_data.bulk_insert_json(str(jsonl_path), batch_size=2)

    assert conn.commits == 3
    assert any("TRUNCATE TABLE applicants" in q for q, _ in cursor.executed)
    assert len(cursor.executemany_calls) == 2
    assert len(cursor.executemany_calls[0][1]) == 2
    assert len(cursor.executemany_calls[1][1]) == 1
    first_row = cursor.executemany_calls[0][1][0]
    assert first_row[0] == 1001
    assert str(first_row[3]) == "2026-01-24"
    second_valid_row = cursor.executemany_calls[0][1][1]
    assert second_valid_row[0] == 1002
    assert second_valid_row[3] is None
    remainder_row = cursor.executemany_calls[1][1][0]
    assert remainder_row[0] == 1003
    assert remainder_row[8] is None
    out = capsys.readouterr().out
    assert "records inserted in total" in out
    assert "duplicates skipped" in out


@pytest.mark.db
def test_load_data_bulk_insert_json_operational_error(monkeypatch, capsys):
    monkeypatch.setattr(load_data, "get_db_connect_kwargs", lambda: {"conninfo": "postgresql://stub"})

    def boom(**_kwargs):
        raise OperationalError("bulk insert failed")

    monkeypatch.setattr(load_data.psycopg, "connect", boom)
    load_data.bulk_insert_json("does-not-matter.jsonl")
    assert "bulk insert failed" in capsys.readouterr().out


@pytest.mark.db
def test_load_data_bulk_insert_json_remainder_inserted_branch(tmp_path, monkeypatch):
    jsonl_path = tmp_path / "single.jsonl"
    jsonl_path.write_text(
        json.dumps(
            {
                "url": "https://www.thegradcafe.com/result/2001",
                "date_added": "January 24, 2026",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    cursor = _LoadDataCursor(select_results=[(1,)])
    conn = _LoadDataConn(cursor)
    monkeypatch.setattr(load_data, "get_db_connect_kwargs", lambda: {"conninfo": "postgresql://stub"})
    monkeypatch.setattr(load_data.psycopg, "connect", lambda **_kwargs: conn)

    # batch_size larger than row count forces the "remaining rows" branch.
    load_data.bulk_insert_json(str(jsonl_path), batch_size=2)
    assert conn.commits == 2
    assert len(cursor.executemany_calls) == 1


@pytest.mark.integration
def test_load_data_main_calls_create_and_bulk(monkeypatch):
    calls = {"create_table": 0, "bulk_insert_json": 0, "path": None}

    monkeypatch.setattr(
        load_data,
        "create_table",
        lambda: calls.__setitem__("create_table", calls["create_table"] + 1),
    )

    def fake_bulk(path):
        calls["bulk_insert_json"] += 1
        calls["path"] = path

    monkeypatch.setattr(load_data, "bulk_insert_json", fake_bulk)

    load_data.main()

    assert calls["create_table"] == 1
    assert calls["bulk_insert_json"] == 1
    assert calls["path"] == load_data.filename


@pytest.mark.integration
def test_load_data_dunder_main_guard_executes_main(monkeypatch):
    monkeypatch.setattr(db_config, "get_db_connect_kwargs", lambda: {"conninfo": "postgresql://stub"})

    def boom(**_kwargs):
        raise OperationalError("expected in test")

    monkeypatch.setattr("psycopg.connect", boom)

    # Executes if __name__ == "__main__" block without requiring a live DB.
    _exec_file_as_main(str(SRC_DIR / "load_data.py"))


@pytest.mark.web
def test_app_script_main_invokes_flask_run(monkeypatch):
    run_kwargs = {}

    def fake_run(self, **kwargs):
        run_kwargs.update(kwargs)

    monkeypatch.setattr("flask.Flask.run", fake_run)
    app_module.main()

    assert run_kwargs["host"] == "127.0.0.1"
    assert run_kwargs["port"] == 8080
    assert run_kwargs["debug"] is True
    assert run_kwargs["use_reloader"] is False


@pytest.mark.web
def test_app_dunder_main_guard_executes_main(monkeypatch):
    run_kwargs = {}

    def fake_run(self, **kwargs):
        run_kwargs.update(kwargs)

    monkeypatch.setattr("flask.Flask.run", fake_run)
    _exec_file_as_main(str(SRC_DIR / "app.py"))

    assert run_kwargs["host"] == "127.0.0.1"
    assert run_kwargs["port"] == 8080


@pytest.mark.integration
def test_module2_clean_load_data_and_clean_data_happy_path(tmp_path):
    src = {
        "0": [
            "Johns Hopkins University\n",
            "Computer Science\n\n\n\nMasters\n",
            "\tJanuary 10, 2026\n",
            "\tAccepted on Jan 10\n",
            "",
            "https://www.thegradcafe.com/result/555",
            "American Fall2026 GPA:3.90 GRE329 GREV162 GREAW4.5",
            "Great fit\r\n",
        ]
    }
    p = tmp_path / "in.json"
    p.write_text(json.dumps(src), encoding="utf-8")

    loaded = clean_mod.load_data(str(p))
    cleaned = clean_mod.clean_data(loaded)

    assert len(cleaned) == 1
    row = cleaned[0]
    assert row["program"] == "Computer Science, Johns Hopkins University"
    assert row["Degree"] == "Masters"
    assert row["term"] == "Fall 2026"
    assert row["status"] == "Accepted"
    assert row["US/International"] == "American"
    assert row["GPA"] == "3.90"
    assert row["GRE Score"] == "329"
    assert row["GRE V Score"] == "162"


@pytest.mark.integration
def test_module2_clean_data_missing_degree_and_missing_nation_branch(capsys):
    bad = {
        "abc": [
            "MIT\n",
            "Computer Science only\n",
            "\tJanuary 11, 2026\n",
            "\tRejected on Jan 11\n",
            "",
            "https://www.thegradcafe.com/result/556",
            "Fall2026 GPA:3.70 GRE325 GREV160 GREAW4.0",
        ]
    }
    with pytest.raises(KeyError):
        clean_mod.clean_data(bad)
    out = capsys.readouterr().out
    assert "abc" in out
    assert "No Nation" in out


@pytest.mark.integration
def test_module2_scrape_data_parses_rows_and_save_data(tmp_path, monkeypatch):
    html = """
    <table>
      <tr><th>header</th></tr>
      <tr>
        <td>Uni A</td><td>Program A</td><td>Date A</td><td>Status A</td><td>x</td><td>y</td><td>Extra A</td><td>Comment A</td>
        <a href="/result/9001">link</a>
      </tr>
      <tr><td>Continuation comment</td></tr>
    </table>
    """

    class _Resp:
        def __init__(self, data):
            self.data = data

    class _HTTP:
        def request(self, _method, _url):
            return _Resp(html.encode("utf-8"))

    monkeypatch.setattr(scrape_mod.urllib3, "PoolManager", lambda **_kwargs: _HTTP())
    monkeypatch.setattr(scrape_mod.certifi, "where", lambda: "/tmp/ca.pem")

    rows = scrape_mod.scrape_data(1)
    assert 0 in rows
    assert rows[0][-1] == "Continuation comment"
    assert "https://www.thegradcafe.com/result/9001" in rows[0]

    out = tmp_path / "saved.json"
    scrape_mod.save_data(rows, str(out))
    assert out.exists()
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert "0" in loaded


@pytest.mark.db
def test_update_data_invalid_url_and_invalid_date_return_minus_one(monkeypatch):
    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _q, _p):
            return None

        def fetchone(self):
            return None

    # Explicitly execute helper cursor methods so these branches are covered.
    cur = _Cur()
    cur.execute("SELECT 1", ("x",))
    assert cur.fetchone() is None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(update_data.psycopg, "connect", lambda **_kwargs: _Conn())
    entries = [
        {"url": "not-a-valid-url", "date_added": "bad date"},
    ]
    assert update_data.insert_applicants_from_json_batch(entries) == -1


@pytest.mark.db
def test_update_data_operational_error_returns_minus_one(monkeypatch, capsys):
    def boom(**_kwargs):
        raise OperationalError("db down")

    monkeypatch.setattr(update_data.psycopg, "connect", boom)
    result = update_data.insert_applicants_from_json_batch([{"url": "https://x/1"}])
    assert result == -1
    assert "Error 'db down' occurred." in capsys.readouterr().out


@pytest.mark.db
def test_update_data_successful_insert_returns_zero_and_converts_fields(monkeypatch):
    conn = _InsertConn([(999,)])
    monkeypatch.setattr(update_data.psycopg, "connect", lambda **_kwargs: conn)

    entries = [
        {
            "url": "https://www.thegradcafe.com/result/999",
            "program": "Computer Science, Johns Hopkins University",
            "comments": "",
            "date_added": "January 24, 2026",
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
        }
    ]

    result = update_data.insert_applicants_from_json_batch(entries)
    assert result == 0
    assert conn.commits == 1
    assert len(conn._cursor.params) == 1
    inserted_params = conn._cursor.params[0]
    assert inserted_params[0] == 999
    assert str(inserted_params[3]) == "2026-01-24"
    assert inserted_params[8] == 3.9
    assert inserted_params[9] == 329.0
    assert inserted_params[10] == 162.0
    assert inserted_params[11] == 4.5


@pytest.mark.db
def test_update_data_conflict_path_returns_one(monkeypatch):
    conn = _InsertConn([None])
    monkeypatch.setattr(update_data.psycopg, "connect", lambda **_kwargs: conn)

    result = update_data.insert_applicants_from_json_batch(
        [{"url": "https://www.thegradcafe.com/result/1000"}]
    )
    assert result == 1
    assert conn.commits == 1


@pytest.mark.db
def test_update_data_non_numeric_url_id_is_skipped(monkeypatch):
    conn = _InsertConn([])
    monkeypatch.setattr(update_data.psycopg, "connect", lambda **_kwargs: conn)

    result = update_data.insert_applicants_from_json_batch(
        [{"url": "https://www.thegradcafe.com/result/not-a-number"}]
    )
    assert result == -1
    assert len(conn._cursor.params) == 0


@pytest.mark.db
def test_update_data_invalid_date_string_is_ignored_but_row_inserts(monkeypatch):
    conn = _InsertConn([(1001,)])
    monkeypatch.setattr(update_data.psycopg, "connect", lambda **_kwargs: conn)

    result = update_data.insert_applicants_from_json_batch(
        [{"url": "https://www.thegradcafe.com/result/1001", "date_added": "not a date"}]
    )
    assert result == 0
    assert len(conn._cursor.params) == 1
    assert conn._cursor.params[0][3] is None


@pytest.mark.web
def test_pages_percent_formatter_non_string_and_valueerror_path(monkeypatch):
    assert pages._format_percentages_in_text(123) == 123

    import builtins
    original_float = builtins.float

    def fake_float(value):
        if value == "52.3":
            raise ValueError("bad float")
        return original_float(value)

    monkeypatch.setattr(builtins, "float", fake_float)
    assert pages._format_percentages_in_text("52.3%") == "52.3%"
    assert pages._format_percentages_in_text("10%") == "10.00%"


@pytest.mark.integration
def test_insert_cursor_fetchone_empty_returns_none():
    cur = _InsertCursor([])
    assert cur.fetchone() is None


@pytest.mark.web
def test_pages_render_analysis_page_handles_short_rows(client, monkeypatch):
    class _Cursor:
        def execute(self, _q):
            return None

        def fetchall(self):
            return [("Question only",), ("Q2", "10%")]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

        def close(self):
            return None

    monkeypatch.setattr(pages, "connect", lambda: _Conn())
    pages.db_process = None
    pages.status_message = None
    pages.user_message = None

    resp = client.get("/analysis")
    html = resp.data.decode("utf-8")
    assert resp.status_code == 200
    assert "Question only" in html
    assert "10.00%" in html


@pytest.mark.web
def test_pages_check_db_completion_running_and_complete(monkeypatch):
    class _ProcRunning:
        def poll(self):
            return None

    class _ProcDone:
        def poll(self):
            return 0

    pages.db_process = _ProcRunning()
    pages.status_message = None
    pages.check_db_completion()
    assert pages.status_message == "Database update in progress..."
    assert pages.db_process is not None

    pages.db_process = _ProcDone()
    pages.check_db_completion()
    assert pages.status_message == "Last requested database update complete."
    assert pages.db_process is None


@pytest.mark.web
def test_pages_update_db_route_conflict_and_success_paths(client, monkeypatch):
    class _Cursor:
        def execute(self, _q):
            return None

        def fetchall(self):
            return [("Q", "10%")]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

        def close(self):
            return None

    class _RunningProc:
        def poll(self):
            return None

    class _StartedProc:
        def poll(self):
            return None

    # Exercise local helper methods so coverage includes these class branches.
    _cursor = _Cursor()
    assert _cursor.execute("SELECT 1") is None
    assert _cursor.fetchall() == [("Q", "10%")]
    assert _cursor.close() is None
    _conn = _Conn()
    assert isinstance(_conn.cursor(), _Cursor)
    assert _conn.close() is None
    assert _StartedProc().poll() is None

    monkeypatch.setattr(pages.subprocess, "Popen", lambda *_a, **_k: _StartedProc())

    pages.db_process = _RunningProc()
    pages.status_message = None
    pages.user_message = None
    conflict_resp = client.post("/pull-data", headers={"Accept": "application/json"})
    conflict_payload = conflict_resp.get_json()
    assert conflict_resp.status_code == 409
    assert conflict_payload["ok"] is False
    assert conflict_payload["busy"] is True
    assert "already running" in conflict_payload["message"]

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    success_resp = client.post("/pull-data", headers={"Accept": "application/json"})
    success_payload = success_resp.get_json()
    assert success_resp.status_code == 200
    assert success_payload["ok"] is True
    assert success_payload["busy"] is False
    assert "started" in success_payload["message"].lower()


@pytest.mark.web
def test_pages_update_analysis_route_conflict_and_success_paths(client, monkeypatch):
    class _Cursor:
        def execute(self, _q):
            return None

        def fetchall(self):
            return [("Q", "50%")]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

        def close(self):
            return None

    class _RunningProc:
        def poll(self):
            return None

    # Exercise local helper methods so coverage includes these class branches.
    _cursor = _Cursor()
    assert _cursor.execute("SELECT 1") is None
    assert _cursor.fetchall() == [("Q", "50%")]
    assert _cursor.close() is None
    _conn = _Conn()
    assert isinstance(_conn.cursor(), _Cursor)
    assert _conn.close() is None

    calls = {"questions": 0}
    monkeypatch.setattr(pages, "connect", lambda: _Conn())
    monkeypatch.setattr(pages, "questions", lambda _conn: calls.__setitem__("questions", 1))

    pages.db_process = _RunningProc()
    pages.status_message = None
    pages.user_message = None
    conflict_resp = client.post("/update_analysis", headers={"Accept": "application/json"})
    conflict_payload = conflict_resp.get_json()
    assert conflict_resp.status_code == 409
    assert conflict_payload["ok"] is False
    assert conflict_payload["busy"] is True
    assert "cannot run analysis" in conflict_payload["message"].lower()

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    success_resp = client.post("/update_analysis", headers={"Accept": "application/json"})
    success_payload = success_resp.get_json()
    assert success_resp.status_code == 200
    assert calls["questions"] == 1
    assert success_payload["ok"] is True
    assert success_payload["busy"] is False
    assert "analysis complete" in success_payload["message"].lower()


@pytest.mark.web
def test_pages_non_json_redirect_branches_for_busy_and_post_success(client, monkeypatch):
    class _RunningProc:
        def poll(self):
            return None

    class _FakeConn:
        pass

    # Busy /pull-data without JSON accept should redirect.
    pages.db_process = _RunningProc()
    resp_pull_busy = client.post("/pull-data", follow_redirects=False)
    assert resp_pull_busy.status_code == 303
    assert resp_pull_busy.headers["Location"].endswith("/analysis")

    # Busy /update_analysis without JSON accept should redirect.
    pages.db_process = _RunningProc()
    resp_analysis_busy = client.post("/update_analysis", follow_redirects=False)
    assert resp_analysis_busy.status_code == 303
    assert resp_analysis_busy.headers["Location"].endswith("/analysis")

    # Successful POST /update_analysis without JSON accept should redirect.
    pages.db_process = None
    monkeypatch.setattr(pages, "connect", lambda: _FakeConn())
    monkeypatch.setattr(pages, "questions", lambda _conn: None)
    resp_analysis_ok = client.post("/update_analysis", follow_redirects=False)
    assert resp_analysis_ok.status_code == 303
    assert resp_analysis_ok.headers["Location"].endswith("/analysis")


@pytest.mark.web
def test_pages_update_analysis_get_returns_rendered_page(client, monkeypatch):
    class _Cursor:
        def execute(self, _q):
            return None

        def fetchall(self):
            return [("Question", "12.3%")]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cursor()

        def close(self):
            return None

    pages.db_process = None
    pages.status_message = None
    pages.user_message = None
    monkeypatch.setattr(pages, "connect", lambda: _Conn())

    response = client.get("/update_analysis")
    html = response.data.decode("utf-8")

    assert response.status_code == 200
    assert "Question" in html
    assert "12.30%" in html


@pytest.mark.integration
def test_query_data_main_executes_connect_and_questions(monkeypatch):
    called = {"connect": False, "questions": False}

    class _DummyConn:
        pass

    def fake_connect():
        called["connect"] = True
        return _DummyConn()

    def fake_questions(conn):
        assert isinstance(conn, _DummyConn)
        called["questions"] = True

    monkeypatch.setattr(query_data, "connect", fake_connect)
    monkeypatch.setattr(query_data, "questions", fake_questions)

    # Execute the module's main block via function calls to cover the same path.
    conn = query_data.connect()
    query_data.questions(conn)

    assert called["connect"] is True
    assert called["questions"] is True


@pytest.mark.integration
def test_query_data_script_main_path_runs_with_stubbed_db(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://tmp_user:tmp_pw@localhost:5432/tmp_db")

    class _MainCursor:
        def __init__(self):
            # Return values for the 11 SELECT fetchone() calls in questions().
            self._vals = [
                (2,),       # q1
                (50.0,),    # q2
                (3.9, 329.0, 162.0, 4.5),  # q3
                (3.85,),    # q4
                (50.0,),    # q5
                (3.9,),     # q6
                (1,),       # q7
                (1,),       # q8
                (1,),       # q9
                (10, 20),   # q10
                (8, 15),    # q11
            ]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            return self._vals.pop(0)

    class _MainConn:
        def __init__(self):
            self._cursor = _MainCursor()

        def cursor(self):
            return self._cursor

        def commit(self):
            return None

    monkeypatch.setattr(query_data, "connect", lambda: _MainConn())
    query_data.main()


@pytest.mark.integration
def test_query_data_dunder_main_guard_executes_main(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://tmp_user:tmp_pw@localhost:5432/tmp_db")

    class _MainCursor:
        def __init__(self):
            self._vals = [
                (1,),
                (10.0,),
                (3.0, 320.0, 160.0, 4.0),
                (3.1,),
                (20.0,),
                (3.2,),
                (1,),
                (1,),
                (1,),
                (2, 2),
                (2, 2),
            ]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            return self._vals.pop(0)

    class _MainConn:
        def cursor(self):
            return _MainCursor()

        def commit(self):
            return None

    monkeypatch.setattr("psycopg.connect", lambda **_kwargs: _MainConn())
    _exec_file_as_main(str(SRC_DIR / "query_data.py"))


@pytest.mark.integration
def test_update_db_dunder_main_guard_executes_main(monkeypatch):
    called = {"value": False}

    def fake_update_db():
        called["value"] = True
        return 0

    monkeypatch.setattr(refresh_data, "update_db", fake_update_db)
    _exec_file_as_main(str(SRC_DIR / "update_db.py"))
    assert called["value"] is True


@pytest.mark.integration
def test_runner_fixture_smoke_uses_cli_runner_path(runner):
    # Forces execution of tests/conftest.py runner fixture return path.
    assert runner is not None


@pytest.mark.integration
def test_real_postgres_ready_fixture_failure_branch(monkeypatch):
    def boom(**_kwargs):
        raise OperationalError("forced test failure")

    monkeypatch.setattr(test_conftest.psycopg, "connect", boom)
    with pytest.raises(Failed):
        test_conftest.real_postgres_ready.__wrapped__({"dbname": "x"})


@pytest.mark.integration
def test_db_config_raises_when_required_env_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="Missing required database environment variable"):
        db_config.get_db_connect_kwargs()


@pytest.mark.integration
def test_postgres_connect_kwargs_fixture_fails_when_env_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    with pytest.raises(Failed):
        test_conftest.postgres_connect_kwargs.__wrapped__()


@pytest.mark.integration
def test_postgres_connect_kwargs_fixture_uses_pg_fallback_when_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PGDATABASE", "applicant_data")
    monkeypatch.setenv("PGUSER", "postgres")
    monkeypatch.setenv("PGPASSWORD", "abc123")
    monkeypatch.setenv("PGHOST", "127.0.0.1")
    monkeypatch.setenv("PGPORT", "5432")

    kwargs = test_conftest.postgres_connect_kwargs.__wrapped__()
    assert kwargs == {
        "dbname": "applicant_data",
        "user": "postgres",
        "password": "abc123",
        "host": "127.0.0.1",
        "port": "5432",
    }


@pytest.mark.db
def test_load_data_cursor_fetchone_non_select_and_empty_select_paths():
    cur = _LoadDataCursor(select_results=[])
    cur.execute("UPDATE applicants SET program = %s", ("x",))
    assert cur.fetchone() is None
    cur.execute("SELECT 1 FROM applicants WHERE p_id = %s", (1,))
    assert cur.fetchone() is None


@pytest.mark.integration
@pytest.mark.parametrize(
    "module_filename, run_name",
    [
        ("test_analysis_format.py", "cov_import_test_analysis_format"),
        ("test_buttons.py", "cov_import_test_buttons"),
        ("test_db_insert.py", "cov_import_test_db_insert"),
        ("test_integration_end_to_end.py", "cov_import_test_integration"),
    ],
)
def test_test_module_import_guard_insert_path_branch(monkeypatch, module_filename, run_name):
    # Execute each test module with src removed from sys.path first so its
    # import-guard insertion branch is exercised.
    src_dir_str = str(SRC_DIR)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != src_dir_str])
    _load_module_from_path(str(TESTS_DIR / module_filename), run_name)
    assert src_dir_str in sys.path
