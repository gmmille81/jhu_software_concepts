## Greg Miller ##
## JHU Software Concepts - Module 4 (Web + DB + Testing + CI) - Feb 13 2026 ##

Link to Docs: https://jhu-software-concepts-module-4-greg-miller.readthedocs.io/en/latest/

Approach:
- Leveraged existing code from module_3 and began writing tests to meet assignment criteria
- When tests could not meet assignment criteria based on existing code, module_3 code was updated to be more consistent with expected behavior
- Added tests file by file and ran pytest to ensure passage
- Tried to reuse test code and have integration tests build on each other without building dependencies
- Once required tests were finalized, generated coverage report and tasked agent with creating tests to meet 100%
- Tested app.py locally to ensure still working with new edits
- Added CI workflow for running tests with PostgreSQL.
- Added Sphinx docs (overview/setup, architecture, API reference, testing guide, operational notes, troubleshooting).

Prior to running (env-specific steps):
- Install PostgreSQL in your environment and start the service.
- Create a database and role appropriate for your local setup.
- Export required DB configuration:
  - Preferred:
    - `DATABASE_URL=postgresql://<user>:<password>@127.0.0.1:5432/<database>`
  - Or fallback:
    - `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGHOST`, `PGPORT`

To Run:
- From `module_4`, install dependencies:
  - `pip install -r requirements.txt`
  - `pip install -r src/requirements.txt`
  - `pip install -r docs/requirements.txt`
- Initialize baseline SQL data:
  - `python3 src/load_data.py`
  - This creates/resets `applicants` and bulk-loads baseline rows from `src/module_2/llm_extend_applicant_data.json`.
- If this is the first run (or `answers_table` is empty), initialize analysis answers:
  - `python3 src/query_data.py`
  - This computes and stores initial answers so `/analysis` shows values immediately.
- Start the Flask app:
  - `python3 src/app.py`
  - App runs at `http://127.0.0.1:8080/analysis`

Web workflow:
- `GET /analysis`: render analysis page and buttons.
- `POST /pull-data`: pull new records and insert into DB.
- `POST /update_analysis`: recompute and store analysis answers.

Testing:
- Run full suite:
  - `pytest`
- Run by required marker groups:
  - `pytest -m "web or buttons or analysis or db or integration"`
- Marker list:
  - `web`, `buttons`, `analysis`, `db`, `integration`

Operational behavior:
- Busy-state policy:
  - If pull is running, pull/update-analysis requests return `409` with `{"busy": true}`.
- Uniqueness policy:
  - `applicants.p_id` is the unique key (derived from GradCafe URL id).
- Idempotency strategy:
  - Duplicate pulls remain consistent with uniqueness constraints.

CI / Docs:
- GitHub Actions runs pytest against PostgreSQL.
- Sphinx docs are in `docs/source`.
- Local docs build:
  - `sphinx-build -b html docs/source docs/build/html`
- Read the Docs config:
  - Repo root `.readthedocs.yaml` points to `module_4/docs/source/conf.py`.

Troubleshooting:
- Missing DB env vars:
  - Set `DATABASE_URL` or all `PG*` values.
- Role errors (`role ... does not exist`):
  - Verify DB username exactly matches an existing PostgreSQL role.
- Pytest marker warnings:
  - Ensure `pytest.ini` marker definitions are properly indented under `markers =`.
- RTD config-path errors:
  - Ensure RTD uses repo root `.readthedocs.yaml` and that it points to `module_4/docs/source/conf.py`.
