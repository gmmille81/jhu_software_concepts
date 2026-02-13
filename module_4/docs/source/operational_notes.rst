Operational Notes
=================

Busy-State Policy
-----------------
- The app treats data pulls as a mutually exclusive background operation.
- If a pull is already running, a second ``POST /pull-data`` request returns ``409`` with ``{"busy": true}``.
- While pull is running, ``POST /update_analysis`` is also blocked and returns ``409`` with ``{"busy": true}``.
- This prevents query refreshes against partially updated data.

Idempotency Strategy
--------------------
- Pull operations can be retried safely.
- Inserting the same scraped records multiple times does not create extra rows.
- The insert path uses conflict handling so repeated payloads are effectively no-ops for existing records.

Uniqueness Keys
---------------
- The canonical uniqueness key is ``p_id`` in ``applicants``.
- ``p_id`` is derived from the numeric suffix of the GradCafe result URL.
- Database constraints enforce uniqueness, and tests validate duplicate pulls remain consistent.

Troubleshooting (Local & CI)
----------------------------

Database connection errors
^^^^^^^^^^^^^^^^^^^^^^^^^^
- Symptom: ``Missing required PostgreSQL environment variable(s)``.
- Fix: set either ``DATABASE_URL`` or all of ``PGDATABASE``, ``PGUSER``, ``PGPASSWORD``, ``PGHOST``, ``PGPORT``.
- Symptom: ``role ... does not exist``.
- Fix: verify the PostgreSQL role name matches ``PGUSER`` (for example ``postgres`` vs ``postgre`` typo).

GitHub Actions postgres startup failures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- Symptom: service container fails with ``superuser password is not specified``.
- Fix: ensure workflow sets a non-empty ``POSTGRES_PASSWORD`` for PostgreSQL startup.
- Symptom: tests fail because ``PGPASSWORD`` is empty in workflow env.
- Fix: confirm the repository secret name exactly matches the workflow reference and is set at repo scope.

Coverage or marker issues
^^^^^^^^^^^^^^^^^^^^^^^^^
- Symptom: ``Unknown config option`` or unknown marker warnings.
- Fix: ensure ``pytest.ini`` marker entries are indented under ``markers =``.
- Symptom: coverage reports ``No data was collected``.
- Fix: point ``--cov`` at the actual import path used by tests (for this module: ``--cov=src`` when running in ``module_4``).

Docs build failures on Read the Docs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- Symptom: ``Expected file not found: docs/conf.py``.
- Fix: set ``.readthedocs.yaml`` to the real path: ``docs/source/conf.py``.
