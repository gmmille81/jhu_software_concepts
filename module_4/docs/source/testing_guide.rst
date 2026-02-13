Testing Guide
=============

Markers
-------
All tests are marked with one or more of:

- ``web``
- ``buttons``
- ``analysis``
- ``db``
- ``integration``

Run all marker groups:

.. code-block:: bash

   pytest -m "web or buttons or analysis or db or integration"

Selectors
---------
UI tests use stable selectors in ``src/templates/index.html``:

- ``data-testid="pull-data-btn"``
- ``data-testid="update-analysis-btn"``

Fixtures
--------
Key shared fixtures live in ``tests/conftest.py``:

- ``client``: Flask test client.
- ``postgres_connect_kwargs``: DB conninfo from ``DATABASE_URL``.
- ``reset_real_applicants_table``: real-table setup/teardown.
- ``seeded_answers_table``: initial analysis rows.
- helper assertions for analysis labels and two-decimal values.

Notes
-----
- Tests avoid live internet dependencies by injecting fakes/mocks for scraper and cleaner paths.
- Busy-state checks are deterministic via injectable process state (no arbitrary sleep loops).
