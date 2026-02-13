Architecture
============

Web Layer
---------
- ``src/app.py``: Flask app factory and process entrypoint.
- ``src/pages.py``: Routes and rendering logic.
  - ``GET /analysis`` renders the analysis page.
  - ``POST /pull-data`` triggers background data refresh.
  - ``POST /update_analysis`` refreshes analysis answers.

Business Layer
---------
- ``src/module_2/scrape.py``: Scrapes data from GradCafe.com.
- ``src/module_2/clean.py``: Transforms raw scraped data into clean, normalized records.
- ``src/refresh_data.py``: Coordinates scrape + clean to get consolidated new data in dict format.
- ``src/update_data.py``: Batch inserts normalized records into PostgreSQL table.

Database Layer
--------------
- ``src/db_config.py``: Centralized DB connection configuration via ``DATABASE_URL``.
- ``src/load_data.py``: Creates baseline SQL DB with stored JSON data.
- ``src/query_data.py``: Runs analysis queries and stores answers in ``answers_table``.

Execution Flow
--------------
1. User clicks **Pull Data** in UI.
2. Web layer starts ``update_db.py`` subprocess, which pulls new data from GradCafe.
3. New data is inserted into PostgreSQL.
4. User clicks **Update Analysis**, and ``query_data.questions()`` runs analysis queries and stores results.
5. Flask webpage displays updated analysis values.
