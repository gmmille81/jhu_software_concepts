Overview & Setup
================

Overview
--------
This project provides:

- A Flask web app that renders admission analysis results, with buttons to pull and analyze data from GradCafe.com.
- PostgreSQL persistence for applicant records and computed analysis.

Environment Variables
---------------------
The application and integration tests require:

- ``DATABASE_URL``: PostgreSQL connection string.

Example:

.. code-block:: bash

   export DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:5432/applicant_data'

Run the App
-----------

Before starting the Flask app, initialize a baseline SQL database:

.. code-block:: bash

   cd module_4
   python3 src/load_data.py

If this is the first run (or ``answers_table`` is empty), run:

.. code-block:: bash

   cd module_4
   python3 src/query_data.py

This precomputes initial analysis answers so values are visible immediately on ``/analysis``.

.. code-block:: bash

   cd module_4
   python3 src/app.py

The web UI is served at ``http://127.0.0.1:8080/analysis``.

Run Tests
---------

.. code-block:: bash

   cd module_4
   pytest -q tests

To run by marker group:

.. code-block:: bash

   pytest -m "web or buttons or analysis or db or integration"
