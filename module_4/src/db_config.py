import os


REQUIRED_DB_ENV_VARS = ("PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT")


def get_db_connect_kwargs():
    """
    Build psycopg connection kwargs from environment variables.

    Required variables:
    - PGDATABASE
    - PGUSER
    - PGPASSWORD
    - PGHOST
    - PGPORT
    """
    missing = [name for name in REQUIRED_DB_ENV_VARS if not os.getenv(name)]
    if missing:
        missing_joined = ", ".join(missing)
        raise RuntimeError(
            f"Missing required database environment variable(s): {missing_joined}"
        )

    return {
        "dbname": os.environ["PGDATABASE"],
        "user": os.environ["PGUSER"],
        "password": os.environ["PGPASSWORD"],
        "host": os.environ["PGHOST"],
        "port": int(os.environ["PGPORT"]),
    }
