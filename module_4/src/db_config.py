import os


def get_db_connect_kwargs():
    """
    Build psycopg connection kwargs from DATABASE_URL.

    Required variable:
    - DATABASE_URL
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "Missing required database environment variable: DATABASE_URL"
        )

    return {"conninfo": database_url}
