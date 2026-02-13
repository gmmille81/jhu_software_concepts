"""Standalone entrypoint used by subprocess-based pull-data route."""

from refresh_data import update_db
from module_2.scrape import scrape_data


def main():
    """Execute one DB refresh cycle and return its status code."""
    return update_db()


if __name__ == "__main__":
    res = main()
