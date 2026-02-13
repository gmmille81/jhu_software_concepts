### Author: Greg Miller
### Class: Modern Software Concepts in Python
### Purpose: Give update_db own file so it can be used by subprocesses. 
from refresh_data import update_db
from module_2.scrape import scrape_data


def main():
    return update_db()


if __name__ == "__main__":
    res = main()
