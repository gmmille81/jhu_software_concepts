### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Get updated data from GradCafe

import subprocess
from module_2.scrape import scrape_data       # Function to scrape data from the website
from module_2.clean import clean_data         # Function to clean/format the scraped data
from update_data import insert_applicants_from_json_batch  # Function to insert data into SQL DB
import psycopg                                # PostgreSQL database connector

def get_newest_p():
    """
    Get the highest p_id (primary key) currently in the applicants table.
    Returns:
        int or None: The maximum p_id in the table, or None if table is empty
    """
    # Connect to the PostgreSQL database
    with psycopg.connect(
        dbname="applicant_data",
        user="postgres",
        password="abc123",
        host="127.0.0.1",
        port=5432
    ) as conn:
        # Open a cursor to execute SQL commands
        with conn.cursor() as cur:
            # Get the maximum p_id from the applicants table
            cur.execute("SELECT MAX(p_id) FROM applicants;")
            max_p_id = cur.fetchone()[0]  # fetchone() returns a tuple, take first element

    # Convert to int if not None, otherwise keep as None
    max_p_id = int(max_p_id) if max_p_id is not None else None
    return max_p_id

def update_db():
    """
    Update the database with any new applicants not yet stored.
    Steps:
      1. Get the newest p_id in the database.
      2. Scrape the first page of the site to see what the newest entry is.
      3. Calculate how many new entries are missing from the DB.
      4. Scrape the missing entries, clean them, and insert into the database.
    Returns:
        int: 0 if new data was added, 1 if database was already up-to-date
    """
    # Scrape the first page to see newest entries
    new_data = scrape_data(1)
    # Clean the scraped data
    new_data_cleaned = clean_data(new_data)

    # Extract the newest p_id from the website (assuming URL ends with p_id)
    newest_site_p = int(new_data_cleaned[0]['url'].split('/')[-1])

    # Calculate number of new entries missing in the database
    num_data_needed = newest_site_p - get_newest_p()
    print(num_data_needed)
    if num_data_needed != 0:
        # Scrape the missing entries
        new_data = scrape_data(num_data_needed)
        # Clean the newly scraped data
        new_data_cleaned = clean_data(new_data)
        # Insert new applicants into the database
        flag = insert_applicants_from_json_batch(new_data_cleaned)

        return 0  # New data was added
    else: 
        return 1  # Database already up-to-date
