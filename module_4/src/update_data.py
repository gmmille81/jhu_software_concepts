### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Update the applicants SQL table with newly-parsed and cleaned data
import sys
import os

# Ensure current folder is in sys.path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import psycopg
from psycopg import OperationalError, sql
from datetime import datetime
#creates db using python so manual db creation in terminal no longer required (only run once to init DB)
from psycopg.sql import SQL, Identifier

def insert_applicants_from_json_batch(entries):
    """
    Inserts a batch of applicant records.

    Returns:
        1 - at least one row hit ON CONFLICT
        0 - all rows inserted successfully
       -1 - all rows invalid or a DB error occurred
    """
    had_conflict = False
    had_success = False

    try:
        with psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        ) as conn:
            with conn.cursor() as cur:

                insert_query = """
                INSERT INTO applicants (
                    p_id, program, comments, date_added, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (p_id) DO NOTHING
                RETURNING p_id;
                """

                for entry in entries:
                    # Extract p_id
                    p_id = None
                    url = entry.get("url")
                    if url and "/" in url:
                        try:
                            p_id = int(url.rstrip("/").split("/")[-1])
                        except ValueError:
                            pass

                    if p_id is None:
                        continue

                    # Date conversion
                    date_val = None
                    if entry.get("date_added"):
                        try:
                            date_val = datetime.strptime(
                                entry["date_added"], "%B %d, %Y"
                            ).date()
                        except ValueError:
                            pass

                    # Numeric conversions
                    gpa = float(entry.get("GPA")) if entry.get("GPA") else None
                    gre = float(entry.get("GRE Score")) if entry.get("GRE Score") else None
                    gre_v = float(entry.get("GRE V Score")) if entry.get("GRE V Score") else None
                    gre_aw = float(entry.get("GRE AW")) if entry.get("GRE AW") else None

                    cur.execute(insert_query, (
                        p_id,
                        entry.get("program", ""),
                        entry.get("comments"),
                        date_val,
                        url,
                        entry.get("status"),
                        entry.get("term"),
                        entry.get("US/International"),
                        gpa,
                        gre,
                        gre_v,
                        gre_aw,
                        entry.get("Degree"),
                        entry.get("llm-generated-program", ""),
                        entry.get("llm-generated-university", "")
                    ))

                    if cur.fetchone():
                        had_success = True
                    else:
                        had_conflict = True
                        #print(f"Conflict detected for p_id: {p_id}")

                conn.commit()

        if had_conflict:
            return 1
        if had_success:
            return 0
        return -1

    except OperationalError as e:
        print("Error '{}' occurred.".format(e))
        return -1