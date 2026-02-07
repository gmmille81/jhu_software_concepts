import sys
import os

# Ensure current folder is in sys.path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import psycopg
from psycopg import OperationalError, sql
import json
from datetime import datetime
#creates db using python so manual db creation in terminal no longer required (only run once to init DB)
from psycopg.sql import SQL, Identifier

def create_database(db_name, db_user, db_password, db_host, db_port):
    """
    Creates a new PostgreSQL database using psycopg3.
    """
    try:
        # Connect to default 'postgres' database
        with psycopg.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        ) as conn:

            # Enable autocommit for CREATE DATABASE
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute(
                    SQL("CREATE DATABASE {}").format(Identifier(db_name))
                )
                print("Database '{}' created successfully.".format(db_name))

    except OperationalError as e:
        print("Error '{}' occurred.".format(e))

def create_table():
    """
    Drops and creates the 'applicants' table in the 'applicant_data' database using psycopg3.
    Uses p_id from the last part of the URL as a BIGINT primary key.
    """
    try:
        with psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        ) as conn:
            with conn.cursor() as cur:
                # Step 1: Drop table if it exists
                cur.execute("DROP TABLE IF EXISTS applicants;")
                print("Dropped existing table 'applicants' (if it existed).")

                # Step 2: Create table
                create_table_query = """
                CREATE TABLE applicants (
                    p_id BIGINT PRIMARY KEY,
                    program TEXT,
                    comments TEXT,
                    date_added DATE,
                    url TEXT,
                    status TEXT,
                    term TEXT,
                    us_or_international TEXT,
                    gpa FLOAT,
                    gre FLOAT,
                    gre_v FLOAT,
                    gre_aw FLOAT,
                    degree TEXT,
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
                """
                cur.execute(create_table_query)
                conn.commit()
                print("Table 'applicants' created successfully (psycopg3).")

    except OperationalError as e:
        print("Error '{}' occurred.".format(e))

def bulk_insert_json(json_file_path, batch_size=1000):
    """
    Bulk inserts JSON Lines data into the 'applicants' table using psycopg3.
    If the table already contains data, it will be deleted first to avoid duplicates.
    p_id is extracted from the URL (last number after '/').
    Prints the number of duplicates skipped.
    """
    try:
        with psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        ) as conn:
            with conn.cursor() as cur:

                # Step 1: Clear existing data
                cur.execute("TRUNCATE TABLE applicants;")
                conn.commit()
                print("Existing data deleted from 'applicants' table.")

                # Step 2: Prepare insert query with conflict handling
                insert_query = """
                INSERT INTO applicants (
                    p_id, program, comments, date_added, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (p_id) DO NOTHING;
                """

                batch = []
                count_inserted = 0
                count_duplicates = 0

                # Step 3: Read JSON Lines file and populate batches
                with open(json_file_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        entry = json.loads(line)

                        # Extract p_id from URL
                        p_id = None
                        url = entry.get("url")
                        if url and "/" in url:
                            try:
                                p_id = int(url.rstrip("/").split("/")[-1])
                            except ValueError:
                                p_id = None

                        if p_id is None:
                            continue  # skip rows with invalid/missing URL

                        # Convert date safely
                        date_val = None
                        if entry.get("date_added"):
                            try:
                                date_val = datetime.strptime(entry["date_added"], "%B %d, %Y").date()
                            except ValueError:
                                date_val = None

                        # Convert numeric fields safely
                        gpa = float(entry.get("GPA")) if entry.get("GPA") else None
                        gre = float(entry.get("GRE Score")) if entry.get("GRE Score") else None
                        gre_v = float(entry.get("GRE V Score")) if entry.get("GRE V Score") else None
                        gre_aw = float(entry.get("GRE AW Score")) if entry.get("GRE AW Score") else None

                        program_field = entry.get("program", "")

                        # Add row to batch
                        batch.append((
                            p_id,
                            program_field,
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
                            entry.get("llm-generated-program"),
                            entry.get("llm-generated-university")
                        ))

                        # Insert batch if size reached
                        if len(batch) >= batch_size:
                            cur.executemany(insert_query, batch)
                            conn.commit()

                            # Count inserted vs duplicates
                            for row in batch:
                                cur.execute("SELECT 1 FROM applicants WHERE p_id = %s", (row[0],))
                                if cur.fetchone():
                                    count_inserted += 1
                                else:
                                    count_duplicates += 1

                            #print(f"{count_inserted} records inserted so far...")
                            batch = []

                # Insert any remaining rows
                if batch:
                    cur.executemany(insert_query, batch)
                    conn.commit()

                    for row in batch:
                        cur.execute("SELECT 1 FROM applicants WHERE p_id = %s", (row[0],))
                        if cur.fetchone():
                            count_inserted += 1
                        else:
                            count_duplicates += 1
                    print("'{}' records inserted in total.".format(count_inserted))

                print("Number of duplicates skipped: '{}'".format(count_duplicates))

        print("All records inserted successfully!")

    except OperationalError as e:
        print("Error '{}' occurred.".format(e))
#Function to append the DB with data from a JSON object (utilitzed in the refresh_data script)


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


#create_database("applicant_data", "postgres", "abc123", "127.0.0.1", 5432)
create_table()
bulk_insert_json('jhu_software_concepts/module_3/module_2/llm_extend_applicant_data.json')

