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
                print(f"Database '{db_name}' created successfully.")

    except OperationalError as e:
        print(f"The error '{e}' occurred")


import psycopg
from psycopg import OperationalError

def create_table():
    """
    Creates the 'applicants' table in the 'applicant_data' database using psycopg3.
    """
    try:
        # Connect to the database using a context manager
        with psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        ) as conn:

            # Use a cursor context manager
            with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS applicants (
                    p_id SERIAL PRIMARY KEY,
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
        print(f"The error '{e}' occurred")

def bulk_insert_json(json_file_path, batch_size=1000):
    """
    Bulk inserts JSON Lines data into the 'applicants' table using psycopg3.
    If the table already contains data, it will be deleted first to avoid duplicates.
    """
    try:
        # Connect to the database
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

                # Step 2: Prepare insert query
                insert_query = """
                INSERT INTO applicants (
                    program, comments, date_added, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw, degree,
                    llm_generated_program, llm_generated_university
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """

                batch = []  # Must define before using
                count = 0

                # Step 3: Read JSON Lines file and populate batches
                with open(json_file_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        entry = json.loads(line)

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
                            program_field,
                            entry.get("comments"),
                            date_val,
                            entry.get("url"),
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
                            count += len(batch)
                            print(f"{count} records inserted...")
                            batch = []  # reset batch

                # Insert any remaining rows
                if batch:
                    cur.executemany(insert_query, batch)
                    conn.commit()
                    count += len(batch)
                    print(f"{count} records inserted...")

        print("All records inserted successfully!")

    except OperationalError as e:
        print(f"The error '{e}' occurred")


#create_database("applicant_data", "postgres", "abc123", "127.0.0.1", 5432)
create_table()
bulk_insert_json('jhu_software_concepts/module_3/module_2/llm_extend_applicant_data.json')
# def load_dict(file_path):
#     data = []
#     with open(file_path) as f:
#         for line in f:
#             data.append(json.loads(line))
#     return data

#data = load_dict(r'jhu_software_concepts/module_3/module_2/llm_extend_applicant_data.json')
#print(data)

