import psycopg
from psycopg import OperationalError


def connect():
    """
    Establish and return a connection to the PostgreSQL database.

    This function attempts to connect using psycopg and returns
    the connection object so it can be reused by other functions.
    """
    try:
        # Attempt to open a connection to the PostgreSQL database
        connection = psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        )
    except OperationalError as e:
        # Print an error message if the database connection fails
        print(f"The error '{e}' occurred")

    # Return the database connection object
    return connection


def questions(connection):
    """
    Run a series of analytical SQL queries against the applicants table.

    This function:
    - Recreates the answers_table
    - Executes multiple analysis queries
    - Stores each question and its answer in answers_table
    - Returns all question–answer pairs for use in Flask
    """

    # Open a database cursor using a context manager
    with connection.cursor() as cur:

        # Remove any existing answers table so results are always fresh
        cur.execute("DROP TABLE IF EXISTS answers_table;")

        # Create a new table to store analysis questions and answers
        cur.execute("""
            CREATE TABLE answers_table (
                question TEXT,
                answer TEXT
            );
        """)
        connection.commit()

        # List used to collect answers for returning to the caller
        answers = []

        # --- QUERY 1 ---
        # Count how many applicants applied for Fall 2026
        question = 'How many entries do you have in your database who have applied for Fall 2026?'
        cur.execute("""
            SELECT COUNT(p_id)
            FROM applicants
            WHERE term = 'Fall 2026';
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Fall 2026 Applicants: ', result)

        # --- QUERY 2 ---
        # Calculate percentage of applicants who are international students
        question = 'What percentage of entries are from international students (not American or Other) (to two decimal places)?'
        cur.execute("""
            SELECT ROUND(
                100.0 * COUNT(*) FILTER (WHERE us_or_international NOT IN ('American', 'Other'))
                / COUNT(*),
                2
            )
            FROM applicants;
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Percent International: ', result)

        # --- QUERY 3 ---
        # Compute average GPA and GRE metrics for applicants who provided them
        question = 'What is the average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics?'
        cur.execute("""
            SELECT
                ROUND(AVG(gpa)::numeric, 2),
                ROUND(AVG(gre)::numeric, 2),
                ROUND(AVG(gre_v)::numeric, 2),
                ROUND(AVG(gre_aw)::numeric, 2)
            FROM applicants;
        """)
        avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = cur.fetchone()

        result_str = f"GPA: {avg_gpa} GRE: {avg_gre} GRE V: {avg_gre_v} GRE AW: {avg_gre_aw}"

        answers.append([question, result_str])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, result_str)
        )
        print('Average Stats: ', result_str)

        # --- QUERY 4 ---
        # Calculate the average GPA of American applicants for Fall 2026
        question = 'What is their average GPA of American students in Fall 2026?'
        cur.execute("""
            SELECT ROUND(AVG(gpa)::numeric, 2)
            FROM applicants
            WHERE us_or_international = 'American' AND term = 'Fall 2026';
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('AVG GPA of Fall 2026 American Students: ', result)

        # --- QUERY 5 ---
        # Compute the acceptance percentage for Fall 2026 applicants
        question = 'What percent of entries for Fall 2026 are Acceptances (to two decimal places)?'
        cur.execute("""
            SELECT ROUND(
                100.0 * COUNT(*) FILTER (WHERE term = 'Fall 2026' AND status = 'Accepted')
                / COUNT(*),
                2
            )
            FROM applicants;
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Percent of acceptance for Fall 2026: ', result)

        # --- QUERY 6 ---
        # Calculate average GPA of accepted applicants for Fall 2026
        question = 'What is the average GPA of applicants who applied for Fall 2026 who are Acceptances?'
        cur.execute("""
            SELECT ROUND(AVG(gpa)::numeric, 2)
            FROM applicants
            WHERE status = 'Accepted' AND term = 'Fall 2026';
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Avg GPA of Fall 2026 Accepted students: ', result)

        # --- QUERY 7 ---
        # Count applicants applying to JHU for a Master's in Computer Science
        question = 'How many entries are from applicants who applied to JHU for a masters degrees in Computer Science?'
        cur.execute("""
            SELECT COUNT(p_id)
            FROM applicants
            WHERE degree = 'Masters'
            AND program = 'Computer Science, Johns Hopkins University';
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Number of entries from JHU Comp Sci Masters Applicants: ', result)

        # --- QUERY 8 ---
        # Count PhD acceptances in CS at selected universities during 2026
        question = 'How many entries from 2026 are acceptances from applicants who applied to Georgetown University, MIT, Stanford University, or Carnegie Mellon University for a PhD in Computer Science?'
        cur.execute("""
            SELECT COUNT(*)
            FROM applicants
            WHERE status = 'Accepted'
            AND EXTRACT(YEAR FROM date_added) = 2026
            AND degree = 'PhD'
            AND TRIM(SPLIT_PART(program, ',', 2)) IN ('Georgetown University', 'MIT', 'Stanford University', 'Carnegie Mellon University')
            AND TRIM(SPLIT_PART(program, ',', 1)) = 'Computer Science';
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Number of acceptances to Georgetown University, MIT, Stanford University, or Carnegie Mellon for a PhD in Computer Science: ', result)

        # --- QUERY 9 ---
        # Repeat Query 8 using LLM-generated program and university fields
        question = 'Do your numbers for question 8 change if you use LLM Generated Fields?'
        cur.execute("""
            SELECT COUNT(*)
            FROM applicants
            WHERE status = 'Accepted'
            AND EXTRACT(YEAR FROM date_added) = 2026
            AND degree = 'PhD'
            AND llm_generated_program = 'Computer Science'
            AND llm_generated_university IN ('Georgetown University', 'MIT', 'Stanford University', 'Carnegie Mellon University');
        """)
        result = cur.fetchone()[0]

        answers.append([question, str(result)])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, str(result))
        )
        print('Same as last question but by using llm fields', result)

        # --- QUERY 10 ---
        # Count unique program and university names from raw program strings
        question = 'How many unique program names and university names are in the data set?'
        cur.execute("""
            SELECT 
                COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 1))),
                COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 2)))
            FROM applicants;
        """)
        unique_programs, unique_universities = cur.fetchone()

        result_str = f"{unique_programs}, {unique_universities}"
        answers.append([question, result_str])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, result_str)
        )
        print('Number of unique programs and universities in dataset, respectively: ', result_str)

        # --- QUERY 11 ---
        # Count unique program and university names from LLM-generated fields
        question = 'How many unique llm-generated program names and university names are in the data set?'
        cur.execute("""
            SELECT 
                COUNT(DISTINCT llm_generated_program),
                COUNT(DISTINCT llm_generated_university)
            FROM applicants;
        """)
        unique_programs, unique_universities = cur.fetchone()

        result_str = f"{unique_programs}, {unique_universities}"
        answers.append([question, result_str])
        cur.execute(
            "INSERT INTO answers_table (question, answer) VALUES (%s, %s)",
            (question, result_str)
        )
        print('Number of unique llm-generated programs and universities in dataset, respectively: ', result_str)

        # Commit all inserted answers to the database
        connection.commit()

    # Return list of question–answer pairs
    return answers


# Allow this file to be executed directly for testing purposes
if __name__ == "__main__":
    conn = connect()
    questions(conn)
