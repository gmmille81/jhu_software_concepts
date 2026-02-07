import psycopg
from psycopg import OperationalError

def connect():
    try:
        connection = psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

import psycopg
from psycopg import OperationalError, sql

def connect():
    try:
        connection = psycopg.connect(
            dbname="applicant_data",
            user="postgres",
            password="abc123",
            host="127.0.0.1",
            port=5432
        )
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None

def questions(connection):
    with connection.cursor() as cur:
        # Drop and recreate the answers table
        cur.execute("DROP TABLE IF EXISTS answers_table;")
        cur.execute("""
            CREATE TABLE answers_table (
                question TEXT,
                answer TEXT
            );
        """)
        connection.commit()

        # List to hold answers for returning to Flask
        answers = []

        # --- QUERY 1 ---
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

        # --- QUERY 2 ---
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

        # --- QUERY 3 ---
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

        # --- QUERY 4 ---
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

        # --- QUERY 5 ---
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

        # --- QUERY 6 ---
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

        # --- QUERY 7 ---
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

        # --- QUERY 8 ---
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

        # --- QUERY 9 ---
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

        # --- QUERY 10 ---
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

        # --- QUERY 11 ---
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

        # Commit all inserts
        connection.commit()

    return answers

# Example usage:
conn = connect()
answers_list = questions(conn)
print(answers_list)


# Example usage
conn = connect()
questions(conn)
