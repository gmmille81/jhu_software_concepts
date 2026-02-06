import psycopg
from psycopg import OperationalError, sql

def connect():
    try:
        # Connect to the database using a context manager
        connection = psycopg.connect(dbname="applicant_data",user="postgres",password="abc123",host="127.0.0.1",port=5432)

            # Use a cursor context manager
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection
def questions(connection):
    with connection.cursor() as cur:
        answers = []
        query1 = """
        SELECT COUNT(p_id)
        FROM applicants
        WHERE term = 'Fall 2026';
        """
        cur.execute(query1)
        answers.append(cur.fetchall()[0])
        query2 = """
        SELECT
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE us_or_international NOT IN ('American', 'Other'))
                / COUNT(*),
                2
            )
        FROM applicants;

        """
        cur.execute(query2)
        answers.append(cur.fetchall()[0])
        query3 = """
        SELECT
            ROUND(AVG(gpa):: numeric, 2) AS avg_gpa,
            ROUND(AVG(gre):: numeric, 2) AS avg_gre,
            ROUND(AVG(gre_v):: numeric, 2) AS avg_gre_v,
            ROUND(AVG(gre_aw):: numeric, 2) AS avg_gre_aw
        FROM applicants;
        """
        cur.execute(query3)
        avg_gpa, avg_gre, avg_gre_v, avg_gre_aw = cur.fetchall()[0]
        answers.append([avg_gpa,avg_gre,avg_gre_v,avg_gre_aw])

        query4 = """
        SELECT
            ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
        FROM applicants
        WHERE us_or_international = 'American'
        AND term = 'Fall 2026';
        """
        cur.execute(query4)
        answers.append(cur.fetchall()[0])

        query5 = """
        SELECT
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE term = 'Fall 2026' AND status = 'Accepted')
                / COUNT(*),
                2
            )
        FROM applicants;
        """
        cur.execute(query5)
        answers.append(cur.fetchall()[0])

        query6 = """
        SELECT
            ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
        FROM applicants
        WHERE status = 'Accepted'
        AND term = 'Fall 2026';
        """
        cur.execute(query6)
        answers.append(cur.fetchall()[0])

        query7 = """
        SELECT COUNT(p_id)
        FROM applicants
        WHERE degree = 'Masters'
        AND program = 'Computer Science, Johns Hopkins University';
        """
        cur.execute(query7)
        answers.append(cur.fetchall()[0])

        query8 = """
        SELECT COUNT(*) AS num_acceptances
        FROM applicants
        WHERE status = 'Accepted'
        AND EXTRACT(YEAR FROM date_added) = 2026
        AND degree = 'PhD'
        AND TRIM(SPLIT_PART(program, ',', 2)) IN ('Georgetown University', 'MIT', 'Stanford University', 'Carnegie Mellon University')
        AND TRIM(SPLIT_PART(program, ',', 1)) = 'Computer Science';
        """
        #
        
        cur.execute(query8)
        answers.append(cur.fetchall()[0])


        query9 = """
        SELECT COUNT(*) AS num_acceptances
        FROM applicants
        WHERE status = 'Accepted'
        AND EXTRACT(YEAR FROM date_added) = 2026
        AND degree = 'PhD'
        AND llm_generated_program = 'Computer Science'
        AND llm_generated_university IN ('Georgetown University', 'MIT', 'Stanford University', 'Carnegie Mellon University');
        """
        
        cur.execute(query9)
        answers.append(cur.fetchall()[0])
        #Provides number of unique programs in non-llm
        query10 = """
        SELECT 
            COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 1))) AS unique_programs,
            COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 2))) AS unique_universities
        FROM applicants;
        """
        
        cur.execute(query10)
        answers.append(cur.fetchall()[0])
        #Provides number of unique programs in llm
        query11 = """
        SELECT 
            COUNT(DISTINCT llm_generated_program) AS unique_programs,
            COUNT(DISTINCT llm_generated_university) AS unique_universities
        FROM applicants;
        """
        
        cur.execute(query11)
        answers.append(cur.fetchall()[0])
        # answers.append(cur.fetchall())
        print(answers)
        print("Table 'applicants' created successfully (psycopg3).")

conn = connect()
questions(conn)