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
        question = 'How many entries do you have in your database who have applied for Fall 2026?'
        query1 = """
        SELECT COUNT(p_id)
        FROM applicants
        WHERE term = 'Fall 2026';
        """
        cur.execute(query1)
        answers.append([question,cur.fetchall()[0][0]])
        question = 'What percentage of entries are from international students (not American or Other) (to two decimal places)?'
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
        answers.append([question,cur.fetchall()[0][0]])
        question = 'What is the average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics?'
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
        print(avg_gpa,avg_gre, avg_gre_v, avg_gre_aw)
        ret_str = "GPA:" + str(avg_gpa) + " GRE:" + str(avg_gre) +  " GRE V:" + str(avg_gre_v) + " GRE AW:" + str(avg_gre_aw)
        answers.append([question,ret_str])
        question = 'What is their average GPA of American students in Fall 2026?'
        query4 = """
        SELECT
            ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
        FROM applicants
        WHERE us_or_international = 'American'
        AND term = 'Fall 2026';
        """
        cur.execute(query4)
        answers.append([question,cur.fetchall()[0][0]])
        question = 'What percent of entries for Fall 2026 are Acceptances (to two decimal places)?'
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
        answers.append([question,cur.fetchall()[0][0]])
        question = 'What is the average GPA of applicants who applied for Fall 2026 who are Acceptances?'
        query6 = """
        SELECT
            ROUND(AVG(gpa)::numeric, 2) AS avg_gpa
        FROM applicants
        WHERE status = 'Accepted'
        AND term = 'Fall 2026';
        """
        cur.execute(query6)
        answers.append([question,cur.fetchall()[0][0]])
        question = 'How many entries are from applicants who applied to JHU for a masters degrees in Computer Science?'
        query7 = """
        SELECT COUNT(p_id)
        FROM applicants
        WHERE degree = 'Masters'
        AND program = 'Computer Science, Johns Hopkins University';
        """
        cur.execute(query7)
        answers.append([question,cur.fetchall()[0][0]])
        question = 'How many entries from 2026 are acceptances from applicants who applied to Georgetown University, MIT, Stanford University, or Carnegie Mellon University for a PhD in Computer Science?'
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
        answers.append([question,cur.fetchall()[0][0]])
        question = 'Do you numbers for question 8 change if you use LLM Generated Fields (rather than your downloaded fields)?'

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
        answers.append([question,cur.fetchall()[0][0]])
        #Provides number of unique programs in non-llm
        question = 'How many unique program names and university names are in the data set?'
        query10 = """
        SELECT 
            COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 1))) AS unique_programs,
            COUNT(DISTINCT TRIM(SPLIT_PART(program, ',', 2))) AS unique_universities
        FROM applicants;
        """
        
        cur.execute(query10)
        ans = cur.fetchall()[0]
        print(ans)
        ret_str = 'Unique Programs: ' + str(ans[0]) + ' Unique Universities: ' + str(ans[1])
        answers.append([question,ret_str])
        #Provides number of unique programs in llm
        question = 'How many unique llm-generated program names and university names are in the data set?'
        query11 = """
        SELECT 
            COUNT(DISTINCT llm_generated_program) AS unique_programs,
            COUNT(DISTINCT llm_generated_university) AS unique_universities
        FROM applicants;
        """
        
        cur.execute(query11)
        ans = cur.fetchall()[0]
        print(ans)
        ret_str = 'Unique Programs: ' + str(ans[0]) + ' Unique Universities: ' + str(ans[1])
        answers.append([question,ret_str])
        print(answers)
        cur.close()
        conn.close()
        return answers
conn = connect()
questions(conn)