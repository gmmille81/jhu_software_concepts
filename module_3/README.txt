## Greg Miller ##
## JHU Software Concepts - Module 3 (Database Queries Assignment Experiment) - Feb 7 2026 ##
Approach: 
- Configured my system to support PostgreSQL and enabled psycopg. This required some back and forth with my ai agent
- Created load_data.py to create a database with the data from module 2
- Created query_data.py to answer each of the questions in the assignment. This took some research on SQL functions and some support from ai agent to determine the best way to answer questions
- Began creating a new function that would update the applicants table based on the newest information from gradcafe (refresh_data.py). 
    - This was one of the biggest challenges of the assignment. It took awhile to think of the best way to leverage my existing code to pull the right number of entries. 
    - I ultimately decided to calculate how outdated my database was by changing my primary key (p_id) from being a SERIAL KEY to an integer based on the urls. 
        - I noticed that gradcafe was essentially already providing a unique identifier to use in the urls that increment for every record. Therefore, using this identifier, I could figure out how many records were missing.
- I then created a new function insert_applicants_from_json_batch(data) (originally in load_data.py but had to move to own file update_data)
    - This function would accept the new data scraped from refresh_data and add it to the applicants table
- Once the underlying functions worked, I began creating a Flask page to display the data. I leveraged my files from module_1 and then tailored them to only be used for one page (removed blueprints)
    - Another one of the challenges I had with this assignment was adding in the logic to app.py to tell the user that Pull Data was already running. This took a lot of back and forth with my ai agent
    - Once core functionality was correct, I worked with my AI agent to make the html look better and update app.py to make the site more user-friendly.
    - Provided code to ai agent to clean up comments and reviewed output

Prior to running (env-specific steps): 
brew install postgresql
brew services start postgresql
initdb /usr/local/var/postgresql@14
Did the following: 
psql -d postgres
postgres=# CREATE DATABASE gregmiller;
CREATE DATABASE
postgres=# CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'abc123';
CREATE ROLE

To Run:
- Once PostgreSQL is working in your environment, run "python3 load_data.py". 
    - load_data.py will run two functions: 
    - create_table will connect to psycopg, create a db named applicant_data with a table named 'applicants' with the following columns:
        -       p_id BIGINT PRIMARY KEY,
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
    - bulk_insert_json(path) will populate the empty table with all of the original module_2 json from jhu_software_concepts/module_3/module_2/llm_extend_applicant_data.json
- Once load_data.py is complete, run "python3 query_data.py"
    - query_data will first run connect(), which established connection with applicant_data DB
    - It will then run questions(), which will create a table called answers_table to store the answers of the queries.
        - It will then run queries on 'applicants' table and store their results in the new answers_table
- Now that there is a data baseline on the local system, run "python3 app.py"
    - This will host a Flask webpage on 127.0.0.1 port 8080 that will display results and allow the user to update the data and/or query results. 
