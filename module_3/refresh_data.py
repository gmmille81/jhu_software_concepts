import subprocess
from module_2.scrape import scrape_data
from module_2.clean import clean_data
from update_data import insert_applicants_from_json_batch
import psycopg

def get_newest_p():
    with psycopg.connect(
    dbname="applicant_data",
    user="postgres",
    password="abc123",
    host="127.0.0.1",
    port=5432
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(p_id) FROM applicants;")
            max_p_id = cur.fetchone()[0]

    max_p_id = int(max_p_id) if max_p_id is not None else None
    return max_p_id

def update_db():
    print(get_newest_p())
    new_data = scrape_data(1)
    #print(new_data)
    new_data_cleaned = clean_data(new_data)
    newest_site_p = int(new_data_cleaned[0]['url'].split('/')[-1])
    print(newest_site_p)
    num_data_needed = newest_site_p-get_newest_p()
    if num_data_needed != 0:
        new_data = scrape_data(num_data_needed)
        new_data_cleaned = clean_data(new_data)
        #print(len(new_data_cleaned))
        flag = insert_applicants_from_json_batch(new_data_cleaned)
        print(flag)
        print(get_newest_p())
        return 0
    else: 
        return 1
