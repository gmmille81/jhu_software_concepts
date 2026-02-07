import subprocess
from module_2.scrape import scrape_data
from module_2.clean import clean_data
from load_data import insert_applicant_from_json
def update_db():
    flag = 0
    while flag == 0:
        new_data = scrape_data(20)
        new_data_cleaned = clean_data(new_data)
        for data in new_data_cleaned:
            flag = insert_applicant_from_json(data)
            if flag == 1:
                break

update_db()