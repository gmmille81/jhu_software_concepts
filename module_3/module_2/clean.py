from scrape import scrape_data,save_data
import urllib3
from bs4 import BeautifulSoup
import json
import re
import certifi

#opens json file and returns dict
def load_data(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    return data

def clean_data(data):
    master_dict = {}
    #iterate through python dict by referencing keys
    for key in data.keys():
        master_dict[key] = {}
        master_dict[key]['university'] = data[key][0].replace('\n','')
        # noticed degree type and program major were in the same entry, separated by \n
        phd_masters_arr = data[key][1].split('\n\n\n\n')
        #extracts out the program major from split array and extracts the degree type if available
        if len(phd_masters_arr) > 1:
            program_major = phd_masters_arr[0].replace('\n','')
            master_dict[key]['program-major'] = program_major
            phd_masters_end = phd_masters_arr[1].find('\n')
            master_dict[key]['Degree'] = phd_masters_arr[1][0:phd_masters_end]
        else:
            print(key)
            master_dict[key]['Degree'] = ''

        #for each data point, uses string functions to extract data
        master_dict[key]['date_added'] = data[key][2].replace('\t','').replace('\n','')
        decision = data[key][3].replace('\t','').replace('\n','').replace(' ','').split('on')[0]
        master_dict[key]['status'] = decision
        master_dict[key]['url'] = data[key][5]
        #if there is a comments field, pull it
        try:
            master_dict[key]['comments'] = data[key][7].replace('\r','').replace('\n','')
        except:
            master_dict[key]['comments'] = ''
        #init fields that may be blank
        master_dict[key]['term'] = ''
        master_dict[key]['US/International'] = ''
        master_dict[key]['GRE Score'] = ''
        master_dict[key]['GRE V Score'] = ''
        master_dict[key]['GPA'] = ''
        master_dict[key]['GRE AW'] = ''
        extra_data = data[key][6].replace('\n','').replace('\t','').replace(' ','')
        #regex pattern to extract the semester
        SEMESTER_PATTERN = re.compile(r"(Fall|Spring|Summer|Winter)\s*(20\d{2})",re.IGNORECASE)
        sem_match = SEMESTER_PATTERN.search(extra_data)
        #if semester found, replace blank
        if sem_match:
            semester, year = sem_match.groups()
            master_dict[key]['term'] = semester+' ' +year
        #regex to extract gpa
        GPA_PATTERN = re.compile(r"GPA[:\s]*([0-4]\.\d{1,2})",re.IGNORECASE)
        gpa_match = GPA_PATTERN.search(extra_data)
        if gpa_match:
            gpa = gpa_match.group(1)
            master_dict[key]['GPA'] = gpa

        #regex to extract gre
        gre_total = re.search(r"GRE(\d{3})", extra_data)
        if gre_total:
            master_dict[key]['GRE Score'] = gre_total.group(1)
        #regex to extract gre v
        gre_v = re.search(r"GREV(\d{3})", extra_data)
        if gre_v:
            master_dict[key]['GRE V Score'] = gre_v.group(1)
        #regex to extract gre aw
        gre_aw = re.search(r"GREAW(\d(?:\.\d{1,2})?)", extra_data)
        if gre_aw:
            master_dict[key]['GRE AW Score'] = gre_aw.group(1)
        #regex to extract nation of origin
        nation = re.search(r"(American|International)", extra_data)
        
        if nation:
            master_dict[key]['US/International'] = nation.group(1)
        else:
            print('No Nation',key)


        master_dict[key]['program'] = master_dict[key]['program-major'] + ', ' + master_dict[key]['university']
        master_arr = []
        for key in master_dict.keys():
            master_arr.append(master_dict[key])
    return master_arr

#calls scrape to pull the number of entries specified
data = scrape_data(35000)
#save data to json file
save_data(data,'jhu_software_concepts/module_2/data4.json')
#open json file and load as dict
data = load_data('jhu_software_concepts/module_2/data4.json')
#clean the data
data_dict = clean_data(data)
#output the cleaned data for later processing for app.py
save_data(data_dict,'jhu_software_concepts/module_2/applicant_data.json')
