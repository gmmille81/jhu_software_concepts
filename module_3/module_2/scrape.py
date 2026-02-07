import urllib3
from bs4 import BeautifulSoup
import json
import re
import certifi
#pulls data from gradcafe and puts individual fields into python dict
def scrape_data(record_count):
    #urllib3 requires pool manager - different from lecture
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    page_data = {}
    counter = 0
    page = 1
    #loops until the specified number of record counter is met
    while True:
        print(page)
        if len(page_data) >= record_count:
            break
        else:
            #set url to the survey pages on grad cafe, allows to pull 20 records per request.
            base_url = 'https://www.thegradcafe.com/survey/?page='
            result_url = 'https://www.thegradcafe.com'
            url = base_url + str(page)
            response = http.request('GET', url)
            html = response.data.decode('utf-8')

            #find all tr objects which include the of rows data
            soup = BeautifulSoup(html, "html.parser")
            fields = soup.find_all('tr')
            d = []
            #for each row, pull out the data in the tds
            for i in range(0,len(fields)):
                td_obj = fields[i].find_all('td')
                td_objs = [k.get_text() for k in td_obj]
                #find the object for the link to the specific record's page and append to base url
                link_str_start = str(fields[i]).split('<a href="')
                if len(link_str_start) > 1:
                    link_str_end = link_str_start[1].find('">')
                    link = result_url + link_str_start[1][0:link_str_end]
                    td_objs.append(link)
                d.append(td_objs)
            #most records span multiple rows, logic here will keep data from same record over multiple rows together
            for i in range(1,len(d)):
                if len(d[i]) == 1:
                    page_data[counter-1].append(d[i][0])
                else:
                    page_data[counter] = d[i]
                    counter+=1
            page+=1
    return page_data
#saves inputted python dict into json file in module2 folder
def save_data(data_arr,filename):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data_arr, f)