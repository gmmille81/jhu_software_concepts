import urllib3
from bs4 import BeautifulSoup
import json
import re
import certifi
#pulls data from gradcafe and puts individual fields into python dict
def scrape_data(record_count):
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    page_data = {}
    counter = 0
    page = 1
    while True:
        print(page)
        if len(page_data) >= record_count:
            break
        else:
            base_url = 'https://www.thegradcafe.com/survey/?page='
            result_url = 'https://www.thegradcafe.com'
            url = base_url + str(page)
            response = http.request('GET', url)
            html = response.data.decode('utf-8')

        #print(url)

        #response = urlopen(base_url)
        #html = response.read().decode("utf-8")
            soup = BeautifulSoup(html, "html.parser")
            fields = soup.find_all('tr')
            d = []
            for i in range(0,len(fields)):
                td_obj = fields[i].find_all('td')
                td_objs = [k.get_text() for k in td_obj]
                link_str_start = str(fields[i]).split('<a href="')
                if len(link_str_start) > 1:
                    link_str_end = link_str_start[1].find('">')
                    link = result_url + link_str_start[1][0:link_str_end]
                    td_objs.append(link)
                #links = re.findall('<a href="result/',str(fields[i]))
                #print(str(fields[i]))
                d.append(td_objs)

            for i in range(1,len(d)):
                if len(d[i]) == 1:
                    page_data[counter-1].append(d[i][0])
                else:
                    page_data[counter] = d[i]
                    counter+=1
            page+=1
            # print(response.status)

    return page_data
#saves inputted python dict into json file in module2 folder
def save_data(data_arr,filename):
    #json_data = json.dumps(data_arr)
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data_arr, f)





# print(master)
# print(len(master))


#print(json_data)
# print(json_data)
# print(fields)
# fields = soup.find_all('dd')
# data = [i.get_text().replace('\t','').replace('\n','') for i in fields]
# decision_date = soup.find_all('time')
# decision_dates = [i.get_text() for i in decision_date]
# data.append(decision_dates[0])
# print(data)
# print(decision_dates)