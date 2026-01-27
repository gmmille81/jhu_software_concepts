import urllib3
from bs4 import BeautifulSoup
import json
import re
def scrape_data():
    http = urllib3.PoolManager()
    page_data = {}
    counter = 0
    page = 1
    while True:
        print(page)
        if len(page_data) >= 2000:
            break
        else:
            base_url = 'https://www.thegradcafe.com/survey/?page='
            url = base_url + str(page)
            response = http.request('GET', url)
            # print(response.status)
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
                d.append(td_objs)

            for i in range(1,len(d)):
                if len(d[i]) == 1:
                    page_data[counter-1].append(d[i][0])
                else:
                    page_data[counter] = d[i]
                    counter+=1
            page+=1
    return page_data

def save_data(data_arr,filename):
    print(len(data_arr))
    #json_data = json.dumps(data_arr)
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data_arr, f)
def load_data(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    return data

def clean_data(data):
    master_dict = {}
    for key in data.keys():
        master_dict[key] = {}
        master_dict[key]['university'] = data[key][0].replace('\n','')
        master_dict[key]['phd_masters'] = re.findall("\b(ph\.?d\.?|masters?('s)?)\b",data[key][1])
        # program_and_type = data[key][1].replace('\n','').split('PhD')
        # program_and_type = program_and_type.split('Masters')
        # master_dict[key]['program'] = program_and_type[0]
        # if len(program_and_type) == 1:
        #     master_dict[key]['phd_masters'] = ''
        # else:
        #     master_dict[key]['phd_masters'] = program_and_type[1]
    return master_dict

#data = scrape_data()
#save_data(data,'jhu_software_concepts/module_2/data.json')

data = load_data('jhu_software_concepts/module_2/data.json')
data_dict = clean_data(data)
print(data_dict)

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