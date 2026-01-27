from urllib.request import urlopen
from urllib import parse
from bs4 import BeautifulSoup

base_url = 'https://www.thegradcafe.com/result/'
url = parse.urljoin(base_url,'30005')
#print(url)

response = urlopen(url)
html = response.read().decode("utf-8")
soup = BeautifulSoup(html, "html.parser")

fields = soup.find_all('dd')
data = [i.get_text().replace('\t','').replace('\n','') for i in fields]
print(data)