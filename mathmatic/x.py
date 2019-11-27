
import requests
from bs4 import BeautifulSoup

def get_jd_url():
    url = 'https://pds-geosciences.wustl.edu/lro/lro-l-lola-3-rdr-v1/lrolol_1xxx/DATA/LOLA_GDR/cylindrical/jp2/'
    headers = {'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0',
               'accept-language': 'zh-CN,en-US;q=0.7,en;q=0.3'}
    req = requests.get(url, headers=headers)
    req.encoding='utf-8'
    
    #soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    soup = BeautifulSoup(req.text, 'html.parser')
    items = soup.find_all('a')
    for item in items:
        href = item.attrs['href']
        if 'ldem_1024' in href:
            print('https://pds-geosciences.wustl.edu/' + href)

get_jd_url()