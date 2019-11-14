import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import requests
from bs4 import BeautifulSoup
import os
import asyncio
import functools

# 整理图片存放根路径
img_root_path = '/home/sofar/下载/toread/'
os.makedirs(img_root_path, exist_ok=True)
#sexes = ['男', '女', '儿童']

# 根据人群和型号生成文件夹
# for i in sexes:
#os.makedirs(img_root_path , exist_ok=True)


def get_jd_url( keyword, price, sql_params, row):
    g_url = None
    g_img = None
    url = 'https://search.jd.com/Search?keyword={}&enc=utf-8'.format(keyword)
    headers = {'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0',
               'accept-language': 'zh-CN,en-US;q=0.7,en;q=0.3'}
    resp = requests.get(url, headers={'user-agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0','accept-language':'zh-CN,en-US;q=0.7,en;q=0.3','Connection':'close'})
    resp.encoding='utf-8'
    #future = loop.run_in_executor(
        #None, functools.partial(requests.get, url, headers=headers))
    #resp = await future
    #resp.encoding = 'utf-8'
    html = resp.text
   # soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find('div', class_='check-error')
    if items is None:
        # 查找连接和图片
        gl_items = soup.find_all('li', class_='gl-item')
        if gl_items is not None and len(gl_items) > 0:
            gl_item = gl_items[0]
            img_src = gl_item.find('div', class_='p-img').find('img').attrs['source-data-lazy-img']
            href = gl_item.find('div', class_='p-img').find('a').attrs['href']
            #print((img_src, href))
             #保存图片
            r = requests.get('https:' + img_src)
            #ft = loop.run_in_executor(None, functools.partial(requests.get, 'https:' + img_src, headers=headers))
           # r = await ft
            file = img_root_path + str(price) + '+' + keyword + '.jpg'
            with open(file, 'wb') as f:
                f.write(r.content)
            g_url, g_img = 'https:' + href, 'https:' + img_src
    sql_params.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                       row[7], row[8], row[9], price, row[11], row[12], sex, g_url, g_img))


df = pd.DataFrame(pd.read_excel("/home/sofar/Works/VSCodeProjects/python-demo/toread/11-6.xls",
                                sheet_name='Sheet1', na_values='-1', keep_default_na=False))
conn = psycopg2.connect(database="postgres", user="postgres",
                        password="1234", host="localhost", port="5432")

#插入商品信息表
sql = 'insert into t_goods(id, level1,level2,level3,code,title,tag_price,g_size,g_storage,rebate,price,legend,sn,sex, url,img) values  %s'
sql_params = []
i = 1
#loop = asyncio.get_event_loop()
tasks = []
with conn:
    with conn.cursor() as cur:
        for row in df.itertuples(name="RowData"):
            title = row[5]
            sex = '通用'
            if title.find('女式') != -1:
                sex = '女'
            elif title.find('男式') != -1:
                sex = '男'
            elif title.find('童') != -1:
                sex = '儿童'
            code = row[4]
            get_jd_url(code, float('%.1f' % row[10]), sql_params, row)
            i = i + 1

            print('第 '+str(i)+'条处理完成' + ',code=' + row[4])
        #loop.run_until_complete(asyncio.wait(tasks))
        #execute_values(cur, sql, sql_params)
        print("数据处理完成")
        loop.close()
conn.close()
