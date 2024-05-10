
# Python program to demonstrate 
# selenium 

import os
import datetime
import random
import sys
import json
import concurrent.futures

import pandas as pd
from time import sleep
from retry import retry

# import webdriver 
from DrissionPage import WebPage, ChromiumOptions  
from DrissionPage.common import Keys

from sqlalchemy import create_engine, Column, Integer, BigInteger, Date, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector

username = '惠多星旗舰店:空易'
password = '123456hdx'
db = 'xtt_002'
port = '9223'

# username = '蜡笔派家居旗舰店:华'
# password = 'ch123456'
# db = 'xtt'
# port = '9222'

def find_categories(cat_id, connection, initial_parent_id=None):
    query = text("""
                    WITH RECURSIVE CategoryAncestors AS (
                    SELECT cid, name, parent_id
                    FROM biz_categories
                    WHERE cid = :cat_id -- Replace :cat_id with the ID of the category you're interested in
                    
                    UNION ALL
                    
                    SELECT c.cid, c.name, c.parent_id
                    FROM biz_categories c
                    INNER JOIN CategoryAncestors ca ON c.cid = ca.parent_id
                    )
                    SELECT * FROM CategoryAncestors;
                """)
 
    result = connection.execute(query, {"cat_id": cat_id})
    categories = []

    for row in result:
        categories.append({'cid': row.cid, 'name': row.name, 'parent_id': row.parent_id})

    return categories

def get_product_ids(connection):
    query = text("""
                    SELECT product_id
                    FROM biz_product
                    WHERE category_lv1 is NULL;
                """)
 
    result = connection.execute(query)
    pids = []

    for row in result:
        pids.append(row.product_id)

    return pids

# Define the connection string
default_file_path = r'E:\sycm\\'
DATABASE_URI = f'mysql+pymysql://root:pwd123OK@47.109.94.69/{db}'
engine = create_engine(DATABASE_URI, echo=False)

# Connect to the database and perform the query
with engine.connect() as connection:

    pids = get_product_ids(connection=connection)
    print("found", len(pids), "pids")

    co = ChromiumOptions()
    co.set_address(f'127.0.0.1:{port}')
    
    page = WebPage()
    page.get("https://loginmyseller.taobao.com/?from=taobaoindex&f=top&style=&sub=true&redirect_url=https%3A%2F%2Fmyseller.taobao.com")
    page.wait(1)

    user_name = username
    pass_word = password
    page("#fm-login-id").input(user_name)
    page("#fm-login-password").input(pass_word)
    # 这里可以做一个判断，用于新老登录界面的异常捕获
    iframe = page("#alibaba-login-box")
    res = iframe(".fm-button fm-submit password-login").click()
    page.wait(2)

    url = "https://myseller.taobao.com/home.htm/SellManage/all?current=1&pageSize=20"
    page.get(url)
    page.wait(10)

    page.listen.start("/h5/mtop.tmall.sell.pc.manage.async/1.0/")
    index = 0
    end_index = False
    table = 'biz_product'
    while index < len(pids):
        pidstring = pids[index]
        for i in range(0, 10):
            index += 1
            if index >= len(pids):
                break
            pidstring += "," + pids[index]

        page.ele('#queryItemId').input(pidstring + '\n')
        page.wait(10)
        index += 1

        while True:  
            res = page.listen.wait(count=1)
            #print(res.response.body)
            if res.response.body is None:
                break

            jsonobj = json.loads(res.response.body['data']['result'])
            if 'data' in jsonobj and 'table' in jsonobj['data']:
                ds_data = jsonobj['data']['table']['dataSource']
                with engine.connect() as connection:
                    for item in ds_data:
                        product_id = item['itemId']
                        print(product_id, item['catId'])
                        categories  = find_categories(item['catId'], connection)
                        print(categories)

                        if len(categories) < 2:
                            continue
                        
                        if len(categories) == 2:
                            cat_name = categories[0]['name']
                            cat1_name = categories[1]['name']
                            transfersql = f"""update {table} set category_name = '{cat_name}', category_lv1 = '{cat1_name}', category_lv2 = '{cat_name}'  
                                            where product_id = '{product_id}'
                                            """
                            print(transfersql)
                        else:
                            cat_name = categories[0]['name']
                            cat2_name = categories[1]['name']
                            cat1_name = categories[2]['name']
                            transfersql = f"""update {table} set category_name = '{cat_name}', category_lv1 = '{cat1_name}', category_lv2 = '{cat2_name}', category_lv3 = '{cat_name}'  
                                            where product_id = '{product_id}'
                                            """
                            print(transfersql)
                        result = connection.execute(text(transfersql))
                    connection.commit()
                    break
