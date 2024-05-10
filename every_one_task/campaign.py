# Python program to demonstrate 
# type: ignore

import os
import sys
import urllib.request

from time import sleep
from retry import retry

# import webdriver 
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions
from DrissionPage.common import Actions
from DrissionPage.common import Keys

from sqlalchemy import create_engine,text


default_file_path = r'E:\sycm\\'
username = '惠多星旗舰店:空易'
password = '123456hdx'
db = 'xtt_002'
port = '9223'

# username = '蜡笔派家居旗舰店:华'
# password = 'ch123456'
# db = 'xtt'
# port = '9222'

#@retry(Exception, tries=3, delay=2)
def login(href):
        page.get(href)
        page.wait(12)

        try:
            iframe = page('xpath: //iframe[contains(@src, "login.taobao.com/member")]')
            id = iframe("#fm-login-id")
            id.input(username)
            page.wait(2)
            password = iframe("#fm-login-password")
            password.input(password)
            page.wait(1)
            link = iframe(".fm-button fm-submit password-login")
            link.click()
            page.wait(2)
        except:
            # do nothing
            return

def get_campaigns(href, bid_type = None):
        page.get(href)
        page.wait(12)
      
        campaigns = {}
        rows = page.s_ele("xpath: //tbody").s_eles("xpath: //tr")
        for i in range(0, len(rows) - 2, 2):
            link = rows[i].ele('xpath: //a').href
            cid = link[link.find('campaignId=') + 11:]
            
            div_text = rows[i].text
            id_index = div_text.find('ID：')
            if  id_index > 0:
                cid = div_text[id_index + 3: div_text.find('\n', id_index)]
            
            if bid_type is None:
                if div_text.find("套餐包") >= 0:
                    bid_type = "套餐包"
                elif div_text.find("最大化拿量") >= 0:
                    bid_type = "最大化拿量"
                elif div_text.find("最大化拿点击") >= 0:
                    bid_type = "最大化拿点击"
                elif div_text.find("最大化拿加购") >= 0:
                    bid_type = "最大化拿加购"
                elif div_text.find("控成本投放") >= 0:
                    bid_type = "控成本投放"
                elif div_text.find("控成本点击") >= 0:
                    bid_type = "n控成本点击"
                elif div_text.find("控成本加购") >= 0:
                    bid_type = "控成本加购"
                elif div_text.find("控投产比投放") >= 0:
                    bid_type = "控投产比投放"
                elif div_text.find("手动出价") >= 0:
                    bid_type = "手动出价"

            campaigns[cid] = bid_type

        sleep(3)
        return campaigns

def update_table(table, campaignss, engine):
    try:
        with engine.connect() as connection:
            for campaign_id in campaignss:
                bid_type = campaignss[campaign_id]
                transfersql = f"""update {table} set bid_type = '{bid_type}' 
                                where plan_id = '{campaign_id}'
                                """
                print(transfersql)
                result = connection.execute(text(transfersql))

                print(f"Updated {result.rowcount} row(s) for campaign_id {campaign_id} with bid_type {bid_type}")
                connection.commit()
    except Exception as e:
        print(f"Error updating database: {e}")

co = ChromiumOptions()
co.set_address(f'127.0.0.1:{port}')
page = WebPage(chromium_options=co)  

login("https://one.alimama.com/index.html")
sleep(2)
#https://one.alimama.com/index.html#!/manage/display?offset=0&pageSize=100
campaignss={}
page_list = ['search', 'display', 'item', 'shop', 'content', 'customer', 'activity']
for page_name in page_list:
    campaigns = get_campaigns('https://one.alimama.com/index.html#!/manage/' + page_name + '?offset=0&pageSize=100')
    campaignss = campaignss | campaigns

# special_page_list = ['onesite']
# for page_name in special_page_list:
#     campaigns = get_campaigns('https://one.alimama.com/index.html#!/manage/' + page_name + '?offset=0&pageSize=100', bid_type="控投产比投放")
#     campaignss = campaignss | campaigns


keys = ['campaign_id', 'bid_type']
database_url = f'mysql+pymysql://root:pwd123OK@47.109.94.69/{db}'
# 创建数据库引擎
engine = create_engine(database_url)
conn = engine.connect()

table = 'biz_bid_type'
try:
    for key in campaignss:
        transfersql = f"""insert into {table} ({",".join(keys)}) 
                        values ('{key}', '{campaignss[key]}')
                        """
        print(transfersql)
        conn.execute(text(transfersql))
        conn.commit()
except Exception as ex:
    print(ex)
