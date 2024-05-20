"""
    内容渠道效果
"""
import random
import json
import os
import re
import shutil
import pandas as pd
import calendar
import logging
import concurrent.futures
from .base_action import base_action
from io import BytesIO
from datetime import datetime, timedelta
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions
from sqlalchemy import create_engine, text, Table, Column, String, Date, Float, MetaData, DECIMAL
from sqlalchemy.dialects.mysql import DOUBLE

class sql_action:
    
    def __init__(self, config) -> None:
        
        self.base = base_action()
        self.config = config
        
        # 修改 存储数据的 excel 名称 [根据需要修改的参数]
        self.task_name = "[生意参谋]&&[sql_action]"
        
        # 获取配置文件中该任务的配置
        self.get_config_bool = self.base.get_configs(self.__class__.__name__, config_name=self.config)
        
        # 获取配置文件中公用配置的对象
        self.base_config = self.base.get_configs_return_obj('base_config', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        
        # 检查并拿到 pageTab [根据需要修改的参数]
        self.check_url = self.base.config_obj['check_url']
        
        self.shop_id = self.base_config['shop_id']
        self.shop_name = self.base_config['shop_name']
        self.table_name = self.__class__.__name__
        
        # [根据需要修改的参数]
        self.primary_key = ['statistic_date']
        # [根据需要修改的参数]
        self.add_col = {}
        self.page = None
    
    def visit_sycm(self):
        
        try:
        
            if self.get_config_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 读取配置文件出错，请检查。')
                return False
            
            if self.create_folder_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 创建存储文件出错，请检查。')
                return False

            port = self.base.config_obj.get('port', self.base_config['port'])    
            
            co = self.base.set_ChromiumOptions()

            co.set_address(f'127.0.0.1:{port}')

            page = WebPage(chromium_options=co)
            
            res = page.get_tab(url=self.check_url)
            
            if res is not None:
                # 已访问
                self.page = res
            else:
                page.get(self.base.config_obj['url'])
                self.page = page

            return {
                'mark': True,
                'msg': f'# {self.shop_name}{self.task_name} <info> 访问成功！'
            }
        
        except Exception as e:
            
            print(f'# {self.shop_name}{self.task_name} <error> 访问失败！')
            print(f'# {self.shop_name}{self.task_name} <error> {str(e)}')
            
            return {
                'mark': False,
                'msg': f'# {self.shop_name}{self.task_name} <info> 访问失败！'
            }   

    def insert_shop_weight(self):
        
        mark = False
        
        try:
        
            engine = self.base.create_engine()
            
            sql1 = text("""
                            INSERT INTO biz_shop_weight
                            (shop_id, shop_name, visitor_count_weight, avg_stay_duration_weight, add_to_cart_rate_weight, paid_buyer_count_weight, paid_quantity_weight, paid_amount_weight, payment_conversion_rate_weight, visitor_value_weight, search_payment_conversion_rate_weight, detail_bounce_rate_weight, seven_day_gmv_threshold)
                            VALUES(:shop_id, :shop_name, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1000);                        
                        """)
            
            arr = [sql1]
            
            with engine.connect() as conn:
                
                for item in arr:
                    
                    print(f"{self.base.config_obj['shop_name']}: 表 shop_weight 开始执行: {item}")
                    result = conn.execute(item, {'shop_id': self.shop_id, 'shop_name': self.shop_name})
                    print(f"{self.base.config_obj['shop_name']}: 表 shop_weight 执行成功: {item}")
                
                conn.commit()
            
            mark = True
                
        except Exception as e:
            print(f'insert_shop_weight 执行出错, error: {str(e)}')
            self.log_([f"error/shs/【{self.get_date_time()}】: calc_prepallet 执行出错!", f'{str(e)}'])
        
        return mark

    def find_categories(self, cat_id, conn):
        query = text("""
                        WITH RECURSIVE CategoryAncestors AS (
                            SELECT cid, name, parent_id
                            FROM xtt.biz_categories
                            WHERE cid = :cat_id -- Replace :cat_id with the ID of the category you're interested in
                            
                            UNION ALL
                            
                            SELECT c.cid, c.name, c.parent_id
                            FROM xtt.biz_categories c
                            INNER JOIN CategoryAncestors ca ON c.cid = ca.parent_id
                        )
                        SELECT * FROM CategoryAncestors;
                    """)
    
        result = conn.execute(query, {"cat_id": cat_id})
        categories = []

        for row in result:
            categories.append({'cid': row.cid, 'name': row.name, 'parent_id': row.parent_id})

        return categories

    def get_product_ids(self, conn):
        query = text("""
                        SELECT product_id
                        FROM biz_product
                        WHERE category_lv1 is NULL;
                    """)
    
        result = conn.execute(query)
        pids = []

        for row in result:
            pids.append(row.product_id)

        return pids

    def fill_product_category(self):
        
        url = "https://myseller.taobao.com/home.htm/SellManage/all?current=1&pageSize=20"
        self.page.get(url)
        self.page.wait(10)

        self.page.listen.start("/h5/mtop.tmall.sell.pc.manage.async/1.0/")
        index = 0
        end_index = False
        table = 'biz_product'
        engine = self.base.create_engine()
        conn = engine.connect()
        pids = self.get_product_ids(conn)
        print("found", len(pids), "pids")

        
        while index < len(pids):
            pidstring = pids[index]
            for i in range(0, 20):
                index += 1
                if index >= len(pids):
                    break
                pidstring += "," + pids[index]

            self.page.ele('#queryItemId').clear()
            self.page.ele('#queryItemId').input(pidstring + '\n')
            self.page.wait(10)
            index += 1

            while True:  
                res = self.page.listen.wait(count=1)
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
                            categories  = self.find_categories(item['catId'], connection)
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


    def get_campaigns(self, href, bid_type = None):
        self.page.get(href)
        self.page.wait(12)
      
        campaigns = {}
        rows = self.page.s_ele("xpath: //tbody").s_eles("xpath: //tr")
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

        return campaigns

    def fill_bid_type(self):
        mark = True
        campaignss={}
        page_list = ['search', 'display', 'item', 'shop', 'content', 'customer', 'activity']
        for page_name in page_list:
            campaigns = self.get_campaigns('https://one.alimama.com/index.html#!/manage/' + page_name + '?offset=0&pageSize=100')
            campaignss = campaignss | campaigns
            res = self.page.ele('xpath: //span[@title="全站推广"]', timeout=2)
            if not res:
                print(f"{self.base.config_obj['shop_name']}: 没有全站推广！")
                mark = False
                
        if mark:
            special_page_list = ['onesite']
            for page_name in special_page_list:
                campaigns = self.get_campaigns('https://one.alimama.com/index.html#!/manage/' + page_name + '?offset=0&pageSize=100', bid_type="控投产比投放")
                campaignss = campaignss | campaigns

        keys = ['campaign_id', 'bid_type']
        engine = self.base.create_engine()
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
                
    def do_action(self): 

        # only need to do once        
        # self.insert_shop_weight()
        
        # fille biz_bid_type table
        # self.fill_bid_type()
        
        # self.fill_product_category()
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天':
            datetime_ = self.base.get_before_day_datetime()
            start_date = datetime_
            end_date = datetime_
        else:
            start_date = self.base.config_obj["start_date"]
            end_date = self.base.config_obj["end_date"]
        
        res = self.base.calc(start_date_=start_date, end_date_=end_date)
        
        if res is False:
            return
        
        res = self.base.calc_prepallet()
        
        if res is False:
            return
        
        # 1. 删除 biz_pallet_product 指定日期的信息
        # 2. 从视图 v_pallet_product 写入相关数据
        
        res = self.base.insert_biz_pallet_product_from_v_pallet_product()
        
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 sql action')

    def log(self, msg, type_='error'):
        # 配置日志输出的格式
        logging.basicConfig(
            filename=f"{self.base.logger_path}/[{type_}]_[{datetime.now().strftime('%Y-%m-%d')}].log",
            format='%(asctime)s - %(levelname)s - %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO  # 设置日志级别为 INFO
        )
        # 记录日志信息
        if type_ == 'debug':
            logging.debug(msg)
        elif type_ == 'info':
            logging.info(msg)
        elif type_ == 'warning':
            logging.warning(msg)
        elif type_ == 'error':
            logging.error(msg)
        else:
            logging.critical(msg)
       
    def run(self):
        
        res = self.visit_sycm()
        
        if res is False:
            return
        
        res = self.do_action()
        
        if res is False:
            return       
    
    def test(self):
        
        self.visit_sycm()
        # self.base.login_sycm(task_name=self.task_name)
        self.get_json_data()
    
if __name__ == "__main__":
    test = sql_action()
    test.run()