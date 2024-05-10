"""
    sku 销售详情
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

class damopan_huopindongcha:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.config = config
        self.task_name = "[货品洞察]"
        self.get_config_bool = self.base.get_configs('damopan_huopindongcha', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        self.page = None
        # 数据库表名
        self.table_name = 'biz_product_damo'
        self.add_col = {}
        self.columns_to_drop = []

    
    def visit_damopan(self):
        
        if self.get_config_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [达摩盘][货品洞查] 读取配置文件出错，请检查。')
            return False
        
        if self.create_folder_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [达摩盘][货品洞查] 创建存储文件出错，请检查。')
            return False

        port = self.base.config_obj['port']    
        
        co = self.base.set_ChromiumOptions()

        co.set_address(f'127.0.0.1:{port}')

        page = WebPage(chromium_options=co)
        
        res = self.base.whether_the_url_exists_in_the_browser(page=page, url_str='sycm.taobao.com')
        
        if res['mark']:
            # 已访问
            pageTab = page.new_tab(self.base.config_obj['url'])
            self.page = pageTab
        else:
            page.get(self.base.config_obj['url'])
            self.page = page

        # page.get(self.base.config_obj['url'])
        
        # self.page = page
        
        # print('达摩盘访问成功！')
        
        self.page.wait(random.randint(1, 3))
        
        return True
    
    def get_json_data(self):
        
        self.page.change_mode('s')
        
        data_arr = []
        
        endDate = ''
        
        url = self.base.config_obj['second_level_url']
        
        end_date = self.base.config_obj['end_date']
        
        end_date_arr = end_date.split('-')
        
        year1 = int(end_date_arr[0])
        month1 = int(end_date_arr[1])
        day1 = int(end_date_arr[2])

        # 计算end_date 与 当天日期是否小于2， 小于2只能指定end_date
        today = self.base.get_before_day_datetime(tag='t')
        
        today_arr = today.split('-')
        
        year2 = int(today_arr[0])
        month2 = int(today_arr[1])
        day2 = int(today_arr[2])
        
        date1 = datetime(year1, month1, day1)
        date2 = datetime(year2, month2, day2)
        
        # 计算两个日期之间的差异
        difference = date2 - date1
        # 打印天数差异
        # print("两个日期之间的天数差是:", difference.days)
        # print(type(difference.days))
        
        if difference.days < 2:
            endDate = self.base.get_before_day_datetime(days_=2)
        else:
            endDate = end_date
        
        date_range = pd.date_range(self.base.config_obj['start_date'], endDate)
        
        # 重试
        retry_count = 1
        
        for date in date_range:
            
            data_arr.clear()
            
            date = date.strftime('%Y-%m-%d')
            
            pageNum = 1
            
            while True:
                
                print(f'{self.task_name}, 开始获取 {date} , 第 {pageNum} 页的数据！')
                
                url_ = self.base.new_url(dict_={'endDate': date, 'page': pageNum}, oldurl=url)
            
                # print(url_['url'])
            
                self.page.get(url_['url'])
                
                data = json.loads(self.page.raw_data)
                
                if data['info']['code'] != 0:
                    
                    if retry_count > 3:
                        print(f'{self.base.config_obj["shop_name"]}: <error> [达摩盘][货品洞查] 请求数据超时，请检查!')
                        self.log(msg='请求数据超时，请检查!')
                        break
                
                    print(f'{self.base.config_obj["shop_name"]}: <info> [达摩盘][货品洞查] 可能请求超时，3 秒后重试！')
                    retry_count += 1
                    self.page.wait(3)
                    continue
                
                if len(data['data']['list']) != 0:
                    
                    # 开始拿数据
                    for item in data['data']['list']:
                        
                        data_obj = {}
                        
                        data_obj['product_id'] = item.get('id', '00000000')
                        data_obj['product_name'] = item.get('name', '无名称')
                        data_obj['statistic_date'] = date
                        # 连带购买量 (可能需要计算)
                        data_obj['associated_purchase_quantity'] = item['indicators'][28]['value']
                        # 连带购买率 (可能需要计算)
                        data_obj['associated_purchase_rate'] = item['indicators'][29]['value']
                        # 连子购买叶子类目
                        data_obj['associated_leaf_category'] = item['indicators'][30]['value']
                        # 复购用户数
                        data_obj['repurchase_user_count'] = item['indicators'][31]['value']
                        # 复购率
                        data_obj['repurchase_rate'] = item['indicators'][32]['value']
                        
                        data_arr.append(data_obj)
                    
                    pageNum += 1

                    # self.page.wait(random.uniform(0.1, 0.2))
                    
                else:
                    print(f'{self.base.config_obj["shop_name"]}: <info> [达摩盘][货品洞查] {date} 所有页的数据获取完毕！')
                    break
        
            # 将这一批数据写入 excel
            res = self.base.pandas_insert_data(data_arr, f"{self.base.source_path}/[达摩盘]&&{self.task_name}&&{date}.xlsx")
            # print(res['msg'])
        
        return True
                         
    # 下载excel
    def down_load_excel(self):
        
        is_byte = False
        data = ''
        index = 5
        product_data = None
        count = 0
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False
        
        date_format = "%Y-%m-%d"
        
        # 切换模式
        self.page.change_mode('s')
        
        re_str = r"dateRange=(\d{4}-\d{2}-\d{2})\|(\d{4}-\d{2}-\d{2})"
        
        # 拿到链接, 处理为正确的链接
        url = self.base.config_obj["second_level_url"]
        date1_str = self.base.config_obj["start_date"]
        date2_str = self.base.config_obj["end_date"]
        
        date1 = datetime.strptime(date1_str, date_format)
        date2 = datetime.strptime(date2_str, date_format)

        # 计算要跑的次数, 如果超过30次及以上, 可以采用线程池. 暂时不做.
        days_difference = (date2 - date1).days
        
        if automatic_date:
            before_day = self.base.get_before_day_datetime()
            data_range = pd.date_range(before_day, before_day)
        else:     
            data_range = pd.date_range(date1_str, date2_str)
        
        for date in data_range:
            # 拿到商品ID
            product_data = self.base.get_item_id(date_=date)
            
            if product_data['mark'] is False:
                print('get_item_id(): product id 获取失败，请检查！')
                self.log(msg='get_item_id(): product id 获取失败，请检查！')
                return False
            
            date_ = date.strftime(date_format)
            print(f'查询到{date_}的在线商品个数：{len(product_data["result"])}')
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                
                futures = []
                
                for itemid in product_data['result']:
                    
                    future = executor.submit(self.get_itemid_download_excel, url, itemid, date_, index)
                    futures.append(future)
                
                    self.page.wait(random.uniform(0.2, 0.5))
                    
                    # count += 1
                    
                    # if count >= 5:
                    #     break
                    
                # 等待所有提交的任务完成
                concurrent.futures.wait(futures)

        print('数据下载完毕！')
        return True
    
    def get_itemid_download_excel(self, url, itemid, date_, index):
        
        print(f'开始下载 {itemid}: {date_} 的数据！')
        
        res = self.base.new_url(dict_={'itemId':itemid, 'dateRange':f'{date_}|{date_}'}, oldurl=url)
        print(res['url'])
        
        if res['mark'] is False:
            print('更新链接地址失败！')
            self.log(msg='更新链接地址失败！')
            return False
                    
        self.data_bool = self.page.get(res['url'])
        print(f"self.data_bool: {self.data_bool}")
         
        if self.data_bool:
            print(self.page.raw_data)
            self.data = self.page.raw_data
            is_byte = self.is_bytes_string(self.data)
            # print(is_byte) 
        else:
            print(f'下载数据失败， 失败日期： {date_}')
            return False
            
        if is_byte:
            
            df = pd.read_excel(BytesIO(self.data), header=index)
            
            df.to_excel(f'{self.base.source_path}/[生意参谋平台]{self.task_name}&&{itemid}&&{date_}&&{date_}.xlsx', index=False, engine="xlsxwriter")
                    
            print(f'{itemid}: {date_} 的数据保存成功！')
            
        else:
                
            print(f'下载的数据格式不正确，请检查, {date_}')
            self.log(msg=f'下载的数据格式不正确，请检查, {date_}')
            return False
            
        # self.base.page.wait(random.randint(1, 2))
    
    def get_excel_data_to_db(self):
        
        # 定义是否有需要删除的列
        columns_to_drop = self.columns_to_drop
        
        table_name = self.table_name
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"[达摩盘]&&{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                # print(f'开始执行 {filename} 的数据！')
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                if len(excel_data_df) == 0:
                    self.log(msg=f'{filename} 是空数据！, {date_}')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                    continue

                # 删除不需要的列
                excel_data_df = excel_data_df.drop(labels=columns_to_drop, axis=1)
                
                # 写入数据库
                res = self.insert_data_to_db(df=excel_data_df, table_name=table_name, add_col=self.add_col, key=['product_id', 'statistic_date'])
                
                if res:
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.succeed_path}/" + filename,
                    )
                else:
                    print(f'{self.base.config_obj["shop_name"]}: <error> [达摩盘][货品洞查] 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    self.log(msg=f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                
            print('数据写入执行完毕！')
              
        except Exception as e:
            
            print(f'{self.base.config_obj["shop_name"]}: <error> [达摩盘][货品洞查] 写入报错， {filename} 文件已剪切至 failure 文件夹！')
            self.log(msg=f'# 写入报错， {filename} 文件已剪切至 failure 文件夹！')
            print(e)

    def clean_and_transform_sku_data(self, df):
        
        mapping = {
            'skuId': 'sku_id',
            'sku名称': 'sku_name',
            '支付金额': 'payment_amount',
            '支付买家数': 'buyer_count',
            '支付件数': 'item_sold_count',
            '加购件数': 'add_to_cart_item_count',
            }

        # 重命名列以匹配数据库字段
        df = df.rename(columns=mapping)

        columns_to_convert = [
            'buyer_count', 'item_sold_count', 'add_to_cart_item_count'
        ]
        for column in columns_to_convert:
            try:
                df[column] = df[column].apply(lambda x : 0.0 if x == '-' else x)
                df[column] = df[column].replace({',': ''}, regex=True).astype('int64')
            except Exception as e:
                #print(column, e)
                df[column] = 0

        columns_to_convert = [
            'payment_amount' 
        ]
        for column in columns_to_convert:
            try:    
                df[column] = df[column].replace({',': ''}, regex=True).str.rstrip('%').astype('float')
            except Exception as e:
                #print(column, e)
                df[column] = 0.0

        return df
    
    # 判断数据是否是以 b 开头
    def is_bytes_string(self, data):
        # 判断数据是否以字节字符串的形式表示
        return isinstance(data, bytes)
    
    def insert_data_to_db(self, df, table_name, key=[], add_col={}, keywords = None):
        
        # print(self.base.insert_data)
        
        res = self.base.insert_data(df_cleaned=df, table_name=table_name, key=key, add_col=add_col, keywords=keywords)
        
        if res is False:
            
            return False
        
        return True
    
    def write_log(self):
        
        self.base.log_(self.base.log_arr)
    
    def send_email(self):
        
       self.base.send_emails()
       
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
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 [达摩盘][货品洞查]!')
        
        res = self.visit_damopan()
        
        if res is False:
            return
        
        res = self.get_json_data()
        
        if res is False:
            return
        
        res = self.get_excel_data_to_db()
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 [达摩盘][货品洞查]!')
    
    def test(self):
        
        self.visit_damopan()
        self.get_json_data()
    
if __name__ == "__main__":
    test = damopan_huopindongcha()
    test.run()