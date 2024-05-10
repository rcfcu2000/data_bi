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

class commodity_sku:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.config = config
        self.task_name = "[商品sku销售详情]"
        self.get_config_bool = self.base.get_configs('sycmCommoditySku', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        # 数据库表名
        self.table_name = 'biz_product_sku'
        self.add_col = {}
        
    def visit_sycm(self):
        
        if self.get_config_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 读取配置文件出错，请检查。')
            return False
        
        if self.create_folder_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 创建存储文件出错，请检查。')
            return False


        res = self.base.visit_sycm(task_name=self.task_name, config=self.config)

        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.base.login_sycm(task_name=self.task_name)
        
        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 登录生意参谋失败，请检查。')
            return False

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
        self.base.page.change_mode('s')
        
        re_str = r"dateRange=(\d{4}-\d{2}-\d{2})\|(\d{4}-\d{2}-\d{2})"
        
        # 拿到链接, 处理为正确的链接
        url = self.base.config_obj["excel_url"]
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
                print('<error> product id 获取失败，请检查！')
                self.log(msg='<error> product id 获取失败，请检查！')
                return False
            
            date_ = date.strftime(date_format)
            print(f'查询到{date_}的在线商品个数：{len(product_data["result"])}')
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                
                futures = []
                
                for itemid in product_data['result']:
                    
                    future = executor.submit(self.get_itemid_download_excel, url, itemid, date_, index)
                    futures.append(future)
                
                    self.base.page.wait(random.uniform(0.2, 0.5))
                    
                    # count += 1
                    
                    # if count >= 5:
                    #     break
                    
                # 等待所有提交的任务完成
                concurrent.futures.wait(futures)

        # print('数据下载完毕！')
        return True
    
    def get_itemid_download_excel(self, url, itemid, date_, index):
        
        # print(f'开始下载 {itemid}: {date_} 的数据！')
        
        res = self.base.new_url(dict_={'itemId':itemid, 'dateRange':f'{date_}|{date_}'}, oldurl=url)
        # print(res['url'])
        
        if res['mark'] is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 更新链接地址失败！')
            self.log(msg='更新链接地址失败！')
            return False
                    
        self.data_bool = self.base.page.get(res['url'])
        # print(self.data_bool)
         
        if self.data_bool:
            # print(self.base.page.raw_data)
            self.data = self.base.page.raw_data
            is_byte = self.is_bytes_string(self.data)
            # print(is_byte) 
        else:
            print(f'{self.base.config_obj["shop_name"]}: <error> 下载数据失败， 失败日期： {date_}')
            return False
            
        if is_byte:
            
            df = pd.read_excel(BytesIO(self.data), header=index)
            
            df.to_excel(f'{self.base.source_path}/[生意参谋平台]{self.task_name}&&{itemid}&&{date_}&&{date_}.xlsx', index=False, engine="xlsxwriter")
                    
            # print(f'{itemid}: {date_} 的数据保存成功！')
            
        else:
                
            print(f'{self.base.config_obj["shop_name"]}: <error> 下载的数据格式不正确，请检查, {date_}')
            self.log(msg=f'下载的数据格式不正确，请检查, {date_}')
            return False
            
        # self.base.page.wait(random.randint(1, 2))
    
    def get_excel_data_to_db(self):
        
        # 定义是否有需要删除的列
        columns_to_drop = []
        
        table_name = self.table_name
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"[生意参谋平台]{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                # print(f'开始执行 {filename} 的数据！')
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                if len(excel_data_df) == 0:
                    print(f'{self.base.config_obj["shop_name"]}: <error> {filename} 是空数据！')
                    self.log(msg=f'{filename} 是空数据！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                    continue
                
                # 清洗数据
                df = self.clean_and_transform_sku_data(excel_data_df)

                # 删除不需要的列
                df = df.drop(labels=columns_to_drop, axis=1)
                
                data_arr = filename.split('&&')
                itemid = data_arr[1]
                statistic_date = data_arr[2]
                self.add_col['product_id'] = itemid
                self.add_col['statistic_date'] = statistic_date
                
                # 写入数据库
                res = self.insert_data_to_db(df=df, table_name=table_name, add_col=self.add_col, key=['product_id', 'statistic_date', 'sku_id'])
                
                if res:
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.succeed_path}/" + filename,
                    )
                else:
                    print(f'{self.base.config_obj["shop_name"]}: <error> 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    self.log(msg=f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                
            # print('数据写入执行完毕！')
            return True
          
        except Exception as e:
            
            print(f'{self.base.config_obj["shop_name"]}: <error> {filename} 文件已剪切至 failure 文件夹！')
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
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 商品SKU！')
        
        res = self.visit_sycm()
        
        if res is False:
            return
        
        res = self.down_load_excel()
        
        if res is False:
            return
        
        res = self.get_excel_data_to_db()
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 商品SKU！')
    
    def test(self):
        
        self.down_load_excel()
    
if __name__ == "__main__":
    test = commodity_sku()
    test.run()