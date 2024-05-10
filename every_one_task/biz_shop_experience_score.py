"""
    商品体验分
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

class biz_shop_experience_score:
    
    def __init__(self, config) -> None:
        
        self.base = base_action()
        self.config = config
        
        # 修改 存储数据的 excel 名称 [根据需要修改的参数]
        self.task_name = "[生意参谋]&&[商品体验分]"
        
        # 获取配置文件中该任务的配置
        self.get_config_bool = self.base.get_configs(self.__class__.__name__, config_name=self.config)
        
        # 获取配置文件中公用配置的对象
        self.base_config = self.base.get_configs_return_obj('base_config', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        
        # 检查并拿到 pageTab [根据需要修改的参数]
        self.check_url = 'myseller.taobao.com'
        
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
                pageTab = page.new_tab(self.base.config_obj['url'])
                self.page = pageTab

            # '//div[@class="next-tabs-tab-inner" and text()="店铺商品体验"]'
            
            ele = self.page.ele('店铺商品体验')
            
            ele.click()
            # self.page('tag: body').child(index=2).click()
            
            self.page.listen.start(self.base.config_obj['monitor_url'])  # 开始监听，指定获取包含该文本的数据包
            
            # for packet in self.page.listen.steps():
            #     print(packet.url)
                
            while True:  
                res = self.page.listen.wait()  
                data = res.response.body
                json_data = data[data.index('(')+1:-1]
                json_object = json.loads(json_data)
                if json_object['api'] == 'mtop.alibaba.tmall.item.diagnosis' and json_object['data']['componentId'] == 'tmallDiagnosisIndustryComparison':
                    print(json_object)
            
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
    
    def get_json_data(self):
        
        date_format = "%Y-%m-%d"
        
        date_range = []
        
        self.page.change_mode('s')
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天' or self.base_config['automatic_date'] == '自动计算前一天':
            before_day = self.base.get_before_day_datetime()
            date_range = pd.date_range(before_day, before_day)
        else:
            date_range = pd.date_range(self.base.config_obj.get('start_date', self.base_config['start_date']), self.base.config_obj.get('end_date', self.base_config['end_date']))
        
        # 重试
        url = self.base.config_obj['second_level_url']
        
        for date in date_range:
            
            data_arr = []
            
            date_ = date.strftime(date_format)
            
            new_url = self.base.new_url(dict_={'dateRange': f'{date_}|{date_}'}, oldurl=url)
            
            print(f"{self.base_config['shop_name']}{self.task_name}: 开始获取 {date_} 的数据, 链接：{new_url}")
            
            self.page.get(new_url)
            
            data_ = json.loads(self.page.raw_data)
            
            if data_['hasError'] is False:
                
                obj = {
                    'level': data_['content']['cateLevel'][-1],
                    'ranking': data_['content']['rank'][-1],
                    'shop_name': self.base_config['shop_name'],
                    'shop_id': '999',
                    'statistic_date': date_
                }
            else:
                self.log(f'获取店铺排名和等级信息失败， 获取日期：{data_}')
            
            data_arr.append(obj)
            
            # 将这一批数据写入 excel
            res = self.base.pandas_insert_data(data_arr, f"{self.base.source_path}/{self.task_name}&&{date_}.xlsx")
            print(f"{self.base_config['shop_name']}{self.task_name}: {res['msg']}")
        
        return res
                         
    def get_excel_data_to_db(self):
        
        # 定义是否有需要删除的列
        columns_to_drop = []
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                print(f"{self.base_config['shop_name']}{self.task_name}: 开始执行 {filename} 的数据！")
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                if len(excel_data_df) == 0:
                    print(f"#{self.base_config['shop_name']}{self.task_name}: {filename} 是空数据！")
                    self.log(msg=f'{filename} 是空数据！, {date_}')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                    continue

                # 删除不需要的列
                excel_data_df = excel_data_df.drop(labels=columns_to_drop, axis=1)
                
                # 写入数据库
                res = self.insert_data_to_db(df=excel_data_df, table_name=self.table_name, add_col=self.add_col, key=self.primary_key)
                
                if res:
                    print(f"#{self.base_config['shop_name']}{self.task_name}: {filename} 的数据执行成功！")
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.succeed_path}/" + filename,
                    )
                else:
                    print(f"#{self.base_config['shop_name']}{self.task_name}:  写入失败， {filename} 文件已剪切至 failure 文件夹！")
                    self.log(msg=f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                
            print(f"#{self.base_config['shop_name']}{self.task_name}: 数据写入执行完毕！")
              
        except Exception as e:
            
            shutil.move(
                f"{self.base.source_path}/" + filename,
                f"{self.base.failure_path}/" + filename,
            )
            
            print(f"##{self.base_config['shop_name']}{self.task_name}:  写入报错， {filename} 文件已剪切至 failure 文件夹！")
            self.log(msg=f'# 写入报错， {filename} 文件已剪切至 failure 文件夹！')
            print(e)

    def clean_and_transform_data(self, df):

        columns_to_convert = [
            'price_strength',
            'unit_price',
            'price_strength_exposure'
        ]
        for column in columns_to_convert:
            try:    
                df[column] = df[column].replace({',': ''}, regex=True).str.rstrip('%').astype('float')
            except Exception as e:
                #print(column, e)
                df[column] = 0.0

        return df
    
    def insert_data_to_db(self, df, table_name, key=[], add_col={}, keywords = None):
        
        # print(self.base.insert_data)
        
        res = self.base.insert_data(df_cleaned=df, table_name=table_name, key=key, add_col=add_col, keywords=keywords)
        
        if res is False:
            
            return False
        
        return True
       
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
        
        # res = self.get_json_data()
        
        # if res is False:
        #     return
        
        # res = self.get_excel_data_to_db()
        # if res is False:
        #     return
    
    def test(self):
        
        self.visit_sycm()
        # self.base.login_sycm(task_name=self.task_name)
        self.get_json_data()
    
if __name__ == "__main__":
    test = biz_shop_experience_score()
    test.run()