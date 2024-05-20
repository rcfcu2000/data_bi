"""
    店铺等级与排名
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

class biz_shop_customer_service:
    
    def __init__(self, config) -> None:
        
        self.base = base_action()
        self.config = config
               
        # 获取配置文件中该任务的配置
        self.get_config_bool = self.base.get_configs(self.__class__.__name__, config_name=self.config)
        
        # 获取配置文件中公用配置的对象
        #self.base_config = self.base.get_configs_return_obj('base_config', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        
        # 检查并拿到 pageTab [根据需要修改的参数]
        self.check_url = self.base.config_obj['check_url']
        
        self.shop_id = self.base.config_obj['shop_id']

        self.shop_name = self.base.config_obj['shop_name']

        # 修改 存储数据的 excel 名称 [根据需要修改的参数]
        self.task_name = self.base.config_obj['task_name']

        self.table_name = self.base.config_obj['table_name']
        
        # [根据需要修改的参数]
        self.primary_key = ['shop_id', 'statistic_date']
        # [根据需要修改的参数]
        self.add_col = {'shop_id':self.shop_id, 'shop_name':self.shop_name}
        self.page = None
    
    def visit_sycm(self):
        
        try:
        
            if self.get_config_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 读取配置文件出错，请检查。')
                return False
            
            if self.create_folder_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 创建存储文件出错，请检查。')
                return False

            port = self.base.config_obj.get('port', self.base.config_obj['port'])    
            
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
    
    def clean_and_transform_shop_cs_data(self, excel_data_df):
        """
        Transforms the Excel data to align with the 'biz_shop_customer_service' database table schema.

        :param excel_data_df: DataFrame containing the Excel data.
        :return: Transformed DataFrame ready for database insertion.
        """
        # Rename columns to match the database schema
        column_mappings = {
            '日期': 'statistic_date',
            '咨询人数': 'inquiry_count',
            '平均响应时长（秒)': 'avg_response_time',
            '客户满意率': 'customer_satisfaction_rate',
            '客服销售额': 'sales_revenue',
            '客服销售人数': 'sales_count',
            '客服销售额占比': 'sales_revenue_ratio',
            '客服销售客单价': 'sales_unit_value',
            '询单转化率': 'inquiry_conversion_rate'
        }
        transformed_df = excel_data_df.rename(columns=column_mappings)

        transformed_df = transformed_df[transformed_df.iloc[:, 0] != '汇总值']

        # Convert date format from float to YYYY-MM-DD (if necessary)
        try:
            transformed_df['statistic_date'] = pd.to_datetime(transformed_df['statistic_date'], format='%Y%m%d')
            transformed_df['statistic_date'] =  transformed_df['statistic_date'].apply(lambda x:x.strftime('%Y-%m-%d')) 
            transformed_df['inquiry_conversion_rate'] =  transformed_df['inquiry_conversion_rate'].apply(lambda x:0.0 if x == '延时统计' else x) 
        except Exception as ex:
            print(ex)

        # 将包含逗号的字符串字段转换为整数
        columns_to_convert = [
            'inquiry_count', 'sales_count', 
        ]

        for column in columns_to_convert:
            try:
                transformed_df[column] = transformed_df[column].apply(lambda x : 0.0 if x == '-' else x)
                transformed_df[column] = transformed_df[column].replace({',': ''}, regex=True).astype('int64')
            except Exception as e:
                #print(column, e)
                transformed_df[column] = 0

        return transformed_df

    def down_load_excel(self):
        
        date_format = "%Y%m%d"
        
        date_range = []
        
        self.page.change_mode('s')
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天' or self.base.config_obj['automatic_date'] == '自动计算前一天':
            before_day = self.base.get_before_day_datetime()
            date_range = pd.date_range(before_day, before_day)
        else:
            date_range = pd.date_range(self.base.config_obj.get('start_date', self.base.config_obj['start_date']), self.base.config_obj.get('end_date', self.base.config_obj['end_date']))
        
        # 重试
        url = self.base.config_obj['second_level_url']
        
        for date in date_range:
            
            data_arr = []
            
            start_date = date + timedelta(days=-30)
            start_date = start_date.strftime(date_format)

            # use date as end_date
            end_date = date.strftime(date_format)
            
            new_url = self.base.new_url(dict_={'startDate': f'{start_date}', 'endDate': f'{end_date}'}, oldurl=url)
            
            print(f"{self.base.config_obj['shop_name']}{self.task_name}: 开始获取 {end_date} 的数据, 链接：{new_url}")
            
            self.page.get(new_url['url'])

            contents = str(self.page.raw_data)

            download_link = contents[contents.find('"data":"') + 8 : contents.find('",\\n\\t"sessionId"')]

            self.page.get(download_link)

            df = pd.read_excel(BytesIO(self.page.raw_data), header=0)

            df = self.clean_and_transform_shop_cs_data(df)

            df.to_excel(f"{self.base.source_path}/{self.task_name}&&{end_date}.xlsx", index=False)
            
            print(f"{self.base.config_obj['shop_name']}{self.task_name}: True")
        
        return True
                         
    def get_excel_data_to_db(self):
        
        # 定义是否有需要删除的列
        columns_to_drop = []
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                print(f"{self.base.config_obj['shop_name']}{self.task_name}: 开始执行 {filename} 的数据！")
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                if len(excel_data_df) == 0:
                    print(f"#{self.base.config_obj['shop_name']}{self.task_name}: {filename} 是空数据！")
                    self.log(msg=f'{filename} 是空数据！')
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
                    print(f"#{self.base.config_obj['shop_name']}{self.task_name}: {filename} 的数据执行成功！")
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.succeed_path}/" + filename,
                    )
                else:
                    print(f"#{self.base.config_obj['shop_name']}{self.task_name}:  写入失败， {filename} 文件已剪切至 failure 文件夹！")
                    self.log(msg=f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                
            print(f"#{self.base.config_obj['shop_name']}{self.task_name}: 数据写入执行完毕！")
              
        except Exception as e:
            
            shutil.move(
                f"{self.base.source_path}/" + filename,
                f"{self.base.failure_path}/" + filename,
            )
            
            print(f"##{self.base.config_obj['shop_name']}{self.task_name}:  写入报错， {filename} 文件已剪切至 failure 文件夹！")
            self.log(msg=f'# 写入报错， {filename} 文件已剪切至 failure 文件夹！')
            print(e)

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
        
        res = self.down_load_excel()
        
        if res is False:
            return
        
        res = self.get_excel_data_to_db()
        if res is False:
            return
    
    def test(self):
        
        self.visit_sycm()
        # self.base.login_sycm(task_name=self.task_name)
        self.down_load_excel()
    
if __name__ == "__main__":
    test = biz_shop_customer_service()
    test.run()