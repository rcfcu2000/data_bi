"""
    店铺关键词
"""
import random
import json
import os
import re
import shutil
import pandas as pd
import calendar
from .base_action import base_action
from io import BytesIO
from datetime import datetime, timedelta

class shop_key_words_through_train:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.task_name = "[店铺关键词][直通车]"
        self.get_config_bool = self.base.get_configs('sycmShopKeyWordsThroughTrain', config_name=config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        # 数据库表名
        self.table_name = 'biz_shop_keyword'
        self.add_col = {
            'shop_id': self.base.config_obj['shop_id'],
            'shop_name': self.base.config_obj['shop_name'],
            'src_type': '直通车'
        }
        self.config = config
    
    def visit_sycm(self):
        
        if self.get_config_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 读取配置文件出错，请检查。')
            return False
        
        if self.create_folder_bool is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 创建存储文件出错，请检查。')
            return False


        res = self.base.visit_sycm(task_name=self.task_name, config=self.config)

        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.base.login_sycm(task_name=self.task_name)
        
        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 登录失败，请检查！')
            return False

        return True
    
    # 下载excel
    def down_load_excel(self):
        
        is_byte = False
        data = ''
        index = 5
        
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
            # print(date.strftime("%Y-%m-%d"))
            date_ = date.strftime(date_format)
            modified_url = re.sub(re_str, f"dateRange={date_}|{date_}", url)
            # print(modified_url)
            # 开始访问
            self.data_bool = self.base.page.get(modified_url)
            
            if self.data_bool:
                self.data = self.base.page.raw_data
                # print(type(data))
                is_byte = self.is_bytes_string(self.data) 
            else:
                print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 下载数据失败， 失败日期： {date_}')
                self.base.page.wait(random.randint(1, 3))
                continue
            
            if is_byte:
                # print(self.base.page.raw_data)
                # 读取下载的数据
                df = pd.read_excel(
                            BytesIO(self.data),
                            header=index,
                        )
                df.to_excel(f'{self.base.source_path}/[生意参谋平台]{self.task_name}&&{date_}&&{date_}.xlsx', index=False,
                            engine="xlsxwriter")
                
            else:
                
                print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] 下载的数据格式不正确，请检查, {date_}')
                self.base.page.wait(random.randint(1, 3))
                continue
            
            self.base.page.wait(random.randint(1, 3))

        return True
    
    def get_excel_data_to_db(self):
        
                # 定义是否有需要删除的列
        columns_to_drop = ['支付金额', '客单价', '下单金额', '下单买家数', '支付买家数', 'UV价值', 
                           '关注店铺人数', '收藏商品买家数', '加购人数', '新访客', '收藏商品-支付买家数', 
                           '加购商品-支付买家数']
        
        table_name = self.table_name
        
        add_col = self.add_col
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"[生意参谋平台]{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                # 清洗数据
                df = self.clean_and_transform_shop_keyword_data(excel_data_df)

                # 删除不需要的列
                df = df.drop(labels=columns_to_drop, axis=1)
                
                date = filename.split('&&')[1]
                
                df['statistic_date'] = date
                
                # 写入数据库
                res = self.insert_data_to_db(df=df, table_name=table_name, add_col=add_col, key=['statistic_date', 'src_type'])
                
                if res:
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                            f"{self.base.source_path}/" + filename,
                            f"{self.base.succeed_path}/" + filename,
                        )
                    # print(f'{filename} 写入成功')
                else:
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                    print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] {filename} 写入失败')
                
        except Exception as e:
            
            shutil.move(
                f"{self.base.source_path}/" + filename,
                f"{self.base.failure_path}/" + filename,
            )
            print(f'{self.base.config_obj["shop_name"]}: <error> [直通车] {filename} 写入报错， 文件已剪切至 failure 文件夹！')
            print(e)

    def clean_and_transform_shop_keyword_data(self, df):
        mapping = {
            '来源名称': 'keyword',
            '访客数': 'visitor_count',
            '下单转化率': 'cart_addition_rate',
            '支付转化率': 'conversion_rate',
            '粉丝支付买家数': 'fan_payment_buyer_count',
            '直接支付买家数': 'direct_payment_buyer_count',
            }

        # 重命名列以匹配数据库字段
        df = df.rename(columns=mapping)

        columns_to_convert = [
            'visitor_count', 
        ]
        for column in columns_to_convert:
            try:
                df[column] = df[column].apply(lambda x : 0.0 if x == '-' else x)
                df[column] = df[column].replace({',': ''}, regex=True).astype('int64')
            except Exception as e:
                #print(column, e)
                df[column] = 0

        columns_to_convert = [
            'cart_addition_rate', 'conversion_rate', 
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
       
    def run(self):
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 [直通车]!')
        
        res = self.visit_sycm()
        
        if res is False:
            self.send_email()
            return
        
        res = self.down_load_excel()
        
        if res is False:
            return
        
        res = self.get_excel_data_to_db()
        
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 [直通车]!')
    
    def test(self):
        
        self.down_load_excel()
    
if __name__ == "__main__":
    test = shop_key_words_through_train()
    test.run()