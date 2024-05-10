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

class commodity_keywords_hand_search:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.config = config
        self.task_name = "[单品分析][手淘搜索]"
        self.get_config_bool = self.base.get_configs('sycmCommodityKeyWordsHandSearch', config_name=self.config)
        self.create_folder_bool = self.base.create_folder("D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        self.page = None
        self.shop_name = self.base.config_obj['shop_name']
        # 数据库表名
        self.table_name = 'biz_product_keyword'
        self.add_col = {}
    
    def visit_sycm(self):
        
        try:
            if self.get_config_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 读取配置文件出错，请检查。')
                return False
            
            if self.create_folder_bool is False:
                print(f'# {self.shop_name}{self.task_name} <error> 创建存储文件出错，请检查。')
                return False

            port = self.base.config_obj['port']    
            
            co = self.base.set_ChromiumOptions()

            co.set_address(f'127.0.0.1:{port}')

            page = WebPage(chromium_options=co)
            
            res = page.get_tab(url='sycm.taobao.com')
            
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
    
    def get_json_data(self):
        
        date_format = "%Y-%m-%d"
        
        date_range = []
        
        self.page.change_mode('s')
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天':
            before_day = self.base.get_before_day_datetime()
            date_range = pd.date_range(before_day, before_day)
        else:
            date_range = pd.date_range(self.base.config_obj["start_date"], self.base.config_obj["end_date"])
        
        url = self.base.config_obj['second_level_url']
        
        for date in date_range:
            
            data_arr = []
            
            date_ = date.strftime(date_format)
            
            # 拿到商品ID
            product_data = self.base.get_item_id(date_=date)
            # print(product_data)
            data = product_data['result']
            
            for i in range(0, len(data)):

                # 构建并打印 URL
                new_url = self.base.new_url(dict_={'itemId': data[i], 'dateRange': f'{date_}|{date_}'}, oldurl=url)
                
                print(new_url['url'])
                
                self.page.get(new_url['url'])
                
                json_data = json.loads(self.page.raw_data)
                
                if json_data['code'] == 0:
                    
                    keywords_data = json_data['data']['data']
                    
                    for data_item in keywords_data:
                        # 商品ID
                        product_id = data_item.get('itemId', {'value': ''})
                        # 来源类型
                        keyword = data_item.get('pageName', {'value': ''})
                        # 访客数
                        visitor_count = data_item.get('uv', {'value': ''})
                        # 加购人数
                        cart_addition_rate = data_item.get('crtRate', {'value': ''})
                        # 转化率
                        conversion_rate = data_item.get('payRate', {'value': ''})
                        # 粉丝支付买家数
                        fan_payment_buyer_count = data_item.get('fansPayByrCnt', {'value': ''})
                        # 直接支付买家数
                        direct_payment_buyer_count = data_item.get('directPayByrCnt', {'value': ''})
                        
                        if 'value' not in product_id:
                            product_id = {'value': data[i]}
                        
                        obj = {
                            'product_id': product_id['value'],
                            'statistic_date': date_,
                            'src_type': '手淘搜索',
                            'keyword': keyword['value'],
                            'visitor_count': visitor_count['value'],
                            'cart_addition_rate': cart_addition_rate['value'],
                            'conversion_rate': conversion_rate['value'],
                            'fan_payment_buyer_count': fan_payment_buyer_count['value'],
                            'direct_payment_buyer_count': direct_payment_buyer_count['value']
                        }
                        
                        data_arr.append(obj)
                    
                else:
                    
                    self.log(msg=f'<error> [商品关键词][手淘搜索] 访问数据失败&&访问日期&&{date}&&商品ID&&{data[i]}')
                    print(f'# {self.shop_name}{self.task_name} <error> 访问数据失败！')
                    return False
                    
            # 将这一批数据写入 excel
            res = self.base.pandas_insert_data(data_arr, f"{self.base.source_path}/[生意参谋]&&{self.task_name}&&{date_}.xlsx")
            print(res['msg'])
        
        return res
                         
    def get_excel_data_to_db(self):
        
        # 定义是否有需要删除的列
        columns_to_drop = []
        
        table_name = self.table_name
        
        filelist = [f for f in os.listdir(f"{self.base.source_path}") if f"[生意参谋]&&{self.task_name}" in f]
        
        try:
        
            for filename in filelist:
                
                print(f'开始执行 {filename} 的数据！')
                
                excel_data_df = pd.read_excel(
                        f"{self.base.source_path}/" + filename)
                
                if len(excel_data_df) == 0:
                    print(f'{filename} 是空数据！')
                    self.log(msg=f'{filename} 是空数据！, {date_}')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                    continue
                
                # 清洗数据
                # df = self.clean_and_transform_sku_data(excel_data_df)

                # 删除不需要的列
                excel_data_df = excel_data_df.drop(labels=columns_to_drop, axis=1)
                
                # data_arr = filename.split('&&')
                # itemid = data_arr[1]
                # statistic_date = data_arr[2]
                # self.add_col['product_id'] = itemid
                # self.add_col['statistic_date'] = statistic_date
                
                # 写入数据库
                res = self.insert_data_to_db(df=excel_data_df, table_name=table_name, add_col=self.add_col, key=['product_id', 'statistic_date', 'src_type'])
                
                if res:
                    # 将成功写入的文件移入 成功的文件夹
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.succeed_path}/" + filename,
                    )
                else:
                    print(f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    self.log(msg=f'# 写入失败， {filename} 文件已剪切至 failure 文件夹！')
                    shutil.move(
                        f"{self.base.source_path}/" + filename,
                        f"{self.base.failure_path}/" + filename,
                    )
                
            print('数据写入执行完毕！')
              
        except Exception as e:
            
            shutil.move(
                f"{self.base.source_path}/" + filename,
                f"{self.base.failure_path}/" + filename,
            )
            
            print(f'# 写入报错， {filename} 文件已剪切至 failure 文件夹！')
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
        
        res = self.get_json_data()
        
        if res is False:
            return
        
        res = self.get_excel_data_to_db()
        if res is False:
            return
    
    def test(self):
        
        self.visit_sycm()
        # self.base.login_sycm(task_name=self.task_name)
        self.get_json_data()
    
if __name__ == "__main__":
    test = damopan_huopindongcha()
    test.run()