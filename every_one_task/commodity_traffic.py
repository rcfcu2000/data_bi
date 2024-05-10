"""
    商品每日数据
"""
import random
import json
import re
import pandas as pd
import calendar
from .base_action import base_action
from datetime import datetime, timedelta

class commodity_traffic:
    
    def __init__(self, config) -> None:
        self.base = base_action()
        self.task_name = "【商品流量数据来源】"
        self.config_name = 'sycmCommodityTrafficSource'
        self.config = config
        pass
    
    def get_config(self):
        
        mark = False
        
        res = self.base.get_configs(self.config_name, config_name=self.config)

        if res:
            # print(self.inst_.config_obj)
            mark = True
        else:
            print(f'{self.base.config_obj["shop_name"]}: <error> 获取配置信息失败!')
        
        return mark
    
    # 创建存储数据的文件
    def create_folder(self):

        res = self.base.create_folder("D:", self.base.config_obj["excel_storage_path"])

        return res
    
    def visit_sycm(self):
        
        res = self.get_config()

        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 读取配置文件出错，请检查。')
            return False

        res = self.create_folder()

        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 创建存储文件出错，请检查。')
            return False

        res = self.base.visit_sycm(task_name=self.task_name, config=self.config)

        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.base.login_sycm(task_name=self.task_name)
        
        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 登录失败，请检查！')
            return False

        return True
    
    # 下载excel
    def down_load_excel(self):
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False
            
        # res = self.base.commodity_flow_data(task_tag='[第一次访问来源]', automatic_date=automatic_date)
        # print('开始执行商品流量来源数据, 【第一次访问来源】。')
        
        res = self.base.commodity_flow_data_from_biz_product_performance(task_tag='[第一次访问来源]', automatic_date=automatic_date)

        # res = self.base.commodity_flow_data(task_tag='[最后一次访问来源]', automatic_date=automatic_date)
        res = self.base.commodity_flow_data_from_biz_product_performance(task_tag='[最后一次访问来源]', automatic_date=automatic_date)
    
        # res = self.base.commodity_flow_data(task_tag='[每一次访问来源]', automatic_date=automatic_date)
        res = self.base.commodity_flow_data_from_biz_product_performance(task_tag='[每一次访问来源]', automatic_date=automatic_date)
        
        return True
    
    def db_insert_data(self):
        
        res = self.base.insert_data_in_db(task_name=self.task_name)
        
        if res is False:
            print(f'{self.base.config_obj["shop_name"]}: <error> 数据写入失败，请检查！')
            return False
        
        return True
    
    def write_log(self):
        
        self.base.log_(self.base.log_arr)
    
    def send_email(self):
        
       self.base.send_emails()
       
    def run(self):
        
        res = self.visit_sycm()
        
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 商品流量数据，请检查！')
        
        res = self.down_load_excel()
        
        if res is False:
            return
        
        res = self.db_insert_data()
        
        if res is False:
            return
        
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
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 商品流量数据')
        
    def test(self):
        self.get_config()
        self.down_load_excel()
        pass
    
if __name__ == "__main__":
    test = commodity_traffic()
    test.test()
        