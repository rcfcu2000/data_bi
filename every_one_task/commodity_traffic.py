"""
    商品每日数据
"""
import random
import json
import re
import pandas as pd
import calendar
import base_action
from datetime import datetime, timedelta

class commodity_traffic:
    
    def __init__(self) -> None:
        self.base = base_action.base_action()
        self.task_name = "【商品流量数据来源】"
        self.config_name = 'sycmCommodityTrafficSource'
        pass
    
    def get_config(self):
        
        mark = False
        
        res = self.base.get_configs(self.config_name)

        if res:
            # print(self.inst_.config_obj)
            mark = True
        else:
            print('# 获取配置信息失败!')
        
        return mark
    
    # 创建存储数据的文件
    def create_folder(self):

        res = self.base.create_folder("D:", self.base.config_obj["excel_storage_path"])

        return res
    
    def visit_sycm(self):
        
        res = self.get_config()

        if res is False:
            print('# error: 读取配置文件出错，请检查。')
            return False

        res = self.create_folder()

        if res is False:
            print('# error: 创建存储文件出错，请检查。')
            return False

        res = self.base.visit_sycm(task_name=self.task_name)

        if res is False:
            print('# 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.base.login_sycm(task_name=self.task_name)
        
        if res is False:
            print('# 登录失败，请检查！')
            return False

        return True
    
    # 下载excel
    def down_load_excel(self):
        
        if self.base.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False
            
        res = self.base.commodity_flow_data(task_tag='[每一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return False
        
        res = self.base.commodity_flow_data(task_tag='[第一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return False

        res = self.base.commodity_flow_data(task_tag='[最后一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return False

        return True
    
    def db_insert_data(self):
        
        res = self.base.insert_data_in_db(task_name=self.task_name)
        
        if res is False:
            return False
        
        return True
    
    def write_log(self):
        
        self.base.log_(self.base.log_arr)
    
    def send_email(self):
        
       self.base.send_emails()
       
    def run(self):
        
        res = self.visit_sycm()
        
        if res is False:
            self.send_email()
            return
        
        res = self.down_load_excel()
        
        if res is False:
            self.send_email()
            return
        
        res = self.db_insert_data()
        
        if res is False:
            self.send_email()
            return
    
if __name__ == "__main__":
    test = commodity_traffic()
    test.run()
        