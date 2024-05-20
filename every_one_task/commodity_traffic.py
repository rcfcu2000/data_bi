"""
    商品每日数据
"""
import random
import json
import os
import re
import pandas as pd
import calendar
from .base_action import base_action
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, Table, Column, String, Date, Float, MetaData, DECIMAL
from sqlalchemy.dialects.mysql import DOUBLE


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
        
        self.excel_data_df_count = 0
        data_count = 0
        transfersql = ''
        source_text = ''
        filename = ''
        
        try:
            engine = self.base.create_engine()
            
            if self.base.create_engine_bool is False:
                return
            
            conn = engine.connect()
            
            filelist = [
                    f
                    for f in os.listdir(f"{self.base.source_path}")
                    if f"【生意参谋平台】" in f
                ]

            index = 0
            list_end = False
            
            while not list_end:
                
                try:
                
                    for i in range(0, 100):
                        
                        filename = filelist[index]

                        match = re.search(r'\[(.*?)\]', filename)
                        source_text = match.group(1)

                        excel_data_df = pd.read_excel(
                            f"{self.base.source_path}/" + filename)
                        
                    
                        # 使用正则表达式提取数字
                        match = re.search(r"\b\d+\b", filename)
                        id_ = match.group()
                        dstring = filename.split("&&")[1]

                        df_cleaned = self.base.clean_and_transform_product_flowes_data(
                            excel_data_df
                        )

                        df_cleaned["product_id"] = id_
                        df_cleaned["shop_name"] = self.base.config_obj["shop_name"]
                        df_cleaned["src"] = source_text

                        if i == 0:
                            df_final = df_cleaned
                        else:
                            df_final = df_final._append(df_cleaned)

                        index += 1
                        if index >= len(filelist):
                            list_end = True
                            break

                    temptable = "temp"
                    table = "biz_product_traffic_stats"
                    key = [
                        "product_id",
                        "statistic_date",
                        "source_type_1",
                        "source_type_2",
                        "source_type_3",
                        'src'
                    ]

                    df_final.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                    """
                                        # where not exists 
                                        # (select 1 from {table} m 
                                        # where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        # )"""
                    # print(transfersql)
                    
                    conn.execute(text(transfersql))

                    conn.execute(text(f"drop table {temptable}"))
                
                except Exception as e:
                    
                    print(f"{self.base.config_obj['shop_name']}: 发生错误的文件 - {filename} - 数据写入出错！\n {str(e)}")
                
        except Exception as e:
            
            print(f"{self.base.config_obj['shop_name']}: {self.task_name} - 数据写入出错！\n {str(e)}")
            

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
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 商品流量数据，请检查！')
        
        res = self.down_load_excel()
        
        if res is False:
            return
        
        res = self.db_insert_data()
        
        if res is False:
            return
        
        # if self.base.config_obj['automatic_date'] == '自动计算前一天':
        #     datetime_ = self.base.get_before_day_datetime()
        #     start_date = datetime_
        #     end_date = datetime_
        # else:
        #     start_date = self.base.config_obj["start_date"]
        #     end_date = self.base.config_obj["end_date"]
        
        # res = self.base.calc(start_date_=start_date, end_date_=end_date)
        
        # if res is False:
        #     return
        
        # res = self.base.calc_prepallet()
        
        # if res is False:
        #     return
        
        # # 1. 删除 biz_pallet_product 指定日期的信息
        # # 2. 从视图 v_pallet_product 写入相关数据
        
        # res = self.base.insert_biz_pallet_product_from_v_pallet_product()
        
        # if res is False:
        #     return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 商品流量数据')
        
    def test(self):
        self.get_config()
        self.down_load_excel()
        pass
    
if __name__ == "__main__":
    test = commodity_traffic()
    test.test()
        