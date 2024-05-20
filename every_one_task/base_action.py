"""
    此类用途：集成蜡笔派相关RPA以及数据提取的方法
"""

import os
import random
import socket
import json
import re
import time
import shutil
import yagmail
import logging
import concurrent.futures
from sqlalchemy import create_engine, text, Table, Column, String, Date, Float, MetaData, DECIMAL
from sqlalchemy.dialects.mysql import DOUBLE
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import configparser
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions


class base_action:
    def __init__(self):
        # test
        self.test = 0
        
        # 端口号 list
        self.port_list = []

        # 浏览器实例
        self.page = None

        # 读取配置文件是否成功
        self.get_config_bool = False

        # 配置项返回的对象
        self.config_obj = {}

        # 创建存储文件是否成功
        self.create_folder_bool = False

        # 是否访问成功
        self.visit_bool = False

        # 是否登录成功
        self.login_bool = False

        # 是否下载成功
        self.down_load_excel_bool = False

        # excel 是否 下载成功
        self.everyday_data_loadExcel_bool = False

        # 创建数据库引擎
        self.create_engine_bool = False

        # 清洗数据是否成功
        self.clean_and_transform_product_data_bool = False

        # 数据是否写入成功
        self.engine_insert_data_bool = False

        # 清洗数据是否成功 [店铺流量来源]
        self.clean_and_transform_shop_data_bool = False

        # 清洗数据是否成功 [搜索排行]
        self.clean_and_transform_search_rank_data_bool = False

        # 人群 top 10
        self.clean_and_transform_crowd_top_10_data_bool = False
        
        # 人群 top 20
        self.clean_and_transform_crowd_top_20_data_bool = False
        
        # 人群
        self.clean_and_transform_crowd_data_bool = False
        
        # 计算 写入数据的总条数
        self.excel_data_df_count = 0

        # 源数据存放路径
        self.source_path = ""
        # 写入成功的存放路径
        self.succeed_path = ""
        # 写入失败的存放路径
        self.failure_path = ""
        # 日志文件
        self.logger_path = ""
        # 日志内容 arr
        self.log_arr = []
        # 发送邮件内容的字符串拼接
        self.email_msg = ""
        self.email_msg_arr = []
        self.change_mode_index = 1
        
        # calc 执行sql 的时间
        self.calc_start_date = ''
        self.calc_end_date = ''
        
        # config_name
        self.config_name = ''

    # 获取可用端口
    def __find_free_port(self):

        res = {}

        try:
            # 创建一个临时套接字
            temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_socket.bind(("0.0.0.0", 0))  # 绑定到一个随机的空闲端口
            temp_socket.listen(1)  # 监听连接
            port = temp_socket.getsockname()[1]  # 获取实际绑定的端口
            self.port_list.append(port)
            temp_socket.close()  # 关闭临时套接字

            res["mark"] = True
            res["result"] = port

            return res

        except Exception as e:

            res["mark"] = False
            res["result"] = str(e)

            return res
        
    # 获取要执行的配置文件name
    def get_config_name(self):
        with open('./config/main.txt', 'r', encoding='utf-8') as f:
            config_name = f.read().split('\n')
            return config_name

    # 读取配置文件
    def get_config(self, key, config_name='my_config.ini', mode='s'):

        local_config_obj = None

        if mode == 's':
            try:
                config = configparser.ConfigParser()

                config.read(f"./config/{config_name}", encoding="utf-8")

                # 获取用户名和密码

                self.get_config_bool = True

                self.config_obj["url"] = config.get(key, "url")
                self.config_obj["excel_url"] = config.get(key, "excel_url")
                self.config_obj["excel_storage_path"] = config.get(
                    key, "excel_storage_path")
                self.config_obj["start_date"] = config.get(key, "start_date")
                self.config_obj["end_date"] = config.get(key, "end_date")
                self.config_obj['automatic_date'] = config.get(key, 'automatic_date')
                self.config_obj["user_name"] = config.get(key, "user_name")
                self.config_obj["pass_word"] = config.get(key, "pass_word")
                self.config_obj["shop_name"] = config.get(key, "shop_name")
                self.config_obj["db_host"] = config.get(key, "db_host")
                self.config_obj["db_user"] = config.get(key, "db_user")
                self.config_obj["db_password"] = config.get(key, "db_password")
                self.config_obj["db_database"] = config.get(key, "db_database")
                self.config_obj["db_raise_on_warnings"] = config.get(
                    key, "db_raise_on_warnings"
                )

                if key == 'sycmSearchRanking':
                    self.config_obj["second_level_url"] = config.get(key, "second_level_url")

                local_config_obj = self.config_obj

            except Exception as e:

                print(f'# {key} - {str(e)}')
                
        else:
            
            self.get_configs(key, config_name=config_name)
            local_config_obj = self.config_obj
            
        return local_config_obj

    
    def get_configs(self, key, config_name='my_config.ini'):
        
        mark = False
        
        try:
            # config_name = self.config_name
            
            config = configparser.ConfigParser()
        
            config.read(f'./config/{config_name}', encoding='utf-8')
        
            documents = config[key]
        
            for key in documents:
                self.config_obj[key] = documents[key]
        
            base_config = self.get_configs_return_obj('base_config', config_name=config_name)
            for key in base_config:
                if key not in self.config_obj.keys():
                    self.config_obj[key] = base_config[key]

            mark = True
            
        except Exception as e:
            
            # self.log_([f"error/shs/【{self.get_date_time()}】: 获取配置文件失败, 下面为错误信息."])
            # self.log_([f"error/shs/【{self.get_date_time()}】: {str(e)}"])
            
            print('# 获取配置文件失败, 下面为错误信息.')
            print(f'# {str(e)}')
        
        return mark
    
    def get_configs_return_obj(self, key, config_name='my_config.ini'):
        
        obj = {}
        
        try:
            
            config = configparser.ConfigParser()
        
            config.read(f'./config/{config_name}', encoding='utf-8')
        
            documents = config[key]
        
            for key in documents:
                obj[key] = documents[key]
        
            return obj
            
        except Exception as e:
            
            print("获取配置文件失败, 下面为错误信息.")
            print(f"{str(e)}")
        
            return False
        

    # get port
    def get_port(self):
        return self.__find_free_port()

    def sycm_login(self, task_name="【商品每日数据】"):
        
        mark = False

        # if self.visit_bool is False:
        #     return mark

        try:
            if self.page.url == self.config_obj["url"]:
                self.page("#fm-login-id").input(self.config_obj["user_name"])
                self.page("#fm-login-password").input(self.config_obj["pass_word"])
                # 这里可以做一个判断，用于新老登录界面的异常捕获
                iframe = self.page("#alibaba-login-box")
                res = iframe(".fm-button fm-submit password-login").click()
                # print(f'res{res}')
                self.page.wait(5)
                
            else:

                self.page.wait(5)
            
            self.login_bool = True
            return True
        
        except Exception as e:
            
            print(f'点击登录失败，可能是老版本的登录界面，准备切换为老版本的登录')
            
            try:
                
                iframe = self.page("@src=//login.taobao.com/member/login.jhtml?from=sycm&full_redirect=true&style=minisimple&minititle=&minipara=0,0,0&sub=true&redirect_url=http://sycm.taobao.com/")
                iframe(".fm-button fm-submit password-login").click()
                self.page.wait(5)
                self.login_bool = True
                return True
            
            except Exception as e:
                
                print(f'登录失败, <error> {str(e)}')
                self.log_([f"error/shs/【{self.get_date_time()}】: 登录失败, 下面为错误信息."])
                self.log_([f"error/shs/【{self.get_date_time()}】: <error> {str(e)}"])
            
                return False

    def down_load_excel(
        self, task_name="【商品每日数据】", mode='s', automatic_date=True
    ):

        date_ = ""
        path_ = ""

        # self.get_config('sycmCommodityEverydayData')

        # 改变模式 切换为 S 模式：requests
        try:
            self.page.change_mode(mode, go=False)

            # 将开始日期和结束日期替换成 start_date
            url = self.config_obj["excel_url"]
            re_str = r"dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})"
            date_match = re.search(re_str, url)
            start_date_str, end_date_str = date_match.groups()

            date1_str = self.config_obj["start_date"]
            date2_str = self.config_obj["end_date"]
            next_day_str = date1_str

            modified_url = re.sub(
                re_str, f"dateRange={next_day_str}%7C{next_day_str}", url
            )
            date_format = "%Y-%m-%d"
            date1 = datetime.strptime(date1_str, date_format)
            date2 = datetime.strptime(date2_str, date_format)

            # 这是要循环的次数
            days_difference = (date2 - date1).days

            if automatic_date:
                # automatic_date 这是代表是否执行当天日期的前一天, 适用于: 商品每日数据, 店铺每日流量以及需要每天去取数的模块, 取前一天的日期
                next_day_str = self.get_before_day_datetime()
                modified_url = re.sub(
                    re_str, f"dateRange={next_day_str}%7C{next_day_str}", modified_url
                )
                days_difference = 1

            elif days_difference == 0:

                days_difference = 1

            else:

                days_difference += 1
            
            count = 0
            error_count = 0
            index = 4

            if (task_name == "【店铺流量来源】[每一次访问来源]"
                    or task_name == '【店铺流量来源】[第一次访问来源]'
                    or task_name == '【店铺流量来源】[最后一次访问来源]'):

                index = 5
                parsed_url = urlparse(modified_url)
                query_params = parse_qs(parsed_url.query)
                belong_param = query_params.get('belong', None)
                if belong_param:
                    if task_name == '【店铺流量来源】[第一次访问来源]':
                        new_belong_param = 'farthest'
                        query_params['belong'] = [new_belong_param]
                        new_query_str = urlencode(query_params, doseq=True)
                        new_url_parts = parsed_url._replace(
                            query=new_query_str)
                        new_url_str = urlunparse(new_url_parts)
                        modified_url = new_url_str

                    elif task_name == '【店铺流量来源】[最后一次访问来源]':
                        new_belong_param = 'nearest'
                        query_params['belong'] = [new_belong_param]
                        new_query_str = urlencode(query_params, doseq=True)
                        new_url_parts = parsed_url._replace(
                            query=new_query_str)
                        new_url_str = urlunparse(new_url_parts)
                        modified_url = new_url_str
                    else:

                        pass

            for i in range(0, days_difference):

                try:
                    date_ = next_day_str

                    # print(modified_url)
                    self.page.get(modified_url)

                    # self.page.raw_data 相当于 requests的response.content
                    if self.page.raw_data:
                        # 这里开始要做一些变化
                        dtype_mapping = {"商品ID": str}
                        df = pd.read_excel(
                            BytesIO(self.page.raw_data),
                            dtype=dtype_mapping,
                            header=index,
                        )
                        excel_path = f"{self.source_path}/【生意参谋平台】{task_name}&&{next_day_str}&&{next_day_str}.xlsx"
                        
                        df.to_excel(excel_path, index=False,
                                    engine="xlsxwriter")
                    else:
                        error_count += 1     

                    # 开始计算下一个日期
                    if days_difference > 1:

                        re_str = r"dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})"
                        date_match = re.search(re_str, modified_url)
                        date1_str, date2_str = date_match.groups()
                        date1 = datetime.strptime(date1_str, date_format)
                        next_day = date1 + timedelta(days=1)
                        next_day_str = next_day.strftime(date_format)
                        modified_url = re.sub(
                            re_str,
                            f"dateRange={next_day_str}%7C{next_day_str}",
                            modified_url,
                        )

                    # 休眠一定时间
                    time.sleep(random.randint(0, 2))
                    count += 1

                except Exception as e:
                    
                    error_count += 1

                    self.log_([f"error/shs/【{self.get_date_time()}】: 下载excel失败, 当前出错日期：{date_}"])
                    self.log_([f"error/shs/【{self.get_date_time()}】: errorMessage: {str(e)}"])

                    res = self.fail_to_txt(next_day_str, task_name=task_name)

                    continue

            self.everyday_data_loadExcel_bool = True
            
            self.down_load_excel_bool = True

        except Exception as e:
            
            self.log_([f"error/shs/【{self.get_date_time()}】: 下载excel出错, 当前出错日期：{date_}, 下载环节异常终止!"])
            self.log_([f"error/shs/【{self.get_date_time()}】: errorMessage: {str(e)}"])
        
        return self.down_load_excel_bool
        
    # 商品流量数据 [根据excel 元数据下载数据]
    def commodity_flow_data(self, task_tag='[每一次访问来源]', mode="s", automatic_date=True):

        # self.get_config('sycmCommodityTrafficSource')
        # 读取excel全部商品数据
        folder_path = f"./commodity_source_data"
        file_list = [file for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
        # print(file_list)
        excel_data = pd.read_excel(
            f"{folder_path}/{file_list[0]}", usecols="B, H")
        excel_data_df = excel_data.sort_values(by=["商品ID"], ascending=False)

        start_date = self.config_obj["start_date"]
        end_date = self.config_obj["end_date"]
        date_range = []
        datetime_ = ""

        if automatic_date:
            datetime_ = self.get_before_day_datetime()
            start_date = datetime_
            end_date = datetime_
        
        self.calc_start_date = start_date
        self.calc_end_date = end_date

        date_range = pd.date_range(start_date, end_date)

        self.page.change_mode(mode)
        self.change_mode_index += 1

        good_lists = []
        counter = 0
        total_counter = 0

        # 创建线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(0, len(excel_data_df)):
                # for i in range(0, 4):
                good_id = str(excel_data_df.iloc[i].values[0])
                good_status = excel_data_df.iloc[i].values[1]
                total_counter += 1

                if good_status == "已下架":
                    print("商品已下架: ", good_id, str(total_counter))
                    continue

                args = ((good_id, date, task_tag) for date in date_range)

                # 使用 executor.map 同时启动多个线程执行任务
                executor.map(self.download_item_keywords, args)

            # 等待当前这10个任务执行完毕
            executor.shutdown(wait=True)

            num = random.randint(15, 30)
            print(f"# 强制等待 {num} 秒钟... ")
            time.sleep(num)

        pass

    def commodity_flow_data_from_biz_product_performance(self, task_tag='[每一次访问来源]', mode='s', automatic_date=True):
        
        # 处理时间
        
        start_date = self.config_obj["start_date"]
        end_date = self.config_obj["end_date"]
        date_range = []
        datetime_ = ""

        if automatic_date:
            datetime_ = self.get_before_day_datetime()
            start_date = datetime_
            end_date = datetime_
        
        self.calc_start_date = start_date
        self.calc_end_date = end_date

        date_range = pd.date_range(start_date, end_date)

        self.page.change_mode(mode)
        
        # 初始化 futures 列表
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            
            futures = []
            
            for date in date_range:     
                # 拿到需要下载的 item id
                res = self.get_item_id(date_=date)
                
                if res['mark'] is False:
                    # print('获取item id 失败。')
                    return {
                    'mark': False,
                    'message': '获取item id 失败。' ,
                    'data': ''
                    }
                    
                item_id = res['result']
                
                # print(f'当前在线商品总个数: {len(item_id)} 个')
                
                for id in item_id:
                    
                    future = executor.submit(self.download_item_keywords_, id, date, task_tag)
                    futures.append(future)
                
                num = random.randint(15, 30)
                # print(f"# 强制等待 {num} 秒钟... ")
                time.sleep(num)
                
            # 等待所有提交的任务完成
            concurrent.futures.wait(futures)
            
    def download_item_keywords_(self, id, date_, task_tag):
        
        dstring = date_.strftime("%Y-%m-%d")
        url = self.config_obj["excel_url"]
        
        match = re.search(
            r"dateRange=(\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2})", url)
        original_date_range = match.group(1)

        new_date_range = f"{dstring}|{dstring}"
        modified_url = re.sub(
            r"dateRange=\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}",
            f"dateRange={new_date_range}",
            url,
        )

        item_ids = re.findall(r"itemId=(\d+)", modified_url)
        new_item_ids = [id, id]
        modified_url = re.sub(
            r"itemId=\d+", lambda x: f"itemId={new_item_ids.pop(0)}", modified_url
        )

        # 修改参数  下载不同维度的数据来源
        parsed_url = urlparse(modified_url)
        query_params = parse_qs(parsed_url.query)

        # 获取belong参数的值，如果存在则进行修改
        belong_param = query_params.get('belong', None)
        if belong_param:
            if task_tag == '[第一次访问来源]':
                new_belong_value = "farthest"
                query_params['belong'] = [new_belong_value]
                new_query_string = urlencode(query_params, doseq=True)
                new_url_parts = parsed_url._replace(query=new_query_string)
                new_url = urlunparse(new_url_parts)
                modified_url = new_url

            elif task_tag == '[最后一次访问来源]':
                new_belong_value = "nearest"
                query_params['belong'] = [new_belong_value]
                new_query_string = urlencode(query_params, doseq=True)
                new_url_parts = parsed_url._replace(query=new_query_string)
                new_url = urlunparse(new_url_parts)
                modified_url = new_url
            else:
                pass
            
        else:
            print("<error> 商品流量数据 url 已变更, 请查看相关请求连接.")
            self.log_([f"error/shs/【{self.get_date_time()}】: 商品流量数据 url 已变更, 请查看相关请求连接"])
            return

        # print(modified_url)
        
        self.page.get(modified_url)

        if self.page.raw_data:
            try:
                # print(self.page.raw_data)
                df = pd.read_excel(BytesIO(self.page.raw_data), header=5)
                excel_path = f"{self.source_path}/【生意参谋平台】【商品流量数据来源】【{id}】{task_tag}&&{dstring}&&{dstring}.xlsx"
                # print(self.source_path)
                df.insert(df.shape[1], "日期", dstring)
                df.to_excel(excel_path, index=False, engine="xlsxwriter")
                
                # print(
                #     f"成功: 【生意参谋平台】【商品流量数据来源】【{id}】{task_tag}&&{dstring}&&{dstring}.xlsx  保存成功!"
                # )
                
            except Exception as e:
                
                # print(
                #     f"失败: 【生意参谋平台】【商品流量数据来源】【{id}】{task_tag}&&{dstring}&&{dstring}.xlsx  下载失败!"
                # )
                self.log_([f"error/shs/【{self.get_date_time()}】:【生意参谋平台】【商品流量数据来源】【{id_}】{task_tag}&&{dstring}&&{dstring}.xlsx  下载失败!"])
                # 创建 txt 记录下载失败的excel , 自动重下
                res = self.fail_to_txt(
                    dstring, task_name=f"【商品流量数据来源】{task_tag}", id_=id)
                
                # print(f"{res['message']}")
           
    def download_item_keywords(self, args):
        id_ = args[0]
        dates_ = args[1]
        task_tag = args[2]
        
        # print(f"{id_}: {dates_}")

        dstring = dates_.strftime("%Y-%m-%d")
        # print(f'字符串类型的date: {dstring}')
        url = self.config_obj["excel_url"]

        match = re.search(
            r"dateRange=(\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2})", url)
        original_date_range = match.group(1)

        new_date_range = f"{dstring}|{dstring}"
        modified_url = re.sub(
            r"dateRange=\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}",
            f"dateRange={new_date_range}",
            url,
        )

        item_ids = re.findall(r"itemId=(\d+)", modified_url)
        new_item_ids = [id_, id_]
        modified_url = re.sub(
            r"itemId=\d+", lambda x: f"itemId={new_item_ids.pop(0)}", modified_url
        )

        # 修改参数  下载不同维度的数据来源
        parsed_url = urlparse(modified_url)
        query_params = parse_qs(parsed_url.query)

        # 获取belong参数的值，如果存在则进行修改
        belong_param = query_params.get('belong', None)
        if belong_param:
            if task_tag == '[第一次访问来源]':
                new_belong_value = "farthest"
                query_params['belong'] = [new_belong_value]
                new_query_string = urlencode(query_params, doseq=True)
                new_url_parts = parsed_url._replace(query=new_query_string)
                new_url = urlunparse(new_url_parts)
                modified_url = new_url

            elif task_tag == '[最后一次访问来源]':
                new_belong_value = "nearest"
                query_params['belong'] = [new_belong_value]
                new_query_string = urlencode(query_params, doseq=True)
                new_url_parts = parsed_url._replace(query=new_query_string)
                new_url = urlunparse(new_url_parts)
                modified_url = new_url
            else:
                pass
            
        else:
            print("# 商品流量数据 url 已变更, 请查看相关请求连接.")
            self.log_([f"error/shs/【{self.get_date_time()}】: 商品流量数据 url 已变更, 请查看相关请求连接"])
            return

        print(modified_url)

        self.page.get(modified_url)

        if self.page.raw_data:
            try:
                # print(self.page.raw_data)
                df = pd.read_excel(BytesIO(self.page.raw_data), header=5)
                excel_path = f"{self.source_path}/【生意参谋平台】【商品流量数据来源】【{id_}】{task_tag}&&{dstring}&&{dstring}.xlsx"
                # print(self.source_path)
                df.insert(df.shape[1], "日期", dstring)
                df.to_excel(excel_path, index=False, engine="xlsxwriter")
                print(
                    f"excel : 【生意参谋平台】【商品流量数据来源】【{id_}】{task_tag}&&{dstring}&&{dstring}.xlsx  保存成功!"
                )
            except Exception as e:
                print(
                    f"excel : 【生意参谋平台】【商品流量数据来源】【{id_}】{task_tag}&&{dstring}&&{dstring}.xlsx  下载失败!"
                )
                self.log_([f"error/shs/【{self.get_date_time()}】:【生意参谋平台】【商品流量数据来源】【{id_}】{task_tag}&&{dstring}&&{dstring}.xlsx  下载失败!"])
                # 创建 txt 记录下载失败的excel , 自动重下
                res = self.fail_to_txt(
                    dstring, task_name=f"【商品流量数据来源】{task_tag}", id_=id_)
                print(f"{res['message']}")
        # pass

    # pandas insert excel
    def pandas_insert_data(self, data, path):

        try:
            # 将数据转换成DataFrame对象
            df = pd.DataFrame(data)

            # 将DataFrame写入Excel文件
            excel_file = path  # 设置输出的Excel文件名
            df.to_excel(excel_file, index=False)
            
            return {
                'mark': True,
                'data': df,
                'msg': 'excel 写入成功！'   
            }
        except Exception as e:
            
            return {
                'mark': False,
                'data': [],
                'error': e,
                'msg': 'excel 写入失败！'  
            }

    # 计算个体单元执行次数
    def compute_count(self, date1_str, date2_str):

        date_format = "%Y-%m-%d"
        date1 = datetime.strptime(date1_str, date_format)
        date2 = datetime.strptime(date2_str, date_format)

        # 这是要循环的次数
        days_difference = (date2 - date1).days

        if days_difference == 0:
            days_difference = 1
        else:
            days_difference += 1

        return days_difference

    # 将下载失败的时候的记录卸载txt中
    def fail_to_txt(
        self, dstring, task_tag="download", task_name="【name is none】", id_=None
    ):
        """
        此方法主要是用于下载或者数据写入失败的时候, 将有效信息存入txt
        :param dstring: 必须传入, 这是记录下载或者写入时候的关键信息, 日期
        :param id_: 可传可不传, 有些功能不一定用到 id
        :param task_tag: 这是文件名中标记此文件属于什么类型的任务, 比如: download, insert_db
        :param task_name: 这是文件名中标记此任务是属于哪一个, 比如: 【商品每日数据】,【店铺流量来源】
        :return: res: 返回是否写入成功, 以及message
        """
        try:
            with open(
                f'{self.failure_path}/txt/{task_tag}_error-【生意参谋平台】'
                f'{task_name}_{self.get_date_time(res="%Y-%m-%d")}.txt',
                "a",
                encoding="utf-8",
            ) as f:
                f.write(f"{id_}:{dstring}\n")
                print(
                    f"excel : excel下载失败, 失败记录已写入 {self.failure_path}/txt/download_error-【生意参谋平台】"
                    f"{task_tag}_{self.get_date_time(res='%Y-%m-%d')}!"
                )
            return {"mark": True, "message": "失败信息写入成功"}

        except Exception as e:

            print(f"# error: {task_tag}, {str(e)}")
            return {
                "mark": False,
                "message": f"# 失败信息写入失败: {task_tag}, {str(e)} ",
            }

    # 清洗商品每日数据
    def clean_and_transform_product_flowes_data(self, df):
        
        try:
            # 数据映射和转换
            column_mappings = {
                "一级来源": "source_type_1",
                "二级来源": "source_type_2",
                "三级来源": "source_type_3",
                "访客数": "visitors_count",
                "浏览量": "views_count",
                "支付金额": "paid_amount",
                "浏览量占比": "view_rate",
                "店内跳转人数": "in_store_transfers",
                "跳出本店人数": "outbound_exits",
                "收藏人数": "favorited_users",
                "加购人数": "add_to_carts",
                "下单买家数": "buyers_placed_orders",
                "下单转化率": "order_conversion_rate",
                "支付件数": "paid_quantity",
                "支付买家数": "total_paid_buyers",
                "支付转化率": "pay_conversion_rate",
                "直接支付买家数": "direct_paid_buyers",
                "收藏商品-支付买家数": "favorited_and_paid_buyers",
                "粉丝支付买家数": "fans_paid_buyers",
                "加购商品-支付买家数": "add_to_cart_and_paid_buyers",
                "日期": "statistic_date",
            }
            df = df.rename(columns=column_mappings)

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "paid_quantity",
                "visitors_count",
                "in_store_transfers",
                "outbound_exits",
                "favorited_users",
                "add_to_carts",
                "buyers_placed_orders",
                "direct_paid_buyers",
                "favorited_and_paid_buyers",
                "fans_paid_buyers",
                "add_to_cart_and_paid_buyers",
                "views_count",
            ]

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({",": ""}, regex=True)
                    .astype("int64", errors="ignore")
                )

            # 将包含逗号和%的字符串字段转换为浮点数
            columns_to_convert = [
                "view_rate",
                "pay_conversion_rate",
                "order_conversion_rate",
                "paid_amount",
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({",": ""}, regex=True)
                        .str.rstrip("%")
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0
        
        except Exception as e:
            
            print(f"数据清洗失败!")
            self.log_([f"error/shs/【{self.get_date_time()}】: 商品流量来源 清洗失败", f'{str(e)}'])

        return df

    # 清洗数据 并做好与数据库的映射
    # 商品每日数据
    def clean_and_transform_product_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:
            self.log_arr.append(
                f"info/shs/【{self.get_date_time()}】: 开始清洗数据 ..."
            )

            df = df.dropna(
                subset=["商品ID"]
            )  # 假设 '商品ID' 列没有空值，用它来确定数据开始的行

            # 重命名列以匹配数据库字段
            df = df.rename(
                columns={
                    "统计日期": "statistic_date",
                    "商品ID": "product_id",
                    "商品名称": "product_name",
                    "货号": "sku",
                    "商品状态": "product_status",
                    "商品标签": "product_tags",
                    "商品访客数": "visitors_count",
                    "商品浏览量": "views_count",
                    "平均停留时长": "avg_stay_duration",
                    "商品详情页跳出率": "detail_bounce_rate",
                    "商品收藏人数": "collection_count",
                    "商品加购件数": "add_to_cart_quantity",
                    "商品加购人数": "add_to_cart_buyers",
                    "下单买家数": "order_placed_buyers",
                    "下单件数": "order_quantity",
                    "下单金额": "order_amount",
                    "下单转化率": "order_conversion_rate",
                    "支付买家数": "paid_buyers",
                    "支付件数": "paid_quantity",
                    "支付金额": "paid_amount",
                    "商品支付转化率": "payment_conversion_rate",
                    "支付新买家数": "new_buyers_paid",
                    "支付老买家数": "returning_buyers_paid",
                    "老买家支付金额": "returning_buyers_paid_amount",
                    "聚划算支付金额": "group_buy_paid_amount",
                    "访客平均价值": "visitor_value",
                    "成功退款金额": "successful_refund_amount",
                    "竞争力评分": "competitiveness_score",
                    "年累计支付金额": "yearly_cumulative_paid_amount",
                    "月累计支付金额": "monthly_cumulative_paid_amount",
                    "月累计支付件数": "monthly_cumulative_paid_quantity",
                    "搜索引导支付转化率": "search_driven_payment_conversion_rate",
                    "搜索引导访客数": "search_driven_visitors_count",
                    "搜索引导支付买家数": "search_driven_paid_buyers",
                    "结构化详情引导转化率": "structured_detail_conversion_rate",
                    "结构化详情引导成交占比": "structured_detail_transaction_ratio",
                    # 如果还有其他列，请在此添加映射
                    # '店铺ID': 'shop_id'
                }
            )

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "visitors_count",
                "views_count",
                "collection_count",
                "add_to_cart_quantity",
                "add_to_cart_buyers",
                "order_placed_buyers",
                "order_quantity",
                "paid_buyers",
                "paid_quantity",
                "monthly_cumulative_paid_quantity",
                "search_driven_visitors_count",
                "search_driven_paid_buyers",
            ]

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({",": ""}, regex=True)
                    .astype("int64", errors="ignore")
                )

            # 将包含逗号和%的字符串字段转换为浮点数
            columns_to_convert = [
                "avg_stay_duration",
                "successful_refund_amount",
                "returning_buyers_paid_amount",
                "group_buy_paid_amount",
                "successful_refund_amount",
                "paid_amount",
                "order_amount",
                "yearly_cumulative_paid_amount",
                "monthly_cumulative_paid_amount",
            ]
            for column in columns_to_convert:
                df[column] = df[column].replace({",": ""}, regex=True)

            columns_to_convert = [
                "visitor_value",
                "detail_bounce_rate",
                "order_conversion_rate",
                "payment_conversion_rate",
                "search_driven_payment_conversion_rate",
                "structured_detail_conversion_rate",
                "structured_detail_transaction_ratio",
                "competitiveness_score",
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({",": ""}, regex=True)
                        .str.rstrip("%")
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.clean_and_transform_product_data_bool = True

        except Exception as e:
            
            print(f"数据清洗失败!")
            self.log_([f"error/shs/【{self.get_date_time()}】: 商品每日数据 清洗失败", f'{str(e)}'])
            self.email_msg = f"清洗数据失败, error: {str(e)}\n"

        return df

    # 店铺流量来源
    def clean_and_transform_shop_data(self, df, tag):

        try:

            mapping = {
                df.columns[0]: "primary_source",
                df.columns[1]: "secondary_source",
                df.columns[2]: "tertiary_source",
                df.columns[3]: "visitors_count",
                df.columns[4]: "visitors_change",
                df.columns[5]: "order_amount",
                df.columns[6]: "order_amount_change",
                df.columns[7]: "buyers_placed_orders",
                df.columns[8]: "buyers_change",
                df.columns[9]: "conversion_rate_order",
                df.columns[10]: "conversion_rate_order_change",
                df.columns[11]: "paid_amount",
                df.columns[12]: "paid_amount_change",
                df.columns[13]: "buyers_paid",
                df.columns[14]: "buyers_paid_change",
                # Placeholder mapping
                df.columns[15]: "conversion_rate_payment",
                # Placeholder mapping
                df.columns[16]: "conversion_rate_payment_change",
                df.columns[17]: "average_order_value",  # Placeholder mapping
                df.columns[18]: "aov_change",  # Placeholder mapping
                df.columns[19]: "uv_value",  # Placeholder mapping
                df.columns[20]: "uv_value_change",  # Placeholder mapping
                df.columns[21]: "followers_count",  # Placeholder mapping
                df.columns[22]: "followers_change",  # Placeholder mapping
                # Placeholder mapping
                df.columns[23]: "product_favorites_count",
                # Placeholder mapping
                df.columns[24]: "product_favorites_change",
                df.columns[25]: "add_to_cart_count",  # Placeholder mapping
                df.columns[26]: "add_to_cart_change",  # Placeholder mapping
                df.columns[27]: "new_visitors_count",  # Placeholder mapping
                df.columns[28]: "new_visitors_change",  # Placeholder mapping
                # Placeholder mapping
                df.columns[29]: "direct_pay_buyers_count",
                # Placeholder mapping
                df.columns[30]: "favorited_product_pay_buyers_count",
                # Placeholder mapping
                df.columns[31]: "follower_pay_buyers_count",
                # Placeholder mapping
                df.columns[32]: "add_to_cart_pay_buyers_count",
                # Placeholder mapping
                df.columns[33]: "homepage_guide_visitor_count",
                # Placeholder mapping
                df.columns[34]: "homepage_guide_visitor_change",
                # Placeholder mapping
                df.columns[35]: "short_video_guide_visitor_count",
                # Placeholder mapping
                df.columns[36]: "product_guide_visitor_count",
                # Placeholder mapping
                df.columns[37]: "grassroot_transaction_count",
                # Placeholder mapping
                df.columns[38]: "grassroot_transaction_amount"
            }

            if tag == '每一次访问来源':
                mapping[df.columns[39]] = 'ad_transaction_amount'
                mapping[df.columns[40]] = 'ad_transaction_amount_change'
                mapping[df.columns[41]] = 'ad_clicks_count'
                mapping[df.columns[42]] = 'ad_clicks_change'

            # 重命名列以匹配数据库字段
            df = df.rename(columns=mapping)

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "visitors_count",
                "buyers_placed_orders",
                "buyers_paid",
                "followers_count",
                "product_favorites_count",
                "add_to_cart_count",
                "new_visitors_count",
                "direct_pay_buyers_count",
                "follower_pay_buyers_count",
                "favorited_product_pay_buyers_count",
                "add_to_cart_pay_buyers_count",
                "homepage_guide_visitor_count",
                "short_video_guide_visitor_count",
                "product_guide_visitor_count",
                "grassroot_transaction_count",
                "ad_clicks_count",
            ]

            for column in columns_to_convert:
                try:
                    if tag != '每一次访问来源':
                        if column == 'ad_clicks_count':
                            continue
                    df[column] = df[column].apply(
                        lambda x: 0.0 if x == "-" else x)
                    df[column] = (
                        df[column].replace(
                            {",": ""}, regex=True).astype("int64")
                        )
                except Exception as e:
                    print(f'1, {column}, {e}')
                    df[column] = 0

            # 将包含逗号和%的字符串字段转换为浮点数
            columns_to_convert = [
                "order_amount",
                "paid_amount",
                "average_order_value",
                "uv_value",
                "grassroot_transaction_amount",
                "ad_transaction_amount",
            ]
            for column in columns_to_convert:
                try:
                    if tag != '每一次访问来源':
                        if column == 'ad_transaction_amount':
                            continue

                    df[column] = df[column].apply(
                        lambda x: 0.0 if x == "-" else x)
                    df[column] = (
                        df[column].replace(
                            {",": ""}, regex=True).astype("float")
                    )
                except Exception as e:
                    print(f'2, {column}, {e}')
                    df[column] = 0.0

            columns_to_convert = [
                "order_amount_change",
                "ad_clicks_change",
                "ad_transaction_amount_change",
                "homepage_guide_visitor_change",
                "new_visitors_change",
                "paid_amount_change",
                "add_to_cart_change",
                "product_favorites_change",
                "buyers_paid_change",
                "followers_change",
                "uv_value_change",
                "aov_change",
                "conversion_rate_payment",
                "conversion_rate_payment_change",
                "visitors_change",
                "buyers_change",
                "conversion_rate_order",
                "conversion_rate_order_change",
            ]

            for column in columns_to_convert:
                try:
                    if tag != '每一次访问来源':
                        
                        if column == 'ad_clicks_change' or column == 'ad_transaction_amount_change':
                            continue

                    df[column] = df[column].apply(
                        lambda x: 0.0 if x == "-" else x)

                    df[column] = (df[column].replace({",": ""}, regex=True).str.strip().str.replace(r'%', '', regex=True).astype(float))

                except Exception as e:
                    print(f'<error> {column}, {e}')
                    df[column] = 0.0

            self.clean_and_transform_shop_data_bool = True

        except Exception as e:
            
            print(f"数据清洗失败!")
            self.log_([f"error/shs/【{self.get_date_time()}】: 店铺流量来源 清洗失败", f'{str(e)}'])
            
            self.email_msg = f"清洗数据失败, error: {str(e)}\n"

        return df

    # 宝贝主体报表
    def clean_and_transform_wanxiang_product_data(self, df):
        global cn
        try:

            column_mappings = {
                "日期": "datetimekey",
                "场景ID": "promotion_id",
                "场景名字": "promotion_name",
                "计划ID": "plan_id",
                "计划名字": "plan_name",
                "主体ID": "product_id",
                "主体类型": "product_type",
                "主体名称": "product_name",
                "展现量": "impressions",
                "点击量": "clicktraffic",
                "花费": "spend",
                "点击率": "点击率",
                "平均点击花费": "平均点击花费",
                "千次展现花费": "千次展现花费",
                "总预售成交金额": "pre_sell_amount",
                "总预售成交笔数": "pre_sell_count",
                "直接预售成交金额": "dir_pre_sell_amount",
                "直接预售成交笔数": "dir_pre_sell_count",
                "间接预售成交金额": "idr_pre_sell_amount",
                "间接预售成交笔数": "idr_pre_sell_count",
                "直接成交金额": "dir_sell_amount",
                "间接成交金额": "idr_sell_amount",
                "总成交金额": "gmv",
                "总成交笔数": "gmv_count",
                "直接成交笔数": "dir_sell_count",
                "间接成交笔数": "idr_sell_count",
                "点击转化率": "点击转化率",
                "投入产出比": "投入产出比",
                "总成交成本": "总成交成本",
                "总购物车数": "shopcart_count",
                "直接购物车数": "dir_shopcart_count",
                "间接购物车数": "idr_shopcart_count",
                "加购率": "加购率",
                "收藏宝贝数": "coll_prod_count",
                "收藏店铺数": "coll_shop_count",
                "店铺收藏成本": "店铺收藏成本",
                "总收藏加购数": "coll_add_count",
                "总收藏加购成本": "总收藏加购成本",
                "宝贝收藏加购数": "coll_add_prod_count",
                "宝贝收藏加购成本": "宝贝收藏加购成本",
                "总收藏数": "coll_count",
                "宝贝收藏成本": "宝贝收藏成本",
                "宝贝收藏率": "宝贝收藏率",
                "加购成本": "加购成本",
                "拍下订单笔数": "take_order_count",
                "拍下订单金额": "take_order_amount",
                "直接收藏宝贝数": "dir_coll_prod_count",
                "间接收藏宝贝数": "idr_coll_prod_count",
                "优惠券领取量": "coupon_count",
                "购物金充值笔数": "recharge_count",
                "购物金充值金额": "recharge_amount",
                "旺旺咨询量": "wangwang_count",
                "引导访问量": "guided_visits",
                "引导访问人数": "guided_visitors",
                "引导访问潜客数": "potential_guided_visitors",
                "引导访问潜客占比": "引导访问潜客占比",
                "入会率": "入会率",
                "入会量": "enrollment_count",
                "引导访问率": "引导访问率",
                "深度访问量": "deep_visits",
                "平均访问页面数": "平均访问页面数",
                "成交新客数": "new_customers",
                "成交新客占比": "成交新客占比",
                "会员首购人数": "first_buy_members",
                "会员成交金额": "members_gmv",
                "会员成交笔数": "members_gmv_count",
                "成交人数": "buyer_count",
                "人均成交笔数": "人均成交笔数",
                "人均成交金额": "人均成交金额",
                "自然流量转化金额": "natural_flow_amount",
                "自然流量曝光量": "natural_flow_count",
            }
            df = df.rename(columns=column_mappings)
            # 还需要新增 3列数据

            df["shop_name"] = self.config_obj["shop_name"]
            df["promotion_type"] = df["promotion_name"].copy()

            # 开始数据清洗
            # 将日期列的 / 换成 -
            columns_to_convert = ["datetimekey"]
            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({"/", "-"}, regex=True)
                    .astype("str", errors="ignore")
                )

            # 字符串 转 整数， 去除 逗号, 去除 \n 字符
            columns_to_convert = [
                "impressions",
                "clicktraffic",
                "pre_sell_count",
                "dir_pre_sell_count",
                "idr_pre_sell_count",
                "gmv_count",
                "dir_sell_count",
                "idr_sell_count",
                "shopcart_count",
                "dir_shopcart_count",
                "idr_shopcart_count",
                "coll_prod_count",
                "coll_shop_count",
                "coll_add_count",
                "coll_add_prod_count",
                "coll_count",
                "take_order_count",
                "dir_coll_prod_count",
                "idr_coll_prod_count",
                "coupon_count",
                "recharge_count",
                "wangwang_count",
                "guided_visits",
                "guided_visitors",
                "potential_guided_visitors",
                "enrollment_count",
                "deep_visits",
                "new_customers",
                "first_buy_members",
                "members_gmv_count",
                "buyer_count",
                "natural_flow_count",
                "平均访问页面数",
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )

            # 字符串转小数
            columns_to_convert = [
                "spend",
                "点击率",
                "平均点击花费",
                "千次展现花费",
                "pre_sell_amount",
                "dir_pre_sell_amount",
                "idr_pre_sell_amount",
                "dir_sell_amount",
                "idr_sell_amount",
                "gmv",
                "点击转化率",
                "投入产出比",
                "总成交成本",
                "加购率",
                "店铺收藏成本",
                "总收藏加购成本",
                "宝贝收藏加购成本",
                "宝贝收藏成本",
                "宝贝收藏率",
                "加购成本",
                "take_order_amount",
                "recharge_amount",
                "引导访问潜客占比",
                "入会率",
                "引导访问率",
                "成交新客占比",
                "members_gmv",
                "人均成交笔数",
                "人均成交金额",
                "natural_flow_amount",
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("float", errors="ignore")
                )

        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 宝贝主体报表 清洗失败", f'{str(e)}'])

        return df

    # 关键词报表
    def clean_and_transform_wanxiang_keywords_data(self, df):
        global cn
        try:
            column_mappings = {
                "日期": "datetimekey",
                "场景ID": "promotion_id",
                "场景名字": "promotion_name",
                "计划ID": "plan_id",
                "计划名字": "plan_name",
                "单元ID": "unit_id",
                "单元名字": "unit_name",
                "宝贝ID": "product_id",
                "宝贝名称": "product_name",
                "词类型": "keyword_type",
                "词ID/词包ID": "keyword_id",
                "词名字/词包名字": "keyword_name",
                "展现量": "impressions",
                "点击量": "clicktraffic",
                "花费": "spend",
                "点击率": "点击率",
                "千次展现花费": "千次展现花费",
                "总预售成交金额": "pre_sell_amount",
                "总预售成交笔数": "pre_sell_count",
                "直接预售成交金额": "dir_pre_sell_amount",
                "直接预售成交笔数": "dir_pre_sell_count",
                "间接预售成交金额": "idr_pre_sell_amount",
                "间接预售成交笔数": "idr_pre_sell_count",
                "直接成交金额": "dir_sell_amount",
                "间接成交金额": "idr_sell_amount",
                "总成交金额": "gmv",
                "总成交笔数": "gmv_count",
                "直接成交笔数": "dir_sell_count",
                "间接成交笔数": "idr_sell_count",
                "点击转化率": "点击转化率",
                "投入产出比": "投入产出比",
                "总成交成本": "总成交成本",
                "总购物车数": "shopcart_count",
                "直接购物车数": "dir_shopcart_count",
                "间接购物车数": "idr_shopcart_count",
                "加购率": "加购率",
                "收藏宝贝数": "coll_prod_count",
                "收藏店铺数": "coll_shop_count",
                "店铺收藏成本": "店铺收藏成本",
                "总收藏加购数": "coll_add_count",
                "总收藏加购成本": "总收藏加购成本",
                "宝贝收藏加购数": "coll_add_prod_count",
                "宝贝收藏加购成本": "宝贝收藏加购成本",
                "总收藏数": "coll_count",
                "宝贝收藏成本": "per_collection_rate",
                "宝贝收藏率": "宝贝收藏率",
                "加购成本": "加购成本",
                "拍下订单笔数": "take_order_count",
                "拍下订单金额": "take_order_amount",
                "直接收藏宝贝数": "dir_coll_prod_count",
                "间接收藏宝贝数": "idr_coll_prod_count",
                "优惠券领取量": "coupon_count",
                "购物金充值笔数": "recharge_count",
                "购物金充值金额": "recharge_amount",
                "旺旺咨询量": "wangwang_count",
                "引导访问量": "guided_visits",
                "引导访问人数": "guided_visitors",
                "引导访问潜客数": "potential_guided_visitors",
                "引导访问潜客占比": "引导访问潜客占比",
                "入会率": "入会率",
                "入会量": "enrollment_count",
                "引导访问率": "引导访问率",
                "深度访问量": "deep_visits",
                "平均访问页面数": "平均访问页面数",
                "成交新客数": "new_customers",
                "成交新客占比": "成交新客占比",
                "会员首购人数": "first_buy_members",
                "会员成交金额": "members_gmv",
                "会员成交笔数": "members_gmv_count",
                "成交人数": "buyer_count",
                "人均成交笔数": "人均成交笔数",
                '人均成交金额': '人均成交金额',
                '平均展现排名': 'avg_display_ranking'
            }
            df = df.rename(columns=column_mappings)

            # df["shop_name"] = self.config_obj["shop_name"]
            df["promotion_type"] = df["promotion_name"].copy()

            # 开始数据清洗
            # 将日期列的 / 换成 -
            columns_to_convert = ["datetimekey"]
            for cn in columns_to_convert:
                df[cn] = pd.to_datetime(
                    df[cn], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')

            # 字符串 转 整数， 去除 逗号, 去除 \n 字符
            columns_to_convert = [
                'pre_sell_count',
                'dir_pre_sell_count',
                'idr_pre_sell_count',
                'gmv_count',
                'dir_sell_count',
                'idr_sell_count',
                'shopcart_count',
                'dir_shopcart_count',
                'idr_shopcart_count',
                'coll_prod_count',
                'coll_shop_count',
                'coll_add_count',
                'coll_add_prod_count',
                'coll_count',
                'take_order_count',
                'dir_coll_prod_count',
                'idr_coll_prod_count',
                'recharge_count',
                'guided_visitors',
                'potential_guided_visitors',
                '平均访问页面数',
                'new_customers',
                'first_buy_members',
                'members_gmv_count',
                'buyer_count',
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )

            # 字符串转小数
            columns_to_convert = [
                'impressions',
                'clicktraffic',
                'spend',
                '点击率',
                '平均点击花费',
                '千次展现花费',
                'pre_sell_amount',
                'dir_pre_sell_amount',
                'idr_pre_sell_amount',
                'dir_sell_amount',
                'idr_sell_amount',
                'gmv',
                '点击转化率',
                '投入产出比',
                '总成交成本',
                '加购率',
                '店铺收藏成本',
                '总收藏加购成本',
                '宝贝收藏加购成本',
                'per_collection_rate',
                '宝贝收藏率',
                '加购成本',
                'take_order_amount',
                'coupon_count',
                'recharge_amount',
                'wangwang_count',
                'guided_visits',
                '引导访问潜客占比',
                '入会率',
                'enrollment_count',
                '引导访问率',
                'deep_visits',
                '成交新客占比',
                'members_gmv',
                '人均成交笔数',
                '人均成交金额',
                'avg_display_ranking'
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("float64", errors="ignore")
                )

            #
        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 关键词报表 清洗失败", f'{str(e)}'])
        
        return df

    # 人群报表
    def clean_and_transform_wanxiang_audience_data(self, df):
        global cn
        try:
            column_mappings = {
                "日期": "datetimekey",
                '场景ID': 'promotion_id',
                '场景名字': 'promotion_name',
                '计划ID': 'plan_id',
                '计划名字': 'plan_name',
                '单元ID': '单元id',
                '单元名字': '单元名字',
                "人群名字": "crowd_type",
                '主体ID': 'product_id',
                '主体类型': 'product_type',
                '主体名称': 'product_name',
                "展现量": "impressions",
                "点击量": "clicktraffic",
                "花费": "spend",
                "点击率": "点击率",
                "平均点击花费": "平均点击花费",
                "千次展现花费": "千次展现花费",
                "总预售成交金额": "pre_sell_amount",
                "总预售成交笔数": "pre_sell_count",
                "直接预售成交金额": "dir_pre_sell_amount",
                "直接预售成交笔数": "dir_pre_sell_count",
                "间接预售成交金额": "idr_pre_sell_amount",
                "间接预售成交笔数": "idr_pre_sell_count",
                "直接成交金额": "dir_sell_amount",
                "间接成交金额": "idr_sell_amount",
                "总成交金额": "gmv",
                "总成交笔数": "gmv_count",
                "直接成交笔数": "dir_sell_count",
                "间接成交笔数": "idr_sell_count",
                "点击转化率": "点击转化率",
                "投入产出比": "投入产出比",
                "总成交成本": "总成交成本",
                "总购物车数": "shopcart_count",
                "直接购物车数": "dir_shopcart_count",
                "间接购物车数": "idr_shopcart_count",
                "加购率": "加购率",
                "收藏宝贝数": "coll_prod_count",
                "收藏店铺数": "coll_shop_count",
                "店铺收藏成本": "店铺收藏成本",
                "总收藏加购数": "coll_add_count",
                "总收藏加购成本": "总收藏加购成本",
                "宝贝收藏加购数": "coll_add_prod_count",
                "宝贝收藏加购成本": "宝贝收藏加购成本",
                "总收藏数": "coll_count",
                "宝贝收藏成本": "宝贝收藏成本",
                "宝贝收藏率": "宝贝收藏率",
                "加购成本": "加购成本",
                "拍下订单笔数": "take_order_count",
                "拍下订单金额": "take_order_amount",
                "直接收藏宝贝数": "dir_coll_prod_count",
                "间接收藏宝贝数": "idr_coll_prod_count",
                "优惠券领取量": "coupon_count",
                "购物金充值笔数": "recharge_count",
                '购物金充值金额': 'recharge_amount',
                '旺旺咨询量': 'wangwang_count',
                "引导访问量": "guided_visits",
                "引导访问人数": "guided_visitors",
                "引导访问潜客数": "potential_guided_visitors",
                "引导访问潜客占比": "引导访问潜客占比",
                "入会率": "enrollment_rate",
                "入会量": "enrollment_count",
                "引导访问率": "引导访问率",
                "深度访问量": "deep_visits",
                "平均访问页面数": "平均访问页面数",
                "成交新客数": "new_customers",
                "成交新客占比": "成交新客占比",
                "会员首购人数": "first_buy_members",
                "会员成交金额": "members_gmv",
                "会员成交笔数": "members_gmv_count",
                "成交人数": "buyer_count",
                "人均成交笔数": "人均成交笔数",
                "人均成交金额": "人均成交金额",
            }
            df = df.rename(columns=column_mappings)

            # df["shop_name"] = self.config_obj["shop_name"]
            df["promotion_type"] = df["promotion_name"].copy()

            # 开始数据清洗
            # 将日期列的 / 换成 -
            columns_to_convert = ["datetimekey"]
            for cn in columns_to_convert:
                # df[cn] = pd.to_datetime(
                #     df[cn], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
                df[cn] = pd.to_datetime(df[cn], errors='coerce')
                df[cn] = df[cn].dt.strftime('%Y-%m-%d')
                df[cn] = df[cn].astype(str)

            # 字符串 转 整数， 去除 逗号, 去除 \n 字符
            columns_to_convert = [
                'pre_sell_count',
                'dir_pre_sell_count',
                'idr_pre_sell_count',
                'gmv_count',
                'dir_sell_count',
                'idr_sell_count',
                'shopcart_count',
                'dir_shopcart_count',
                'idr_shopcart_count',
                'coll_prod_count',
                'coll_shop_count',
                'coll_add_count',
                'coll_add_prod_count',
                'coll_count',
                'take_order_count',
                'dir_coll_prod_count',
                'idr_coll_prod_count',
                'recharge_count',
                'guided_visitors',
                'potential_guided_visitors',
                '平均访问页面数',
                'new_customers',
                'first_buy_members',
                'members_gmv_count',
                'buyer_count',
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )

            # 字符串转小数
            columns_to_convert = [
                'impressions',
                'clicktraffic',
                'spend',
                '点击率',
                '平均点击花费',
                '千次展现花费',
                'pre_sell_amount',
                'dir_pre_sell_amount',
                'idr_pre_sell_amount',
                'dir_sell_amount',
                'idr_sell_amount',
                'gmv',
                '点击转化率',
                '投入产出比',
                '总成交成本',
                '加购率',
                '店铺收藏成本',
                '总收藏加购成本',
                '宝贝收藏成本',
                '宝贝收藏加购成本',
                '宝贝收藏率',
                '加购成本',
                'take_order_amount',
                'coupon_count',
                'recharge_amount',
                'wangwang_count',
                'guided_visits',
                '引导访问潜客占比',
                'enrollment_count',
                '引导访问率',
                'deep_visits',
                '成交新客占比',
                'members_gmv',
                '人均成交笔数',
                '人均成交金额',
                'enrollment_rate'
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("float64", errors="ignore")
                )

            #
            pass
        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 人群报表 清洗失败", f'{str(e)}'])

        return df

    # 搜索排行
    def clean_and_transform_search_rank_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:
            self.log_arr.append(
                f"info/shs/【{self.get_date_time()}】: 开始清洗数据 ..."
            )

            # df.columns = df.columns.str.replace("\n", "")

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "k_rank",
                "visitor_count",
                "click_count"
            ]

            # for column in columns_to_convert:
            #     df[column] = pd.to_numeric(df[column].str.replace(",", ""), errors='coerce')

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({",": ""}, regex=True)
                    .astype("int64", errors="ignore")
                )
                # print(df[column])

            # df.to_excel('./output.xlsx', index=False, engine="xlsxwriter")

            columns_to_convert = [
                "click_rate",
                "conversion_rate"
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({",": ""}, regex=True)
                        .str.rstrip("%")
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.log_arr.append(
                f"success/shs/【{self.get_date_time()}】: 清洗数据成功 ..."
            )

            self.clean_and_transform_search_rank_data_bool = True

        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 搜索排行 清洗失败", f'{str(e)}'])

        return df

    # 人群top10
    def clean_and_transform_crowd_top_10_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:
            self.log_arr.append(
                f"info/shs/【{self.get_date_time()}】: 开始清洗数据 ..."
            )

            # df.columns = df.columns.str.replace("\n", "")

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "visitors",
                "paid_buyers",
                "tgi"
            ]

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({"-": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )
                # print(df[column])

            # df.to_excel('./output.xlsx', index=False, engine="xlsxwriter")

            columns_to_convert = [
                "paid_amount",
                "conversion_rate"
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({"-": 0.0}, regex=True)
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.log_arr.append(
                f"success/shs/【{self.get_date_time()}】: 清洗数据成功 ..."
            )

            self.clean_and_transform_crowd_top_10_data_bool = True

        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 人群top10 清洗失败", f'{str(e)}'])
            return False

        return df

    # 人群top20
    def clean_and_transform_crowd_top_20_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:

            # df.columns = df.columns.str.replace("\n", "")

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "visitors",
                "paid_buyers",
                "tgi"
            ]

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({"-": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )
                # print(df[column])

            # df.to_excel('./output.xlsx', index=False, engine="xlsxwriter")

            columns_to_convert = [
                "paid_amount",
                "conversion_rate"
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({"-": 0.0}, regex=True)
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.clean_and_transform_crowd_top_20_data_bool = True

        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 人群top10 清洗失败", f'{str(e)}'])
            return False

        return df

    # 人群
    def clean_and_transform_crowd_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:
            self.log_arr.append(
                f"info/shs/【{self.get_date_time()}】: 开始清洗数据 ..."
            )

            # df.columns = df.columns.str.replace("\n", "")

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                "visitors",
                "paid_buyers",
                "add_to_cart_count"
            ]

            for column in columns_to_convert:
                df[column] = (
                    df[column]
                    .replace({"-": 0}, regex=True)
                    .astype("int64", errors="ignore")
                )
                # print(df[column])

            # df.to_excel('./output.xlsx', index=False, engine="xlsxwriter")

            columns_to_convert = [
                "paid_amount",
            ]

            for column in columns_to_convert:
                try:
                    df[column] = (
                        df[column]
                        .replace({"-": 0.0}, regex=True)
                        .astype("float")
                    )
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.log_arr.append(
                f"success/shs/【{self.get_date_time()}】: 清洗数据成功 ..."
            )

            self.clean_and_transform_crowd_data_bool = True

        except Exception as e:
            
            print("# 数据清洗失败:", cn, df[cn], e)
            self.log_([f"error/shs/【{self.get_date_time()}】: 人群 清洗失败", f'{str(e)}'])
            return False

        return df

    # 创建数据库引擎
    def create_engine(self, db_obj=None):

        try:
            if db_obj is not None:
                self.config_obj['db_user'] = db_obj['db_user']
                self.config_obj['db_password'] = db_obj['db_password']
                self.config_obj['db_host'] = db_obj['db_host']
                self.config_obj['db_database'] = db_obj['db_database']
                
            database_url = f"mysql+pymysql://{self.config_obj['db_user']}:{self.config_obj['db_password']}@{self.config_obj['db_host']}/{self.config_obj['db_database']}"
            engine = create_engine(database_url)
            self.create_engine_bool = True
            return engine

        except Exception as e:
            print("# 创建数据库引擎失败!")
            self.log_([f"error/shs/【{self.get_date_time()}】: 创建数据库引擎失败!", f'{str(e)}'])
            return False

    def engine_insert_data(self, task_name="【商品每日数据】"):
        
        self.excel_data_df_count = 0
        data_count = 0
        transfersql = ''
        source_text = ''
        engine = self.create_engine()
        
        if self.create_engine_bool is False:
            return
        
        conn = engine.connect()
        if task_name == '[搜索排行]' or task_name == '[人群top10]' or task_name == '[人群]' or task_name == '[人群top20]':
            filelist = [
                f
                for f in os.listdir(f"{self.source_path}")
                if f"[生意参谋平台]{task_name}" in f
            ]
        else:
            filelist = [
                f
                for f in os.listdir(f"{self.source_path}")
                if f"【生意参谋平台】{task_name}" in f
            ]

        # print(filelist)
        for filename in filelist:
            
            # print(f'开始执行 {filename} 的数据！')
            
            try:

                excel_data_df = pd.read_excel(
                    f"{self.source_path}/" + filename)
                # print(filename, 'product data: ' + str(len(excel_data_df)))
                # 这里开始清洗数据
                if task_name == '【商品流量数据来源】' or task_name == '【店铺流量来源】':
                    match = re.search(r'\[(.*?)\]', filename)
                    source_text = match.group(1)
                    # print(f'# src: {source_text}')

                temptable = "temp"

                if task_name == "【店铺流量来源】":
                    tag = source_text
                    df_cleaned = self.clean_and_transform_shop_data(
                        excel_data_df, tag)

                    # df_cleaned.to_excel(
                    #     f'./output.xlsx',
                    #     index=False, engine='xlsxwriter')

                    # for index, row in df_cleaned.iterrows():
                    #     print(index, row)

                    table = "biz_shop_traffic"
                    key = [
                        "date",
                        "primary_source",
                        "secondary_source",
                        "tertiary_source",
                        'src'
                    ]
                    df_cleaned["date"] = filename.split("&&")[1]
                    df_cleaned["shop_name"] = self.config_obj["shop_name"]
                    # source_text
                    df_cleaned["src"] = source_text
                    if self.clean_and_transform_shop_data_bool is False:
                        return
                    df_cleaned.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                            select * from {temptable} t 
                                            where not exists 
                                            (select 1 from {table} m 
                                            where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                            )"""
                    # print(df_cleaned)
                    # print(transfersql)

                elif task_name == "【商品流量数据来源】":

                    self.log_arr.clear()
                    self.email_msg = ""

                    # 使用正则表达式提取数字
                    match = re.search(r"\b\d+\b", filename)
                    id_ = match.group()
                    dstring = filename.split("&&")[1]

                    df_cleaned = self.clean_and_transform_product_flowes_data(
                        excel_data_df
                    )
                    
                    # df_cleaned.to_excel(
                    #     f'./output.xlsx',
                    #     index=False, engine='xlsxwriter')

                    # print(
                    #     f"excel : 【生意参谋平台】【商品流量数据来源】【{id_}】{[source_text]}&&{dstring}&&{dstring}.xlsx  数据清洗完毕!"
                    # )

                    df_cleaned["product_id"] = id_
                    df_cleaned["shop_name"] = self.config_obj["shop_name"]
                    df_cleaned["src"] = source_text

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

                    df_cleaned.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                        where not exists 
                                        (select 1 from {table} m 
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                    # print(transfersql)

                elif task_name == '[搜索排行]':

                    df_cleaned = self.clean_and_transform_search_rank_data(
                        excel_data_df
                    )
                    temptable = "temp"
                    table = "biz_industry_keyword"
                    key = [
                        "statistic_date",
                        "category_lv1",
                        "category_lv2",
                        "category_lv3",
                        "k_type",
                        'keyword'
                    ]
                    df_cleaned.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                        where not exists 
                                        (select 1 from {table} m 
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                    # print(transfersql)

                elif task_name == '[人群top10]':

                    df_cleaned = self.clean_and_transform_crowd_top_10_data(
                        excel_data_df
                    )

                    if df_cleaned is False:
                        return False

                    table_name = 'biz_shop_audience_pruduct_t10'

                    transfersql = self.insert_data_sql(engine, df_cleaned, table_name,
                                               key=['year_month', 'product_id', 'product_name'], 
                                               keywords=['year_month'])

                    if transfersql is False:
                        print('<error>: [人群top10], sql 拼接失败!')
                        return False
                
                elif task_name == '[人群top20]':

                    df_cleaned = self.clean_and_transform_crowd_top_20_data(
                        excel_data_df
                    )

                    if df_cleaned is False:
                        return False

                    table_name = 'biz_shop_audience_channel_t20'

                    transfersql = self.insert_data_sql(engine, df_cleaned, table_name,
                                               key=['year_month'], 
                                               keywords=['year_month'])

                    if transfersql is False:
                        print('<error>: [人群top20], sql 拼接失败!')
                        return False
                
                elif task_name == '[人群]':

                    df_cleaned = self.clean_and_transform_crowd_data(
                        excel_data_df
                    )

                    if df_cleaned is False:
                        return False

                    table_name = 'biz_shop_audience_month'

                    transfersql = self.insert_data_sql(engine, df_cleaned, table_name,
                                               key=['shop_id', 'shop_name', 'year_month', 'crowd_type'],
                                               keywords=['year_month'])

                    if transfersql is False:
                        print('<error>: [人群], sql 拼接失败!')
                        return False

                else:

                    df_cleaned = self.clean_and_transform_product_data(
                        excel_data_df)
                                        
                    table = "biz_product_performance"
                    key = ["product_id", "statistic_date"]
                    df_cleaned["shop_name"] = self.config_obj["shop_name"]
                    df_cleaned["shop_id"] = self.config_obj['shop_id']
                    if self.clean_and_transform_product_data_bool is False:
                        return

                    df_cleaned.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                        where not exists 
                                        (select 1 from {table} m 
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                                        
                conn.execute(text(transfersql))

                conn.execute(text(f"drop table {temptable}"))
                
                if task_name == '【商品每日数据】':
                    
                    res = self.update_biz_product(date_=df_cleaned.iloc[1, df_cleaned.columns.get_loc('statistic_date')])
                    
                    if res:
                        print(f"{df_cleaned.iloc[1, df_cleaned.columns.get_loc('statistic_date')]}, biz_product 执行成功！")

                # 将成功写入的文件移入 成功的文件夹
                shutil.move(
                    f"{self.source_path}/" + filename,
                    f"{self.succeed_path}/" + filename,
                )

                self.engine_insert_data_bool = True
                data_count += len(excel_data_df)
                self.excel_data_df_count += len(excel_data_df)
                
                # print(f'{filename} 已执行完毕！')
                
                # time.sleep(0.5)
            
            except Exception as e:
                
                data_count += len(excel_data_df)
                
                shutil.move(
                    f"{self.source_path}/" + filename,
                    f"{self.failure_path}/" + filename,
                )
                
                # print("数据写入失败!")
                self.log_([f"error/shs/【{self.get_date_time()}】: 数据写入失败!", f'<error>: {str(e)}'])
            
            # self.email_msg += f"数据总条数: {data_count} 条\n"
            # self.email_msg += f"写入成功的条数: {self.excel_data_df_count} 条\n"
        
    # 写入数据库得封装函数
    def insert_data_sql(self, engine, df_cleaned, table_name, key: list, keywords=None):

        if keywords is None:
            keywords = []

        sql = False

        try:
            temptable = "temp"
            table = table_name

            df_cleaned.to_sql(
                name=temptable, con=engine, index=False, if_exists="replace"
            )

            sql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                                    select * from {temptable} t 
                                                    where not exists 
                                                    (select 1 from {table} m 
                                                    where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                                    )"""

            for item in keywords:
                sql = sql.replace(item, f"`{item}`")

            print(f'# sql 预览: {sql}')

        except Exception as e:
            
                print("# sql拼接失败!")
                self.log_([f"error/shs/【{self.get_date_time()}】: sql拼接失败!", f'{str(e)}'])

        return sql

     # 写入数据库的方法 [ 直接执行到数据库的方法 ]
    
    def insert_data(self, df_cleaned, table_name, key=[], add_col={}, keywords=None):
        
        mark = False
        
        engine = self.create_engine()
    def insert_data(self, df_cleaned, table_name, key=[], add_col={}, keywords=None, db_obj=None):
        
        mark = False
        
        engine = self.create_engine(db_obj=db_obj)
        
        if engine:
            
            if len(add_col) != 0:
                for key_, value in add_col.items():
                    df_cleaned[key_] = value
            
            if keywords is None:
                keywords = []
            
            res = False
            try:
                temptable = "temp"
                table = table_name

                df_cleaned.to_sql(
                    name=temptable, con=engine, index=False, if_exists="replace"
                )

                sql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                                select * from {temptable} t 
                                                where not exists 
                                                (select 1 from {table} m 
                                                where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                                )"""

                for item in keywords:
                    sql = sql.replace(item, f"`{item}`")

                # print(f'# sql 预览: {sql}')
                
                conn = engine.connect()
            
                conn.execute(text(sql))
                
                conn.execute(text(f"drop table {temptable}"))
                
                # print(f'# 数据写入成功。')

                mark = True
                
            except Exception as e:
                
                print("<error> 数据写入失败!")
                self.log_([f"error/shs/【{self.get_date_time()}】: 数据写入失败!", f'{str(e)}'])

        return mark

    # 数据写入完成后, 执行计算
    def calc(self, start_date_, end_date_):
        
        mark = False
        
        try:
            
            engine = self.create_engine()
            
            if engine is False:
                return
            
            sql_query = text("""
                    with t1 as (
                    select 
                    shop_name,  -- 店铺名称
                    product_id,  -- 商品ID
                    sum(visitors_count) as visitor_count,  -- 商品访客数
                    sum(views_count) as page_views,  -- 商品浏览量
                    sum(avg_stay_duration*visitors_count)/NULLIF(sum(visitors_count),0.0) as time_on_site,  -- 平均停留时长=总停留时长/商品访客数
                    100 - sum((100 - detail_bounce_rate) * visitors_count)/NULLIF(sum(visitors_count),0.0) as bounce_rate,  -- 商品详情页跳出率=1-点击详情页人数/详情页访客数
                    CAST(sum(add_to_cart_buyers) as FLOAT)/NULLIF(CAST(sum(visitors_count) as FLOAT),0.0) as add_rate,-- 加购率=商品加购人数/商品访客数
                    sum(order_placed_buyers) as payers,  -- 支付买家数
                    sum(order_quantity) as pay_pcs,  -- 支付件数
                    sum(order_amount) as pay_payment,  -- 支付金额
                    sum(order_amount)/NULLIF(sum(order_quantity),0.0) as avg_pay_value,  -- 件单价
                    CAST(sum(order_placed_buyers) as FLOAT)/NULLIF(CAST(sum(visitors_count) as FLOAT),0.0) as pay_cvr,  -- 商品支付转化率
                    sum(order_amount)/NULLIF(sum(visitors_count),0.0) as uv_value,  -- 访客平均价值
                    CAST(sum(search_driven_paid_buyers) as FLOAT)/NULLIF(CAST(sum(search_driven_visitors_count) as FLOAT),0.0) as search_guided_payment_cvr  -- 搜索引导支付转化率=搜索引导支付买家数/搜索引导访客数
                    from biz_product_performance
                    --  where datetimekey< DATEADD(DAY, 1-DATEPART(weekday, GETDATE()), CAST(GETDATE() AS date)) and datetimekey>= DATEADD(DAY, -13-DATEPART(weekday, GETDATE()), CAST(GETDATE() AS date))
                    where statistic_date <= :end_date and statistic_date >= DATE_ADD(:end_date, interval -6 day )
                    group by shop_name,  -- 店铺名称
                    product_id  -- 商品ID
                    ),t2 as (
                    select t1.shop_name, CAST(count(*) as float) as pro_num, max(visitor_count) as max_visitor_count,max(pay_payment) as max_pay_payment
                    from t1 left join biz_shop_weight w
                    on t1.shop_name = w.shop_name
                    where t1.pay_payment >= w.seven_day_gmv_threshold
                    group by t1.shop_name
                    ), t3 as (
                    select 
                    t1.shop_name,  -- 店铺名称
                    t1.product_id,  -- 商品ID
                    t1.avg_pay_value,  -- 件单价
                    t1.visitor_count,  -- 商品访客数
                    rank()over(partition by t1.shop_name order by t1.visitor_count asc)/t2.pro_num*w.visitor_count_weight+t1.visitor_count/t2.max_visitor_count*2 as visitor_count_score,  -- 商品访客数得分
                    time_on_site,  -- 平均停留时长=总停留时长/商品访客数
                    rank()over(partition by t1.shop_name order by t1.time_on_site asc)/t2.pro_num*w.avg_stay_duration_weight as time_on_site_score,  -- 平均停留时长得分
                    add_rate,-- 加购率=商品加购人数/商品访客数
                    rank()over(partition by t1.shop_name order by t1.add_rate asc)/t2.pro_num*w.add_to_cart_rate_weight as add_rate_score,-- 加购率得分
                    bounce_rate,  -- 商品详情页跳出率=1-点击详情页人数/详情页访客数
                    rank()over(partition by t1.shop_name order by t1.bounce_rate asc)/t2.pro_num*w.detail_bounce_rate_weight as bounce_rate_score,-- 商品详情页跳出率得分
                    payers,  -- 支付买家数
                    rank()over(partition by t1.shop_name order by t1.payers asc)/t2.pro_num*w.paid_buyer_count_weight as payers_score,-- 支付买家数得分
                    pay_pcs,  -- 支付件数
                    rank()over(partition by t1.shop_name order by t1.pay_pcs asc)/t2.pro_num*w.paid_quantity_weight as pay_pcs_score,-- 支付件数得分
                    pay_payment,  -- 支付金额
                    rank()over(partition by t1.shop_name order by t1.pay_payment asc)/t2.pro_num*w.paid_amount_weight + t1.pay_payment/t2.max_pay_payment*2 as pay_payment_score,-- 支付金额得分
                    pay_cvr,  -- 商品支付转化率
                    rank()over(partition by t1.shop_name order by t1.pay_cvr asc)/t2.pro_num*w.payment_conversion_rate_weight as pay_cvr_score,-- 商品支付转化得分
                    uv_value,  -- 访客平均价值
                    rank()over(partition by t1.shop_name order by t1.uv_value asc)/t2.pro_num*w.visitor_value_weight as uv_value_score,-- 访客平均价值得分
                    search_guided_payment_cvr,  -- 搜索引导支付转化率=搜索引导支付买家数/搜索引导访客数
                    rank()over(partition by t1.shop_name order by t1.search_guided_payment_cvr asc)/t2.pro_num*w.search_payment_conversion_rate_weight as search_pay_cvr_score -- 搜索引导支付转化率得分
                    -- case when add_rate>=w.market_add_rate and pay_cvr >=w.market_pay_cvr and uv_value>=w.market_uv_value then 'S+' else '' end as is_ss
                    from t1 
                    left join biz_shop_weight w
                    on t1.shop_name = w.shop_name
                    left join t2 
                    on t1.shop_name =t2.shop_name
                    where t1.pay_payment >=w.seven_day_gmv_threshold
                    )
                    select 
                    t3.shop_name,  -- 店铺名称
                    t3.product_id,  -- 商品ID
                    case when nullif(m.product_alias,'') is null then m.product_name else m.product_alias end as product_name,  -- 商品名称
                    m.category_name,
                    t3.visitor_count,  -- 商品访客数
                    t3.visitor_count_score,  -- 商品访客数得分
                    t3.time_on_site,  -- 平均停留时长=总停留时长/商品访客数
                    t3.time_on_site_score,  -- 平均停留时长得分
                    t3.add_rate,-- 加购率=商品加购人数/商品访客数
                    t3.add_rate_score,-- 加购率得分
                    t3.bounce_rate,  -- 商品详情页跳出率=1-点击详情页人数/详情页访客数
                    t3.bounce_rate_score,-- 商品详情页跳出率得分
                    t3.payers,  -- 支付买家数
                    t3.payers_score,-- 支付买家数得分
                    t3.pay_pcs,  -- 支付件数
                    t3.pay_pcs_score,-- 支付件数得分
                    t3.pay_payment,  -- 支付金额
                    t3.pay_payment_score,-- 支付金额得分
                    t3.pay_cvr,  -- 商品支付转化率
                    t3.pay_cvr_score,-- 商品支付转化得分
                    t3.uv_value,  -- 访客平均价值
                    t3.uv_value_score,-- 访客平均价值得分
                    t3.search_guided_payment_cvr,  -- 搜索引导支付转化率=搜索引导支付买家数/搜索引导访客数
                    t3.search_pay_cvr_score,
                    t3.visitor_count_score + t3.time_on_site_score + t3.add_rate_score + t3.bounce_rate_score + t3.payers_score +
                    t3.pay_pcs_score + t3.pay_payment_score + t3.pay_cvr_score + t3.uv_value_score + t3.search_pay_cvr_score as all_score,
                    case 
                    -- when t3.visitor_count_score + t3.time_on_site_score + t3.add_rate_score + t3.bounce_rate_score + t3.payers_score +
                    -- t3.pay_pcs_score + t3.pay_payment_score + t3.pay_cvr_score + t3.uv_value_score + t3.search_pay_cvr_score >10 and is_ss = 'S+' then 'S+'
                    when t3.visitor_count_score + t3.time_on_site_score + t3.add_rate_score + t3.bounce_rate_score + t3.payers_score +
                    t3.pay_pcs_score + t3.pay_payment_score + t3.pay_cvr_score + t3.uv_value_score + t3.search_pay_cvr_score >10 then 'S'
                    when t3.visitor_count_score + t3.time_on_site_score + t3.add_rate_score + t3.bounce_rate_score + t3.payers_score +
                    t3.pay_pcs_score + t3.pay_payment_score + t3.pay_cvr_score + t3.uv_value_score + t3.search_pay_cvr_score >7.5 then 'A'
                    when t3.visitor_count_score + t3.time_on_site_score + t3.add_rate_score + t3.bounce_rate_score + t3.payers_score +
                    t3.pay_pcs_score + t3.pay_payment_score + t3.pay_cvr_score + t3.uv_value_score + t3.search_pay_cvr_score >5 then 'B'
                    ELSE 'C' END as pallet,
                    case t3.payers when 0 then 0 else t3.pay_payment/t3.payers end as average_payment,  -- 客单价
                    t3.avg_pay_value  -- 件单价
                    from t3 left join biz_product m
                    on t3.shop_name = m.shop_name and t3.product_id = m.product_id
                    union all
                    select 
                    t1.shop_name,  -- 店铺名称
                    t1.product_id,  -- 商品ID
                    case when nullif(m.product_alias,'') is null then m.product_name else m.product_alias end as product_name,  -- 商品名称
                    m.category_name,
                    t1.visitor_count,  -- 商品访客数
                    0 as visitor_count_score,  -- 商品访客数得分
                    t1.time_on_site,  -- 平均停留时长=总停留时长/商品访客数
                    0 as time_on_site_score,  -- 平均停留时长得分
                    t1.add_rate,-- 加购率=商品加购人数/商品访客数
                    0 as add_rate_score,-- 加购率得分
                    t1.bounce_rate,  -- 商品详情页跳出率=1-点击详情页人数/详情页访客数
                    0 as bounce_rate_score,-- 商品详情页跳出率得分
                    t1.payers,  -- 支付买家数
                    0 as payers_score,-- 支付买家数得分
                    t1.pay_pcs,  -- 支付件数
                    0 as pay_pcs_score,-- 支付件数得分
                    t1.pay_payment,  -- 支付金额
                    0 as pay_payment_score,-- 支付金额得分
                    t1.pay_cvr,  -- 商品支付转化率
                    0 as pay_cvr_score,-- 商品支付转化得分
                    t1.uv_value,  -- 访客平均价值
                    0 as uv_value_score,-- 访客平均价值得分
                    t1.search_guided_payment_cvr,  -- 搜索引导支付转化率=搜索引导支付买家数/搜索引导访客数
                    0 as search_pay_cvr_score,
                    0 as all_score,
                    'D' AS pallet,
                    case t1.payers when 0 then 0 else t1.pay_payment/t1.payers end as average_payment,  -- 客单价
                    t1.avg_pay_value  -- 件单价
                    from t1 left join biz_shop_weight w
                    on t1.shop_name = w.shop_name
                    left join biz_product m
                    on t1.shop_name = m.shop_name and t1.product_id = m.product_id
                    where t1.pay_payment < w.seven_day_gmv_threshold
                """)
            
            metadata = MetaData()
            
            biz_product_classes = Table('biz_product_classes', metadata,
                Column('shop_name', String(255)),
                Column('product_id', String(191)),
                Column('product_name', String(255)),
                Column('category_name', String(100)),
                Column('visitor_count', DECIMAL(42, 0)),
                Column('visitor_count_score', DOUBLE),
                Column('time_on_site', DECIMAL(65, 6)),
                Column('time_on_site_score', DOUBLE),
                Column('add_rate', DOUBLE),
                Column('add_rate_score', DOUBLE),
                Column('bounce_rate', DECIMAL(58, 8)),
                Column('bounce_rate_score', DOUBLE),
                Column('payers', DECIMAL(42, 0)),
                Column('payers_score', DOUBLE),
                Column('pay_pcs', DECIMAL(42, 0)),
                Column('pay_pcs_score', DOUBLE),
                Column('pay_payment', DECIMAL(42, 2)),
                Column('pay_payment_score', DOUBLE),
                Column('pay_cvr', DOUBLE),
                Column('pay_cvr_score', DOUBLE),
                Column('uv_value', DECIMAL(46, 6)),
                Column('uv_value_score', DOUBLE),
                Column('search_guided_payment_cvr', DOUBLE),
                Column('search_pay_cvr_score', DOUBLE),
                Column('all_score', DOUBLE),
                Column('pallet', String(5)),
                Column('average_payment', DECIMAL(46, 6)),
                Column('avg_pay_value', DECIMAL(46, 6)),
                Column('statistic_date', Date),
            )
            
            start_date = start_date_
            end_date = end_date_
            
            with engine.connect() as conn:
                date_range = pd.date_range(start_date, end_date)
                # 初始化一个集合来跟踪已处理的(product_id, statistic_date)对
                processed_pairs = set()
                
                for date in date_range:
                    calc_date = date.strftime('%Y-%m-%d')
                    result = conn.execute(sql_query, {'end_date': calc_date})
                    rows = result.fetchall()

                    print("products: ", len(rows))

                    # 准备批量数据
                    batch_data = []
                    for row in rows:
                        # 假设product_id是第一个字段，可以根据实际情况调整
                        product_id = row[1]
                        pair = (product_id, calc_date)

                        # 如果这个组合已经处理过，则跳过
                        if pair in processed_pairs:
                            continue
                        
                        # 否则，添加到集合中以跟踪
                        processed_pairs.add(pair)

                        modified_row = list(row)  # 将行元组转换为列表
                        modified_row.append(calc_date)  # 添加新值
                        # 为每行创建一个字典
                        row_dict = {column.name: value for column, value in zip(biz_product_classes.columns, modified_row)}
                        batch_data.append(row_dict)

                    # 执行批量插入
                    if batch_data:  # 确保批量数据不为空
                        
                        print('开始执行 biz_product_classes 表的数据！')
                        
                        conn.execute(biz_product_classes.insert(), batch_data)
                        # 显式提交事务
                        conn.commit()
                        
                        print('表 biz_product_classes 已执行完毕!')   
                        engine.dispose()
                        mark = True
                
                    
        except Exception as e:
                        
            print(f'biz_product_classes 执行出错, error: {str(e)}')
            self.log_([f"error/shs/【{self.get_date_time()}】: biz_product_classes 执行出错!", f'{str(e)}'])
            
        return mark
    
    def calc_prepallet(self):
        
        mark = False
        
        try:
        
            engine = self.create_engine()
            
            sql1 = text("""
                        UPDATE biz_product_classes bpc1
                        JOIN biz_product_classes bpc2 
                        ON bpc1.statistic_date = DATE_ADD(bpc2.statistic_date, INTERVAL 7 DAY) and bpc1.product_id = bpc2.product_id
                        SET bpc1.pre_pallet = bpc2.pallet
                        WHERE bpc2.statistic_date IS NOT NULL;
                        """)
            
            sql2 = text("""
                        UPDATE biz_product_classes bpc1
                        JOIN biz_product_classes bpc2 
                        ON bpc1.statistic_date = DATE_ADD(bpc2.statistic_date, INTERVAL 7 DAY) and bpc1.product_id = bpc2.product_id
                        SET bpc1.pallet_change = CASE
                            WHEN bpc1.pallet = bpc2.pallet THEN 0
                            WHEN bpc1.pallet IS NULL OR bpc2.pallet IS NULL THEN 100
                            WHEN bpc1.pallet IN ('S', 'A', 'B', 'C', 'D') AND bpc2.pallet IN ('S', 'A', 'B', 'C', 'D') THEN
                                CASE 
                                    WHEN FIELD(bpc1.pallet, 'S', 'A', 'B', 'C', 'D') > FIELD(bpc2.pallet, 'S', 'A', 'B', 'C', 'D') THEN -1
                                    WHEN FIELD(bpc1.pallet, 'S', 'A', 'B', 'C', 'D') < FIELD(bpc2.pallet, 'S', 'A', 'B', 'C', 'D') THEN 1
                                    ELSE 0
                                END
                            ELSE 100
                        END
                        WHERE bpc2.statistic_date IS NOT NULL;
                        """)
            
            arr = [sql1, sql2]
            
            with engine.connect() as conn:
                
                for item in arr:
                    
                    print(f"表 biz_product_classes 开始执行: {item}")
                    conn.execute(item)
                    print(f"表 biz_product_classes 执行成功: {item}")
                
                conn.commit()
            
            mark = True
                
        except Exception as e:
            print(f'calc_prepallet 执行出错, error: {str(e)}')
            self.log_([f"error/shs/【{self.get_date_time()}】: calc_prepallet 执行出错!", f'{str(e)}'])
        
        return mark
    
    # 删除 biz_pallet_product 并从视图 v_pallet_product 重新写入
    def insert_biz_pallet_product_from_v_pallet_product(self):
        
        mark = False
        
        sql1 = text("delete from biz_pallet_product")
        
        sql2 = text(f"""
        insert into biz_pallet_product   
        ( select * from v_pallet_product )
        """)
        
        engine = self.create_engine()
        
        if engine is False:
            return
        
        try:
            
            with engine.connect() as conn:
                
                conn.execute(sql1)
                
                print(f'成功删除 biz_pallet_product 的数据!')
                
                conn.execute(sql2)
                
                print(f'成功从视图写入 biz_pallet_product 的数据!')
                
                conn.commit()
            
            engine.dispose()

            mark = True
            
        except Exception as e:
            
            print(f'biz_pallet_product 执行出错, error: {str(e)}')
            self.log_([f"error/shs/【{self.get_date_time()}】: biz_pallet_product 执行出错!", f'{str(e)}'])
        
        return mark
    
    def get_date_time(self, res="%Y-%m-%d %H:%M:%S"):
        # 获取当前日期和时间
        current_datetime = datetime.now()

        # 将日期和时间格式化为字符串
        date = current_datetime.strftime("%Y-%m-%d")
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        if res == "%Y-%m-%d":
            return date

        return formatted_datetime

    # 获取前一天的日期
    def get_before_day_datetime(self, tag="b", days_=1):
        # 获取当前日期
        current_date = datetime.now()

        # 计算前一天日期
        previous_date = current_date - timedelta(days=days_)

        today = current_date.strftime("%Y-%m-%d")
        before_day = previous_date.strftime("%Y-%m-%d")

        # print("当前日期:", current_date.strftime("%Y-%m-%d"))
        # print("前一天日期:", previous_date.strftime("%Y-%m-%d"))

        if tag == "t":
            return today
        else:
            return before_day
    
    def log_(self, msg_arr, type='error'):

        self.log_writer(msg_arr, type=type)

    def append_logArr(self, msg, separator="/shs/", type_="info"):
        self.log_arr.append(
            f"{type_}{separator}【{self.get_date_time()}】: {msg}")
        pass

    def log_writer(self, msg_arr, type='error'):

        with open(
            f'{self.logger_path}/log-{type}&&{self.get_date_time(res="%Y-%m-%d")}.txt',
            "a+",
            encoding="utf-8",
        ) as f:
            for item in msg_arr:
                item_ = item.split("/shs/")
                if len(item_) <= 1:
                    f.write(item_[0])
                    continue
                tag = item_[0]
                msg = item_[1]
                str_ = f"""# {tag} /【生意参谋平台】/ {msg} \n \n"""
                f.write(str_)
        pass

    def create_folder(self, hard_drive, folder_path):
        
        mark = False
        
        try:
            # 先检查盘符是否存在
            if os.path.exists(hard_drive):
                path = f"{hard_drive}{folder_path}"
                if not os.path.exists(path):
                    os.makedirs(path)
            else:        
                path = f"C:{folder_path}"
                if not os.path.exists(path):
                    os.makedirs(path)
            
            # source
            path_ = f"{path}/source"
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.source_path = path_

            path_ = f"{path}/succeed"
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.succeed_path = path_

            path_ = f"{path}/failure"
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.failure_path = path_

            path_ = f"{path}/failure/txt"
            if not os.path.exists(path_):
                os.makedirs(path_)

            path_ = f"{path}/log"
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.logger_path = path_
            self.create_folder_bool = True
            mark = True
            return mark
        
        except Exception as e:
            
            return mark

    def send_email(self, theme, email_msg_arr):

        mark = False

        chen_sir_email = "rcfcu2023@outlook.com"
        stone_email = "449649902@qq.com"

        emails = [stone_email]

        try:
            for item in emails:
                with yagmail.SMTP(
                    "19158865648@163.com", "Song7meng", host="smtp.163.com", port=465
                ) as yag:
                    yag.send(item, theme, email_msg_arr)

            print("# 邮箱发送成功.")
            mark = True
        except Exception as e:
            print(f"# 邮箱发送失败, error: {str(e)}")

        return mark

    # 商品每日数据的主函数
    def sycm_commodity_everyday_data(self):

        config_str = "sycmCommodityEverydayData"
        self.get_config(config_str)

        if self.get_config_bool is False:
            print("# error：配置项读取失败~")
            return

        page = WebPage()

        # 判断是否 登录
        if page.url == "chrome://newtab/":
            page.get(self.config_obj["url"])
            self.page = page
            self.visit_bool = True
        else:
            self.page = page
            self.visit_bool = True

        self.log_arr.clear()
        self.email_msg = ""

        print("程序开始自动化 每日商品数据！")
        self.email_msg = "任务名称：商品每日数据\n"

        # 开始登录
        self.sycm_login()

        if self.login_bool is False:
            return

        # 创建存储数据的文件夹
        self.create_folder("D:", self.config_obj["excel_storage_path"])

        if self.create_folder_bool is False:
            return

        mark = True
        if self.change_mode_index > 1:
            mark = False

        if self.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False

        # 下载excel
        self.down_load_excel(automatic_date=automatic_date)

        if self.down_load_excel_bool is False:
            print(self.down_load_excel_bool)
            return

        # 写入数据库
        self.engine_insert_data()

        # 写入日志
        self.log_(self.log_arr)

        print("程序执行成功， 执行结果请查看 log！")
        self.email_msg += "任务执行完毕：执行详细过程请查看log日志\n"
        self.email_msg += "**************************\n"

        print("开始发送邮件~！")
        self.email_msg_arr.clear()
        self.email_msg_arr.append(self.email_msg)

        self.send_email("【生意参谋平台】/ 商品每日数据", self.email_msg_arr)
        print("邮件发送成功~！")

    # 店铺流量来源
    def sycm_shop_flow_source(self, config_):
        
        config_str = "sycmShopTrafficSource"
        self.get_config_bool = self.get_configs(config_str, config_name=config_)
        port = self.config_obj['port']
        
        if self.get_config_bool is False:
            print("<error>：配置项读取失败~")
            return

        # co = ChromiumOptions()
        print(f'{self.config_obj["shop_name"]}: <info> 开始执行 [店铺流量来源]!')
        
        co = self.set_ChromiumOptions()

        co.set_address(f'127.0.0.1:{port}')

        page = WebPage(chromium_options=co)

        # 判断是否 登录
        if page.url == "chrome://newtab/":
            page.get(self.config_obj["url"])
            self.page = page
            self.visit_bool = True
        else:
            self.page = page
            self.visit_bool = True

        self.log_arr.clear()
        self.email_msg = ""

        # print("程序开始自动化 店铺流量来源！")
        self.email_msg = "任务名称：店铺流量来源\n"
        # 开始登录
        self.sycm_login()

        if self.login_bool is False:
            return

        # 创建存储数据的文件夹
        self.create_folder("D:", self.config_obj["excel_storage_path"])

        if self.create_folder_bool is False:
            return

        if self.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False

        # 下载excel
        self.down_load_excel(
            task_name="【店铺流量来源】[每一次访问来源]", automatic_date=automatic_date
        )

        if self.down_load_excel_bool is False:
            return

        self.down_load_excel(
            task_name="【店铺流量来源】[第一次访问来源]", automatic_date=automatic_date
        )

        if self.down_load_excel_bool is False:
            return
        #
        self.down_load_excel(
            task_name="【店铺流量来源】[最后一次访问来源]", automatic_date=automatic_date
        )

        if self.down_load_excel_bool is False:
            return

        # # 写入数据库
        self.engine_insert_data(task_name="【店铺流量来源】")

        pass

    # --------------------------------------为调用简单而封装
    # 执行程序的封装处理
    # 访问 sycm
    def visit_sycm(self, task_name="【店铺流量来源】", config=''):

        self.log_arr.clear()
        self.email_msg = ""
        
        mark = False
        
        # print(f"程序开始自动化 {task_name}！")
        
        obj = self.get_configs_return_obj(key='browserPort', config_name=config)
        
        if obj is False:
            print('<error> 没有端口号，不能启动!')
        else:
            port = obj['port']
            # print(f'端口号：{port}')
            
        # co = ChromiumOptions()
        
        co = self.set_ChromiumOptions()

        co.set_address(f'127.0.0.1:{port}')

        try:
            page = WebPage(chromium_options=co)
            
            # print(f'1. browser_id: {page._browser_id} && tab_id: {page.tab_id} && browser_url: {page._browser_url} && {page.tabs_count}')
            
            # 判断是否 登录
            if page.url == "chrome://newtab/":
                page.get(self.config_obj["url"])
                self.page = page
                self.visit_bool = True
            else:
                self.page = page
                self.visit_bool = True

            mark = True
            # print('访问生意参谋成功！')
            
        except Exception as e:
 
            self.log_([f"error/shs/【{self.get_date_time()}】: 生意参谋访问失败, 下面为错误信息."])
            self.log_([f"error/shs/【{self.get_date_time()}】: <error> {str(e)}"])

        return mark

    # 登录生意参谋
    def login_sycm(self, task_name="商品每日数据"):
        
        mark = False

        # 开始登录
        self.sycm_login(task_name=task_name)

        if self.login_bool:
            
            mark = True
            # print(f'登录成功！')
            
            # 检查是否需要发送验证码
            code_ = self.page('#J_GetCode')
            if code_:
                code_.click()
                # 开始等待用户输入验证码
                # input('请在页面上输入验证码以后，输入随意字符继续任务：')
                self.page.wait(1200)
        else:

            self.log_([f"error/shs/【{self.get_date_time()}】: 访问或者登录失败!"])

        return mark
    
    # 创建存储数据的文件夹
    def create_storage_data_folder(self):
        mark = False

        self.create_folder("D:", self.config_obj["excel_storage_path"])
        print(self.config_obj["excel_storage_path"])

        if self.create_folder_bool:
            mark = True

        return mark

    # 下载excel
    def down_load_excel_data(self, automatic_date, task_name="【商品每日数据】"):

        mark = False

        # 下载excel
        self.down_load_excel(task_name=task_name, mode='s',
                             automatic_date=automatic_date)

        if self.down_load_excel_bool:
            mark = True

        return mark

    # 写入数据库
    def insert_data_in_db(self, task_name="【商品每日数据】"):
        mark = False
        # 写入数据库
        self.engine_insert_data(task_name=task_name)

        if self.engine_insert_data_bool:
            mark = True

        return mark

    # 开始发送邮件
    def send_emails(self, theme="商品每日数据"):

        self.email_msg_arr.clear()
        self.email_msg_arr.append(self.email_msg)

        res = self.send_email(f"【生意参谋平台】/ {theme}", self.email_msg_arr)

        return res

    # --------------------------------------仅此而已

    # 商品数据来源
    def commodity_data_source(self):

        config_str = "sycmCommodityTrafficSource"
        self.get_config(config_str)

        if self.get_config_bool is False:
            print("# error：配置项读取失败~")
            return

        # 访问生意参谋
        res = self.visit_sycm()
        #
        if res is False:
            return
        #
        # # 清空 备用
        self.log_arr.clear()
        self.email_msg = ""

        res = self.login_sycm(task_name="【商品流量数据来源】")

        if res is False:
            return
        #
        # # 创建数据存储的文件夹
        res = self.create_storage_data_folder()

        if res is False:
            return

        if self.config_obj['automatic_date'] == '自动计算前一天':
            automatic_date = True
        else:
            automatic_date = False

        # # # 下载数据
        res = self.commodity_flow_data(task_tag='[每一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return

        res = self.commodity_flow_data(task_tag='[第一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return

        res = self.commodity_flow_data(task_tag='[最后一次访问来源]', automatic_date=automatic_date)

        if res is False:
            return

        # 写入数据库
        res = self.insert_data_in_db(task_name="【商品流量数据来源】")

        if res is False:
            return
        
        # 开始计算 biz_pallet_product
        if False:
            self.calc_start_date = '2024-04-08'
            self.calc_end_date = '2024-04-08'
        
        res = self.calc(start_date_=self.calc_start_date, end_date_=self.calc_end_date)
        
        if res is False:
            return
        
        self.calc_prepallet()
        
        # 1. 删除 biz_pallet_product 指定日期的信息
        # 2. 从视图 v_pallet_product 写入相关数据
        
        self.insert_biz_pallet_product_from_v_pallet_product()
        
        self.email_msg += "任务执行完毕：执行详细过程请查看log日志\n"
        self.email_msg += "******************************\n"

        self.send_emails(theme="商品流量数据来源")

        # 写入日志
        # self.log_(self.log_arr, task_name="【商品数据来源】")

    def wanxiang_table(self, table_name):
        
        # config_str = table_name
        
        # self.get_config(config_str)

        # if self.get_config_bool is False:
        #     print("# error：配置项读取失败~")
        #     return

        if (
            "https" in self.config_obj["excel_url"]
            or "http" in self.config_obj["excel_url"]
        ):
            # 自动化方式
            pass
        else:
            # 手动方式excel_url
            excel_url = self.source_path
            file_list = []

            if table_name == 'wanxiang_product':
                file_list = [
                    file
                    for file in os.listdir(excel_url)
                    if os.path.isfile(os.path.join(excel_url, file))
                    and "宝贝主体报表_" in file
                    and file.endswith("csv")
                ]
            elif table_name == 'wanxiang_keywords':
                file_list = [
                    file
                    for file in os.listdir(excel_url)
                    if os.path.isfile(os.path.join(excel_url, file))
                    and "关键词报表_" in file
                    and file.endswith("csv")
                ]
                dtype_options = {'花费': 'float64', '平均点击花费': 'float64',
                                 '入会量': 'int64', '人均成交金额': 'float64'}

            else:
                file_list = [
                    file
                    for file in os.listdir(excel_url)
                    if os.path.isfile(os.path.join(excel_url, file))
                    and "人群报表_" in file
                    and file.endswith("csv")
                ]
            # print(file_list)
            clean_df = None

            for file in file_list:
                try:
                    if table_name == 'wanxiang_keywords':
                        # 定义数据类型选项
                        csv_data = pd.read_csv(f"{excel_url}\\{file}", encoding="gbk", dtype=dtype_options,
                                               low_memory=False, na_values='\\N')
                    else:
                        csv_data = pd.read_csv(
                            f"{excel_url}\\{file}", encoding="gbk")

                    # print(csv_data)

                except Exception as e:

                    if table_name == 'wanxiang_keywords':
                        # 定义数据类型选项
                        csv_data = pd.read_csv(f"{excel_url}\\{file}", encoding="utf-8", dtype=dtype_options,
                                               low_memory=False)
                    else:
                        csv_data = pd.read_csv(
                            f"{excel_url}\\{file}", encoding="utf-8")
                    
                    # print(csv_data)

                pass

                if table_name == 'wanxiang_product':
                    # 清洗宝贝主体报表
                    clean_df = self.clean_and_transform_wanxiang_product_data(
                        csv_data)

                elif table_name == 'wanxiang_keywords':
                    # 清洗关键词报表
                    clean_df = self.clean_and_transform_wanxiang_keywords_data(
                        csv_data)

                    pass
                else:
                    clean_df = self.clean_and_transform_wanxiang_audience_data(csv_data)
                    pass

                # clean_df.to_excel(
                #     f'./{file.replace("csv", "xlsx")}',
                #     index=False, engine='xlsxwriter')

                engine = self.create_engine()
                conn = engine.connect()

                try:
                    # 拼接sql
                    table = table_name
                    temptable = "temp"
                    key = ["datetimekey", "plan_id", "product_id"]

                    clean_df.to_sql(
                        name=temptable, con=engine, index=False, if_exists="replace"
                    )
                    transfersql = f"""insert into {table} ({",".join(clean_df.columns)})
                                        select * from {temptable} t
                                        where not exists
                                        (select 1 from {table} m
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                    # print(clean_df)
                    # print(f"# sql 已拼接完成：{transfersql}")
                    conn.execute(text(transfersql))
                    conn.execute(text(f"drop table {temptable}"))
                    print(f"# {file}, sql 执行成功")
                    shutil.move(
                        f"{self.source_path}/" + file,
                        f"{self.succeed_path}/" + file,
                    )
                except Exception as e:
                    
                    print(f"# {table_name}, sql 执行失败")
                    print("# 失败的数据行:")
                    print(
                        clean_df.loc[
                            clean_df.apply(
                                lambda row: conn.execute(
                                    text(transfersql), row.to_dict()
                                ).scalar()
                                == 1,
                                axis=1,
                            )
                        ]
                    )
                    print("错误信息：" + str(e))
                    shutil.move(
                        f"{self.source_path}/" + file,
                        f"{self.failure_path}/" + file,
                    )
        pass

    # 检验是否是数字
    def is_number(self, param):
        # 使用正则表达式匹配数字的模式
        pattern = r'^[-+]?[0-9]*\.?[0-9]+$'
        return re.match(pattern, param)
    
    # 返回一个修改过参数的新的url
    def new_url(self, dict_: dict, oldurl):
    
        obj = {}
        
        try:
            # 解析 URL
            parsed_url = urlparse(oldurl)
            query_params = parse_qs(parsed_url.query)
            
            for key, value in dict_.items():
                # print(key, value)
                query_params[key] = [value]
            
            # 将查询参数转换回查询字符串
            new_query = urlencode(query_params, doseq=True)
            
            # 重建 URL
            new_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))
            
            obj['mark'] = True
            obj['url'] = new_url
            
        except Exception as e:
            
            obj['mark'] = False
            obj['url'] = str(e)
        
        return obj
    
    # 修改和新增biz_product
    def update_biz_product(self, date_=''):
               
        mark = False
        
        str_ = '(SELECT MAX(statistic_date) FROM biz_product_performance)' if date_ == '' else date_ 
        
        sql1 = text(f"""
                    UPDATE biz_product bp
                    JOIN (
                        SELECT 
                            bpp.product_id, 
                            bpp.product_status
                        FROM biz_product_performance bpp
                        WHERE bpp.statistic_date = '{str_}'
                    ) as latest_status ON bp.product_id = latest_status.product_id
                    SET bp.product_status = latest_status.product_status;
                    """)
        
        sql2 = text(f"""
                    INSERT INTO biz_product (product_id, product_status, responsible, product_name, product_alias, shop_name, shop_id)
                    SELECT 
                        bpp.product_id, 
                        bpp.product_status,
                        '{self.config_obj["principal"]}',
                        product_name,
                        sku,
                        shop_name,
                        shop_id
                    FROM biz_product_performance bpp
                    WHERE bpp.statistic_date = '{str_}'
                    AND NOT EXISTS (
                        SELECT 1 FROM biz_product WHERE product_id = bpp.product_id
                    );
                    """)
        
        engine = self.create_engine()
        
        if engine is False:
            return
        
        try:
            
            with engine.connect() as conn:
                
                conn.execute(sql1)
                
                conn.execute(sql2)
                
                conn.commit()
            
            engine.dispose()

            mark = True
            
        except Exception as e:
            
            self.log_([f"error/shs/【{self.get_date_time()}】: biz_product 执行出错!", f'{str(e)}'])
        
        return mark
    
    # 拿到商品ID的数据
    def get_item_id(self, date_):
        
        obj = {}
        res = ''
        arr = []
        date_str = date_.strftime("%Y-%m-%d")
        
        sql = text(f"select product_id from biz_product_performance where product_status = '当前在线' and statistic_date = '{date_str}'")
        
        engine = self.create_engine()
        
        if engine is False:
            obj['mark'] = False
            obj['result'] = 'engin error'
            return
        
        try:
            with engine.connect() as conn:
                
                res = conn.execute(sql)
            
            for row in res:
                arr.append(row[0])
            
            obj['mark'] = True
            obj['result'] = arr
            
            return obj
        
        except Exception as e:
            
            obj['mark'] = False
            obj['result'] = str(e)
            return obj
    
    # 日志模块
    def logging(self, msg, mode='info'):
        # 配置日志输出的格式
        logging.basicConfig(
            filename=f"{self.logger_path}/app.log",
            format='%(asctime)s - %(levelname)s - %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO  # 设置日志级别为 INFO
        )
        # 记录日志信息
        if mode == 'debug':
            
            logging.debug(msg)
            
        elif mode == 'info':
            
            logging.info(msg)
            
        elif mode == 'warning':
            
            logging.warning(msg)
            
        elif mode == 'error':
            
            logging.error(msg)
            
        else:
            logging.critical(msg)
    
    def run(self):
        # 商品每日数据  每天
        # self.sycm_commodity_everyday_data()
        # 店铺流量数据  每天
        # self.sycm_shop_flow_source()

        # 宝贝主体报表 (数据库表名命名) 每月
        # self.wanxiang_table(table_name='wanxiang_product')

        # 人群报表
        # self.wanxiang_table(table_name='wanxiang_audience')

        # self.wanxiang_table(table_name='wanxiang_keywords')

        # 商品流量数据  每天
        # self.commodity_data_source()
        
        self.get_config_name()
    
    def set_ChromiumOptions(self):
        
        co = ChromiumOptions()
        
        # 禁止所有弹出窗口
        co.set_pref(arg='profile.default_content_settings.popups', value='0')
        
        # 隐藏是否保存密码的提示
        co.set_pref('credentials_enable_service', False)
        
        return co

    # 检查是否需要重新开启浏览器或者访问需要的网址
    def whether_the_url_exists_in_the_browser(self, page, url_str):
        
        if url_str in page._browser_url:
            
            return {
                'mark': True,
                'url': page._browser_url,
                'browser_id': page.tab_id,
                'msg': 'url已存在'
            }
            
        else:
            
            return {
                'mark': False,
                'url': page._browser_url,
                'browser_id': page.tab_id,
                'msg': 'url不存在'
            }
            
    def test_action(self, date_):
        self.get_configs('sycmCommodityEverydayData')
        res = self.get_item_id(date_=date_)
        print(res['result'])
        print(len(res['result']))
        # for row in res['result']:
        #     print(row)

if __name__ == "__main__":
    test = base_action()
    test.run()
