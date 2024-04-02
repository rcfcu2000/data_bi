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
import concurrent.futures
from sqlalchemy import create_engine, text
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import configparser
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions


class labipaiRPA:
    def __init__(self):

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

        # 源数据存放路径
        self.source_path = ''
        # 写入成功的存放路径
        self.succeed_path = ''
        # 写入失败的存放路径
        self.failure_path = ''
        # 日志文件
        self.logger_path = ''
        # 日志内容 arr
        self.log_arr = []
        # 发送邮件内容的字符串拼接
        self.email_msg = ''
        self.email_msg_arr = []

        # 这个参数主要用于控制 顺序执行的方法是否需要 改变模式, 改变模式的目的是为了下载excel数据的返回格式正确. 为 1 需要改变, 不为1 就不需要
        self.change_mode_index = 1

    # 获取可用端口
    def __find_free_port(self):

        res = {}

        try:
            # 创建一个临时套接字
            temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_socket.bind(('0.0.0.0', 0))  # 绑定到一个随机的空闲端口
            temp_socket.listen(1)  # 监听连接
            port = temp_socket.getsockname()[1]  # 获取实际绑定的端口
            self.port_list.append(port)
            temp_socket.close()  # 关闭临时套接字

            res['mark'] = True
            res['result'] = port

            return res

        except Exception as e:

            res['mark'] = False
            res['result'] = str(e)

            return res

        pass

    # 读取配置文件
    def get_config(self, key):

        local_config_obj = None

        config = configparser.ConfigParser()

        config.read('./config/my_config.ini', encoding='utf-8')

        # 获取用户名和密码

        self.get_config_bool = True

        self.config_obj['url'] = config.get(key, 'url')
        self.config_obj['excel_url'] = config.get(key, 'excel_url')
        self.config_obj['excel_storage_path'] = config.get(key, 'excel_storage_path')
        self.config_obj['start_date'] = config.get(key, 'start_date')
        self.config_obj['end_date'] = config.get(key, 'end_date')
        self.config_obj['user_name'] = config.get(key, 'user_name')
        self.config_obj['pass_word'] = config.get(key, 'pass_word')
        self.config_obj['shop_name'] = config.get(key, 'shop_name')
        self.config_obj['db_host'] = config.get(key, 'db_host')
        self.config_obj['db_user'] = config.get(key, 'db_user')
        self.config_obj['db_password'] = config.get(key, 'db_password')
        self.config_obj['db_database'] = config.get(key, 'db_database')
        self.config_obj['db_raise_on_warnings'] = config.get(key, 'db_raise_on_warnings')

        local_config_obj = self.config_obj

        return local_config_obj

        pass

    # get port
    def get_port(self):
        return self.__find_free_port()
        pass

    def sycm_login(self, task_name='【商品每日数据】'):

        self.log_arr.append(
            f'info/shs/【{self.get_date_time()}】: 开始自动化每日商品数据 ...')

        if self.visit_bool is False:
            self.log_arr.append(f'error/shs/【{self.get_date_time()}】: 浏览器访问失败!')
            return

        self.log_arr.append(f'success/shs/【{self.get_date_time()}】: 访问成功 !')
        try:
            if self.page.url == self.config_obj['url']:
                self.log_arr.append(f'info/shs/【{self.get_date_time()}】: 现在开始登录 ...!')
                self.page('#fm-login-id').input(self.config_obj['user_name'])
                self.page('#fm-login-password').input(self.config_obj['pass_word'])
                # 这里可以做一个判断，用于新老登录界面的异常捕获
                iframe = self.page('#alibaba-login-box')
                res = iframe('.fm-button fm-submit password-login').click()
                # print(f'res{res}')

                self.log_arr.append(f'success/shs/【{self.get_date_time()}】: 登录成功... 强制等待 5 秒钟 !')

                self.page.wait(5)
            else:

                self.log_arr.append(f'info/shs/【{self.get_date_time()}】: 已经登录，无需再次登录... 强制等待 5 秒钟 !')

                self.page.wait(5)

            self.login_bool = True

        except Exception as e:

            self.log_arr.append(f'error/shs/【{self.get_date_time()}】: 登录失败, error: {str(e)}')

            self.log_(self.log_arr,  task_name=task_name)
        pass

    def down_load_excel(self, task_name='【商品每日数据】', change_mode=False, automatic_date=True):

        date_ = ''
        path_ = ''

        # self.get_config('sycmCommodityEverydayData')

        # 改变模式 切换为 S 模式：requests
        try:
            if change_mode:
                self.page.change_mode()
                self.change_mode_index += 1
                self.log_arr.append(
                    f'info/shs/【{self.get_date_time()}】: 切换为 requests（session_page） 模式')

            # 将开始日期和结束日期替换成 start_date
            url = self.config_obj['excel_url']
            re_str = r'dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})'
            date_match = re.search(re_str, url)
            start_date_str, end_date_str = date_match.groups()

            date1_str = self.config_obj['start_date']
            date2_str = self.config_obj['end_date']
            next_day_str = date1_str

            modified_url = re.sub(re_str, f'dateRange={next_day_str}%7C{next_day_str}', url)

            # 计算日期
            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 开始计算日期 ...')
            date_format = "%Y-%m-%d"
            date1 = datetime.strptime(date1_str, date_format)
            date2 = datetime.strptime(date2_str, date_format)

            # 这是要循环的次数
            days_difference = (date2 - date1).days

            if automatic_date:
                # automatic_date 这是代表是否执行当天日期的前一天, 适用于: 商品每日数据, 店铺每日流量以及需要每天去取数的模块, 取前一天的日期
                next_day_str = self.get_before_day_datetime()
                days_difference = 1

            elif days_difference == 0:

                days_difference = 1

            else:

                days_difference += 1

            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 当前下载日期： {date1_str}...')

            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 循环下载次数为 {days_difference}...')

            # print(modified_url)
            # print(days_difference)
            self.email_msg += f'下载excel起始日期：{date1_str}\n'
            self.email_msg += f'下载excel结束日期：{date2_str}\n'
            count = 0
            index = 4

            if task_name == '【店铺流量来源】':
                index = 5

            for i in range(0, days_difference):

                try:
                    date_ = next_day_str

                    self.log_arr.append(
                        f'info/shs/【{self.get_date_time()}】: 第{i+1}次, 开始访问链接 ...')

                    self.page.get(modified_url)

                    # self.page.raw_data 相当于 requests的response.content
                    if self.page.raw_data:
                        # print(self.page.raw_data)
                        self.log_arr.append(
                            f'success/shs/【{self.get_date_time()}】: 第{i + 1}次, 内容已下载, 开始保存为 excel...')
                        # print(self.page.raw_data)
                        # 这里开始要做一些变化
                        dtype_mapping = {'商品ID': str}
                        df = pd.read_excel(BytesIO(self.page.raw_data), dtype=dtype_mapping, header=index)
                        excel_path = f"{self.source_path}/【生意参谋平台】{task_name}&&{next_day_str}&&{next_day_str}.xlsx"
                        df.to_excel(
                            excel_path,
                            index=False, engine='xlsxwriter')

                        self.log_arr.append(
                            f'success/shs/【{self.get_date_time()}】: 第{i + 1}次, excel保存成功, 保存路径为：{excel_path}')

                        pass

                    # 开始计算下一个日期
                    if days_difference > 1:

                        self.log_arr.append(
                            f'info/shs/【{self.get_date_time()}】: 开始计算下一个日期 ...')

                        re_str = r'dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})'
                        date_match = re.search(re_str, modified_url)
                        date1_str, date2_str = date_match.groups()
                        date1 = datetime.strptime(date1_str, date_format)
                        next_day = date1 + timedelta(days=1)
                        next_day_str = next_day.strftime(date_format)
                        modified_url = re.sub(re_str, f'dateRange={next_day_str}%7C{next_day_str}', modified_url)

                        self.log_arr.append(
                            f'info/shs/【{self.get_date_time()}】: 下一个日期为： {next_day_str}...')

                        pass

                    # 休眠一定时间
                    self.log_arr.append(
                        f'info/shs/【{self.get_date_time()}】: 强制休眠随机6秒内...')
                    time.sleep(random.randint(0, 6))
                    count += 1

                except Exception as e:
                    self.log_arr.append(
                        f'error/shs/【{self.get_date_time()}】: 下载excel出错, 错误信息：{str(e)}, 当前出错日期：{date_} ...')
                    self.email_msg += f'下载excel失败, 日期：{date_}\n'
                    print(f"# error: 下载excel出错，{e}")

                    self.fail_to_txt(next_day_str, task_name='【商品数据来源】')

                    continue

            self.everyday_data_loadExcel_bool = True
            self.log_arr.append(
                f'success/shs/【{self.get_date_time()}】: 数据已下载完成 下载文件个数：{count} , 下载个数与循环次数是否相等：{days_difference == count} ...')
            self.email_msg += f'下载excel个数：{count}\n'
            self.down_load_excel_bool = True

        except Exception as e:

            self.log_arr.append(
                f'error/shs/【{self.get_date_time()}】: 下载excel出错, error：{str(e)} ...')
            self.email_msg = f'下载excel出错, 错误信息：{str(e)}\n'
            self.log_(self.log_arr)
            pass

        pass

    # 商品流量数据
    def commodity_flow_data(self, mode='s', automatic_date=True):

        # self.get_config('sycmCommodityTrafficSource')
        # 读取excel全部商品数据
        folder_path = f'./commodity_source_data'
        file_list = [file for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
        print(file_list)

        if len(file_list) > 1:
            print(f'# error: {folder_path} 文件夹中有多个文件，请检查后再启动。')
            return False

        excel_data = pd.read_excel(f'{folder_path}/{file_list[0]}', usecols="B, H")
        excel_data_df = excel_data.sort_values(by=['商品ID'], ascending=False)

        print(f'# 商品数据表总数: {len(excel_data_df)}')

        # 遍历DataFrame中的每一行数据
        # for index, row in excel_data_df.iterrows():
        #     # 可以访问每一行的数据，例如：
        #     id = row['商品ID']
        #     # 如果有其他列，可以继续类似地访问，例如：
        #     status_ = row['商品状态']
        #
        #     # 在这里进行你的处理或打印
        #     print(f"商品ID: {id}, 列H的数据: {status_}")

        start_date = self.config_obj['start_date']
        end_date = self.config_obj['end_date']
        date_range = []
        datetime_ = ''
        if automatic_date:
            datetime_ = self.get_before_day_datetime()
        else:
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

                if good_status == '已下架':
                    print("商品已下架: ", good_id, str(total_counter))
                    continue

                args = ((good_id, date) for date in date_range)

                # 使用 executor.map 同时启动多个线程执行任务
                executor.map(self.download_item_keywords, args)

                num = random.randint(15, 30)

                print(f'# 强制等待 {num} 秒钟... ')
                time.sleep(num)

            # 等待当前这10个任务执行完毕
            executor.shutdown(wait=True)

        pass

    def download_item_keywords(self, args):
        id_ = args[0]
        dates_ = args[1]
        print(f'{id_}: {dates_}')

        dstring = dates_.strftime('%Y-%m-%d')
        # print(f'字符串类型的date: {dstring}')
        url = self.config_obj['excel_url']

        match = re.search(r'dateRange=(\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2})', url)
        original_date_range = match.group(1)

        new_date_range = f"{dstring}|{dstring}"
        modified_url = re.sub(r'dateRange=\d{4}-\d{2}-\d{2}\|\d{4}-\d{2}-\d{2}', f'dateRange={new_date_range}', url)

        item_ids = re.findall(r'itemId=(\d+)', modified_url)
        new_item_ids = [id_, id_]
        modified_url = re.sub(r'itemId=\d+', lambda x: f'itemId={new_item_ids.pop(0)}', modified_url)

        print(modified_url)

        self.page.get(modified_url)

        if self.page.raw_data:
            try:
                # print(self.page.raw_data)
                df = pd.read_excel(BytesIO(self.page.raw_data), header=5)
                excel_path = f"{self.source_path}/【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx"
                # print(self.source_path)
                df.insert(df.shape[1], '日期', dstring)
                df.to_excel(
                    excel_path,
                    index=False, engine='xlsxwriter')
                print(f"excel : 【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx  保存成功!")
            except Exception as e:
                print(f"excel : 【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx  下载失败!")
                # excel_path = f"{self.failure_path}/error【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx"
                # 创建 txt 记录下载失败的excel , 自动重下
                res = self.fail_to_txt(dstring, task_name='【商品数据来源】', id_=id_)

                print(res)
        pass

    # 判断是否是数字
    def is_number(self, s):
        # 使用正则表达式匹配数字的模式
        pattern = r'^[-+]?[0-9]*\.?[0-9]+$'
        return re.match(pattern, s)

    # 将下载失败的时候的记录卸载txt中
    def fail_to_txt(self, dstring, task_tag='download', task_name='【name is none】', id_=None):
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
                    f'{self.failure_path}/txt/{task_tag}_error-【生意参谋平台】{task_name}_{self.get_date_time(res="%Y-%m-%d")}',
                    'a', encoding='utf-8') as f:
                f.write(f'{id_}:{dstring}\n')
                print(
                    f"excel : excel下载失败, 失败记录已写入 {self.failure_path}/txt/download_error-【生意参谋平台】【商品数据来源】_{self.get_date_time(res='%Y-%m-%d')}!")
            return {
                'mark': True,
                'message': '失败信息写入成功'
            }

        except Exception as e:

            print(f'# error: {task_tag}, {str(e)}')
            return {
                'mark': False,
                'message': f'# 失败信息写入失败: {task_tag}, {str(e)} '
            }

    # 清洗商品每日数据
    def clean_and_transform_product_flowes_data(self, df):

        # 数据映射和转换
        column_mappings = {
            '一级来源': 'source_type_1',
            '二级来源': 'source_type_2',
            '三级来源': 'source_type_3',
            '访客数': 'visitors_count',
            '浏览量': 'views_count',
            '支付金额': 'paid_amount',
            '浏览量占比': 'view_rate',
            '店内跳转人数': 'in_store_transfers',
            '跳出本店人数': 'outbound_exits',
            '收藏人数': 'favorited_users',
            '加购人数': 'add_to_carts',
            '下单买家数': 'buyers_placed_orders',
            '下单转化率': 'order_conversion_rate',
            '支付件数': 'paid_quantity',
            '支付买家数': 'total_paid_buyers',
            '支付转化率': 'pay_conversion_rate',
            '直接支付买家数': 'direct_paid_buyers',
            '收藏商品-支付买家数': 'favorited_and_paid_buyers',
            '粉丝支付买家数': 'fans_paid_buyers',
            '加购商品-支付买家数': 'add_to_cart_and_paid_buyers',
            '日期': 'statistic_date'
        }
        df = df.rename(columns=column_mappings)

        # 将包含逗号的字符串字段转换为整数
        columns_to_convert = [
            'paid_quantity', 'visitors_count', 'in_store_transfers', 'outbound_exits',
            'favorited_users', 'add_to_carts', 'buyers_placed_orders',
            'direct_paid_buyers', 'favorited_and_paid_buyers',
            'fans_paid_buyers', 'add_to_cart_and_paid_buyers', 'views_count',
        ]

        for column in columns_to_convert:
            df[column] = df[column].replace({',': ''}, regex=True).astype('int64', errors='ignore')

        # 将包含逗号和%的字符串字段转换为浮点数
        columns_to_convert = [
            'view_rate', 'pay_conversion_rate', 'order_conversion_rate', 'paid_amount',
        ]

        for column in columns_to_convert:
            try:
                df[column] = df[column].replace({',': ''}, regex=True).str.rstrip('%').astype('float')
            except Exception as e:
                # print(column, e)
                df[column] = 0.0

        return df

    # 清洗数据 并做好与数据库的映射
    # 商品每日数据
    def clean_and_transform_product_data(self, df):
        # 处理 Excel 文件的前几行无用数据
        try:
            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 开始清洗数据 ...')

            df = df.dropna(subset=['商品ID'])  # 假设 '商品ID' 列没有空值，用它来确定数据开始的行

            # 重命名列以匹配数据库字段
            df = df.rename(columns={
                '统计日期': 'statistic_date',
                '商品ID': 'product_id',
                '商品名称': 'product_name',
                '货号': 'sku',
                '商品状态': 'product_status',
                '商品标签': 'product_tags',
                '商品访客数': 'visitors_count',
                '商品浏览量': 'views_count',
                '平均停留时长': 'avg_stay_duration',
                '商品详情页跳出率': 'detail_bounce_rate',
                '商品收藏人数': 'collection_count',
                '商品加购件数': 'add_to_cart_quantity',
                '商品加购人数': 'add_to_cart_buyers',
                '下单买家数': 'order_placed_buyers',
                '下单件数': 'order_quantity',
                '下单金额': 'order_amount',
                '下单转化率': 'order_conversion_rate',
                '支付买家数': 'paid_buyers',
                '支付件数': 'paid_quantity',
                '支付金额': 'paid_amount',
                '商品支付转化率': 'payment_conversion_rate',
                '支付新买家数': 'new_buyers_paid',
                '支付老买家数': 'returning_buyers_paid',
                '老买家支付金额': 'returning_buyers_paid_amount',
                '聚划算支付金额': 'group_buy_paid_amount',
                '访客平均价值': 'visitor_value',
                '成功退款金额': 'successful_refund_amount',
                '竞争力评分': 'competitiveness_score',
                '年累计支付金额': 'yearly_cumulative_paid_amount',
                '月累计支付金额': 'monthly_cumulative_paid_amount',
                '月累计支付件数': 'monthly_cumulative_paid_quantity',
                '搜索引导支付转化率': 'search_driven_payment_conversion_rate',
                '搜索引导访客数': 'search_driven_visitors_count',
                '搜索引导支付买家数': 'search_driven_paid_buyers',
                '结构化详情引导转化率': 'structured_detail_conversion_rate',
                '结构化详情引导成交占比': 'structured_detail_transaction_ratio',
                # 如果还有其他列，请在此添加映射
                # '店铺ID': 'shop_id'
            })

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                'visitors_count', 'views_count', 'collection_count', 'add_to_cart_quantity',
                'add_to_cart_buyers', 'order_placed_buyers', 'order_quantity',
                'paid_buyers', 'paid_quantity',
                'monthly_cumulative_paid_quantity', 'search_driven_visitors_count',
                'search_driven_paid_buyers'
            ]

            for column in columns_to_convert:
                df[column] = df[column].replace({',': ''}, regex=True).astype('int64', errors='ignore')

            # 将包含逗号和%的字符串字段转换为浮点数
            columns_to_convert = [
                'avg_stay_duration', 'successful_refund_amount',
                'returning_buyers_paid_amount', 'group_buy_paid_amount', 'successful_refund_amount',
                'paid_amount', 'order_amount',
                'yearly_cumulative_paid_amount', 'monthly_cumulative_paid_amount',
            ]
            for column in columns_to_convert:
                df[column] = df[column].replace({',': ''}, regex=True)

            columns_to_convert = [
                'detail_bounce_rate', 'order_conversion_rate', 'payment_conversion_rate',
                'search_driven_payment_conversion_rate', 'structured_detail_conversion_rate',
                'structured_detail_transaction_ratio', 'competitiveness_score',
            ]

            for column in columns_to_convert:
                try:
                    df[column] = df[column].replace({',': ''}, regex=True).str.rstrip('%').astype('float')
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            self.log_arr.append(
                f'success/shs/【{self.get_date_time()}】: 清洗数据成功 ...')

            self.clean_and_transform_product_data_bool = True

        except Exception as e:
            self.log_arr.append(
                f'error/shs/【{self.get_date_time()}】: 清洗数据失败, error: {str(e)} ...')
            self.email_msg = f'清洗数据失败, error: {str(e)}\n'
            self.log_(self.log_arr)

        return df

    # 店铺流量来源
    def clean_and_transform_shop_data(self, df):

        try:
            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 开始清洗数据 ...')

            mapping = {
                df.columns[0]: 'primary_source',
                df.columns[1]: 'secondary_source',
                df.columns[2]: 'tertiary_source',
                df.columns[3]: 'visitors_count',
                df.columns[4]: 'visitors_change',
                df.columns[5]: 'order_amount',
                df.columns[6]: 'order_amount_change',
                df.columns[7]: 'buyers_placed_orders',
                df.columns[8]: 'buyers_change',
                df.columns[9]: 'conversion_rate_order',
                df.columns[10]: 'conversion_rate_order_change',
                df.columns[11]: 'paid_amount',
                df.columns[12]: 'paid_amount_change',
                df.columns[13]: 'buyers_paid',
                df.columns[14]: 'buyers_paid_change',
                df.columns[15]: 'conversion_rate_payment',  # Placeholder mapping
                df.columns[16]: 'conversion_rate_payment_change',  # Placeholder mapping
                df.columns[17]: 'average_order_value',  # Placeholder mapping
                df.columns[18]: 'aov_change',  # Placeholder mapping
                df.columns[19]: 'uv_value',  # Placeholder mapping
                df.columns[20]: 'uv_value_change',  # Placeholder mapping
                df.columns[21]: 'followers_count',  # Placeholder mapping
                df.columns[22]: 'followers_change',  # Placeholder mapping
                df.columns[23]: 'product_favorites_count',  # Placeholder mapping
                df.columns[24]: 'product_favorites_change',  # Placeholder mapping
                df.columns[25]: 'add_to_cart_count',  # Placeholder mapping
                df.columns[26]: 'add_to_cart_change',  # Placeholder mapping
                df.columns[27]: 'new_visitors_count',  # Placeholder mapping
                df.columns[28]: 'new_visitors_change',  # Placeholder mapping
                df.columns[29]: 'direct_pay_buyers_count',  # Placeholder mapping
                df.columns[30]: 'favorited_product_pay_buyers_count',  # Placeholder mapping
                df.columns[31]: 'follower_pay_buyers_count',  # Placeholder mapping
                df.columns[32]: 'add_to_cart_pay_buyers_count',  # Placeholder mapping
                df.columns[33]: 'homepage_guide_visitor_count',  # Placeholder mapping
                df.columns[34]: 'homepage_guide_visitor_change',  # Placeholder mapping
                df.columns[35]: 'short_video_guide_visitor_count',  # Placeholder mapping
                df.columns[36]: 'product_guide_visitor_count',  # Placeholder mapping
                df.columns[37]: 'grassroot_transaction_count',  # Placeholder mapping
                df.columns[38]: 'grassroot_transaction_amount',  # Placeholder mapping
                df.columns[39]: 'ad_transaction_amount',  # Placeholder mapping
                df.columns[40]: 'ad_transaction_amount_change',  # Placeholder mapping
                df.columns[41]: 'ad_clicks_count',  # Placeholder mapping
                df.columns[42]: 'ad_clicks_change',  # Placeholder mapping
            }

            # 重命名列以匹配数据库字段
            df = df.rename(columns=mapping)

            # 将包含逗号的字符串字段转换为整数
            columns_to_convert = [
                'visitors_count', 'buyers_placed_orders', 'buyers_paid', 'followers_count',
                'product_favorites_count', 'add_to_cart_count', 'new_visitors_count',
                'direct_pay_buyers_count', 'follower_pay_buyers_count', 'favorited_product_pay_buyers_count',
                'add_to_cart_pay_buyers_count',
                'homepage_guide_visitor_count', 'short_video_guide_visitor_count',
                'product_guide_visitor_count', 'grassroot_transaction_count', 'ad_clicks_count',
            ]

            for column in columns_to_convert:
                try:
                    df[column] = df[column].apply(lambda x: 0.0 if x == '-' else x)
                    df[column] = df[column].replace({',': ''}, regex=True).astype('int64')
                except Exception as e:
                    # print(column, e)
                    df[column] = 0

            # 将包含逗号和%的字符串字段转换为浮点数
            columns_to_convert = [
                'order_amount', 'paid_amount',
                'average_order_value', 'uv_value', 'grassroot_transaction_amount',
                'ad_transaction_amount',
            ]
            for column in columns_to_convert:
                try:
                    df[column] = df[column].apply(lambda x: 0.0 if x == '-' else x)
                    df[column] = df[column].replace({',': ''}, regex=True).astype('float')
                except Exception as e:
                    # print(column, e)
                    df[column] = 0.0

            columns_to_convert = [
                'order_amount_change', 'ad_clicks_change', 'ad_transaction_amount_change',
                'homepage_guide_visitor_change', 'new_visitors_change', 'paid_amount_change',
                'add_to_cart_change', 'product_favorites_change', 'buyers_paid_change',
                'followers_change', 'uv_value_change', 'aov_change', 'conversion_rate_payment',
                'conversion_rate_payment_change', 'visitors_change', 'buyers_change',
                'conversion_rate_order', 'conversion_rate_order_change',
            ]

            for column in columns_to_convert:
                try:
                    df[column] = df[column].apply(lambda x: 0.0 if x == '-' else x)
                    df[column] = df[column].replace({',': ''}, regex=True).str.rstrip('%').astype('float')
                except Exception as e:
                    print(column, df[column], e)
                    df[column] = 0.0

            self.clean_and_transform_shop_data_bool = True

        except Exception as e:

            self.log_arr.append(
                f'error/shs/【{self.get_date_time()}】: 清洗数据失败, error: {str(e)} ...')
            self.email_msg = f'清洗数据失败, error: {str(e)}\n'
            self.log_(self.log_arr)

            pass

        return df

    # 创建数据库引擎
    def create_engine(self):

        try:
            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 开始创建数据库引擎 ...')

            database_url = f"mysql+pymysql://{self.config_obj['db_user']}:{self.config_obj['db_password']}@{self.config_obj['db_host']}/{self.config_obj['db_database']}"
            engine = create_engine(database_url)
            self.create_engine_bool = True
            return engine

        except Exception as e:
            self.log_arr.append(
                f'error/shs/【{self.get_date_time()}】: 数据库引擎创建失败 {str(e)}... ')
            self.email_msg = f'数据库引擎创建失败: {str(e)}\n'
            self.log_(self.log_arr)

        pass

    def engine_insert_data(self, task_name='【商品每日数据】'):

        engine = self.create_engine()

        if self.create_engine_bool is False:
            print('创建数据库引擎出错了！')
            return

        conn = engine.connect()
        filelist = [f for f in os.listdir(f"{self.source_path}") if f'【生意参谋平台】{task_name}' in f]
        for filename in filelist:
            try:

                excel_data_df = pd.read_excel(f"{self.source_path}/" + filename)

                self.log_arr.append(
                    f'info/shs/【{self.get_date_time()}】: 准备写入数据, 文件名: {filename}, 数据总量为：{len(excel_data_df)} ...')
                # print(filename, 'product data: ' + str(len(excel_data_df)))
                # 这里开始清洗数据
                temptable = "temp"

                if task_name == '【店铺流量来源】':
                    df_cleaned = self.clean_and_transform_shop_data(excel_data_df)
                    table = 'biz_shop_traffic'
                    key = ["date", "primary_source", "secondary_source", "tertiary_source"]
                    df_cleaned['date'] = filename.split('&&')[1]
                    df_cleaned['shop_name'] = self.config_obj['shop_name']
                    if self.clean_and_transform_shop_data_bool is False:
                        return
                    df_cleaned.to_sql(name=temptable, con=engine, index=False, if_exists='append')
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                            select * from {temptable} t 
                                            where not exists 
                                            (select 1 from {table} m 
                                            where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                            )"""
                    print(df_cleaned)
                    print(f'# sql 已拼接完成：{transfersql}')

                elif task_name == '【商品数据来源】':

                    # 使用正则表达式提取数字
                    match = re.search(r'\b\d+\b', filename)
                    id_ = match.group()
                    dstring = filename.split('&&')[1]

                    df_cleaned = self.clean_and_transform_product_flowes_data(excel_data_df)

                    print(f"excel : 【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx  数据清洗完毕!")

                    df_cleaned['product_id'] = id_
                    df_cleaned['shop_name'] = self.config_obj['shop_name']

                    temptable = "temp"
                    table = "biz_product_traffic_stats"
                    key = ["product_id", "statistic_date", "source_type_1", "source_type_2", "source_type_3"]

                    df_cleaned.to_sql(name=temptable, con=engine, index=False, if_exists='append')
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                        where not exists 
                                        (select 1 from {table} m 
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                    print(transfersql)
                    conn.execute(text(transfersql))
                    print(f"excel : 【生意参谋平台】【商品数据来源】【{id_}】&&{dstring}&&{dstring}.xlsx  数据写入完毕!")
                    conn.execute(text(f"drop table {temptable}"))
                    pass

                else:
                    df_cleaned = self.clean_and_transform_product_data(excel_data_df)
                    table = 'biz_product_performance'
                    key = ["product_id", "statistic_date"]
                    df_cleaned['shop_name'] = self.config_obj['shop_name']
                    df_cleaned['shop_id'] = '999'
                    if self.clean_and_transform_product_data_bool is False:
                        return

                    df_cleaned.to_sql(name=temptable, con=engine, index=False, if_exists='replace')
                    transfersql = f"""insert into {table} ({",".join(df_cleaned.columns)}) 
                                        select * from {temptable} t 
                                        where not exists 
                                        (select 1 from {table} m 
                                        where {"and".join([f" t.{col} = m.{col} " for col in key])}
                                        )"""
                    print(df_cleaned)
                    print(f'# sql 已拼接完成：{transfersql}')

                self.log_arr.append(
                    f'info/shs/【{self.get_date_time()}】: 开始写入数据, 如果数据存在就不写入, 写入逻辑在sql中 ...')

                # print(transfersql)
                conn.execute(text(transfersql))

                print(f'# sql 已执行！')

                self.log_arr.append(
                    f'success/shs/【{self.get_date_time()}】: sql执行成功 ...')

                conn.execute(text(f"drop table {temptable}"))

                self.log_arr.append(
                    f'success/shs/【{self.get_date_time()}】: 删除临时表 ...')

                # 以 网站商品数据为依据
                # if task_name == '【商品每日数据】':
                #     shutil.copyfile(f"{self.source_path}/" + filename, f'./commodity_source_data/{filename}')
                #     pass

                # 将成功写入的文件移入 成功的文件夹
                shutil.move(f"{self.source_path}/" + filename, f"{self.succeed_path}/" + filename)

                self.log_arr.append(
                    f'success/shs/【{self.get_date_time()}】: 将写入成功的文件剪切至  {self.succeed_path}/ {filename} ...')

                self.email_msg += f'数据写入总量（以数据库为准，sql执行了，重复的不写入）: {len(excel_data_df)} 条\n'
                self.engine_insert_data_bool = True

                # time.sleep(1)

            except Exception as e:

                shutil.move(f"{self.source_path}/" + filename, f"{self.failure_path}/" + filename)

                self.log_arr.append(
                    f'error/shs/【{self.get_date_time()}】: 数据写入失败, 将写入失败的文件剪切至  {self.failure_path}/ {filename} ,  error: {str(e)}...')

                self.log_(self.log_arr, '【店铺流量来源】')

                self.email_msg += f'数据写入失败，已将写入失败的文件剪切至  {self.failure_path}/ {filename} ,  error: {str(e)}'
                print(f'# 数据写入失败 error: {str(e)}')

                continue

            self.log_arr.append(
                f'info/shs/【{self.get_date_time()}】: 至此, 数据写入完毕 ...')
        pass

    def get_date_time(self, res='%Y-%m-%d %H:%M:%S'):
        # 获取当前日期和时间
        current_datetime = datetime.now()

        # 将日期和时间格式化为字符串
        date = current_datetime.strftime('%Y-%m-%d')
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        if res == '%Y-%m-%d':
            return date

        return formatted_datetime

    # 获取前一天的日期
    def get_before_day_datetime(self, tag='b'):
        # 获取当前日期
        current_date = datetime.now()

        # 计算前一天日期
        previous_date = current_date - timedelta(days=1)

        today = current_date.strftime("%Y-%m-%d")
        before_day = previous_date.strftime("%Y-%m-%d")

        # print("当前日期:", current_date.strftime("%Y-%m-%d"))
        # print("前一天日期:", previous_date.strftime("%Y-%m-%d"))

        if tag == 't':
            return today
        else:
            return before_day

        pass

    def log_(self, msg_arr, task_name='【商品每日数据】'):

        self.log_writer(msg_arr, task_name)

    def append_logArr(self, msg, separator='/shs/', type_='info'):
        self.log_arr.append(
            f'{type_}{separator}【{self.get_date_time()}】: {msg}')
        pass

    def log_writer(self, msg_arr, task_name):

        with open(f'{self.logger_path}/log-{self.get_date_time(res="%Y-%m-%d")}.txt', 'w', encoding='utf-8') as f:
            for item in msg_arr:
                item_ = item.split('/shs/')
                tag = item_[0]
                msg = item_[1]
                str_ = f"""# {tag} /【生意参谋平台】/{task_name} /  {msg} \n \n"""
                f.write(str_)
        pass

    def create_folder(self, hard_drive, folder_path):
        # 先检查盘符是否存在
        if os.path.exists(hard_drive):
            path = f'{hard_drive}{folder_path}'

            if not os.path.exists(path):

                os.makedirs(path)
                print(f'# 创建文件夹，{path} 所需文件夹已创建！')

            else:
                print(f'# 创建文件夹，{path} 文件夹已存在，无需创建！')

            # source
            path_ = f'{path}/source'
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.source_path = path_

            path_ = f'{path}/succeed'
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.succeed_path = path_

            path_ = f'{path}/failure'
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.failure_path = path_

            path_ = f'{path}/log'
            if not os.path.exists(path_):
                os.makedirs(path_)

            self.logger_path = path_

            self.create_folder_bool = True

        else:
            print(f'# 创建文件夹，{hard_drive} 盘符不存在，请检查！')
        pass

    def send_email(self, theme, email_msg_arr):

        mark = False

        chen_sir_email = 'rcfcu2023@outlook.com'
        stone_email = '449649902@qq.com'

        emails = [stone_email]

        try:
            for item in emails:
                with yagmail.SMTP('19158865648@163.com', 'Song7meng', host='smtp.163.com', port=465) as yag:
                    yag.send(item, theme, email_msg_arr)

            print('# 邮箱发送成功.')
            mark = True
        except Exception as e:
            print(f'# 邮箱发送失败, error: {str(e)}')

        return mark
        pass

    # 商品每日数据的主函数
    def sycm_commodity_everyday_data(self):

        config_str = 'sycmCommodityEverydayData'
        self.get_config(config_str)

        if self.get_config_bool is False:
            print('# error：配置项读取失败~')
            return

        page = WebPage()

        # 判断是否 登录
        if page.url == 'chrome://newtab/':
            page.get(self.config_obj['url'])
            self.page = page
            self.visit_bool = True
        else:
            self.page = page
            self.visit_bool = True

        self.log_arr.clear()
        self.email_msg = ''

        print('程序开始自动化 每日商品数据！')
        self.email_msg = '任务名称：商品每日数据\n'

        # 开始登录
        self.sycm_login()

        if self.login_bool is False:
            return

        # 创建存储数据的文件夹
        self.create_folder('D:', self.config_obj['excel_storage_path'])

        if self.create_folder_bool is False:
            return

        mark = True
        if self.change_mode_index > 1:
            mark = False

        # 下载excel
        self.down_load_excel(change_mode=mark)

        if self.down_load_excel_bool is False:
            print(self.down_load_excel_bool)
            return

        # 写入数据库
        self.engine_insert_data()

        # 写入日志
        self.log_(self.log_arr)

        print('程序执行成功， 执行结果请查看 log！')
        self.email_msg += '任务执行完毕：执行详细过程请查看log日志\n'
        self.email_msg += '**************************\n'

        print('开始发送邮件~！')
        self.email_msg_arr.clear()
        self.email_msg_arr.append(self.email_msg)

        self.send_email('【生意参谋平台】/ 商品每日数据', self.email_msg_arr)
        print('邮件发送成功~！')

        pass

    # 店铺流量来源
    def sycm_shop_flow_source(self):

        config_str = 'sycmShopTrafficSource'
        self.get_config(config_str)

        if self.get_config_bool is False:
            print('# error：配置项读取失败~')
            return

        page = WebPage()

        # 判断是否 登录
        if page.url == 'chrome://newtab/':
            page.get(self.config_obj['url'])
            self.page = page
            self.visit_bool = True
        else:
            self.page = page
            self.visit_bool = True

        self.log_arr.clear()
        self.email_msg = ''

        print('程序开始自动化 店铺流量来源！')
        self.email_msg = '任务名称：店铺流量来源\n'
        # 开始登录
        self.sycm_login()

        if self.login_bool is False:
            return
        #
        # 创建存储数据的文件夹
        self.create_folder('D:', self.config_obj['excel_storage_path'])

        if self.create_folder_bool is False:
            return

        mark = True
        if self.change_mode_index > 1:
            mark = False
        #
        # 下载excel
        self.down_load_excel(change_mode=mark, task_name='【店铺流量来源】')

        if self.down_load_excel_bool is False:
            return

        # 写入数据库
        self.engine_insert_data(task_name='【店铺流量来源】')
        # 写入日志
        self.log_(self.log_arr, task_name='【店铺流量来源】')

        print('程序执行成功， 执行结果请查看 log！')

        self.email_msg += '任务执行完毕：执行详细过程请查看log日志\n'
        self.email_msg += '******************************\n'
        print('开始发送邮件~！')
        self.email_msg_arr.clear()
        self.email_msg_arr.append(self.email_msg)
        self.send_email('【生意参谋平台】/ 店铺流量来源', self.email_msg_arr)
        print('邮件发送成功~！')

        pass

    # --------------------------------------为调用简单而封装
    # 执行程序的封装处理
    # 访问 sycm
    def visit_sycm(self, task_name='【店铺流量来源】'):

        mark = False

        try:
            page = WebPage()
            # 判断是否 登录
            if page.url == 'chrome://newtab/':
                page.get(self.config_obj['url'])
                self.page = page
                self.visit_bool = True
            else:
                self.page = page
                self.visit_bool = True

            mark = True

        except Exception as e:
            print(f'访问生意参谋失败, error: {str(e)}')

            self.log_arr.append(
                f'error/shs/【{self.get_date_time()}】: 访问生意参谋失败, error: {str(e)}')

            self.log_(self.log_arr, task_name)

        return mark
        pass

    # 登录生意参谋
    def login_sycm(self, task_name='商品每日数据'):
        mark = False

        print(f'程序开始自动化 {task_name}！')
        self.email_msg = f'任务名称：{task_name}\n'

        # 开始登录
        self.sycm_login(task_name=task_name)

        if self.login_bool:
            mark = True

        return mark
        pass

    # 创建存储数据的文件夹
    def create_storage_data_folder(self):
        mark = False

        self.create_folder('D:', self.config_obj['excel_storage_path'])
        print(self.config_obj['excel_storage_path'])

        if self.create_folder_bool:
            mark = True

        return mark
        pass

    # 下载excel
    def down_load_excel_data(self, task_name='【商品每日数据】'):

        tag = False

        mark = True
        if self.change_mode_index > 1:
            mark = False

        # 下载excel
        self.down_load_excel(task_name=task_name, change_mode=mark)

        if self.down_load_excel_bool:
            tag = True

        return tag
        pass

    # 写入数据库
    def insert_data_in_db(self, task_name='【商品每日数据】'):
        mark = False
        # 写入数据库
        self.engine_insert_data(task_name=task_name)

        if self.engine_insert_data_bool:
            mark = True

        return mark
        pass

    # 开始发送邮件
    def send_emails(self, theme='商品每日数据'):

        self.email_msg_arr.clear()
        self.email_msg_arr.append(self.email_msg)

        res = self.send_email(f'【生意参谋平台】/ {theme}', self.email_msg_arr)

        return res
        pass
    # --------------------------------------仅此而已

    # 商品数据来源
    def commodity_data_source(self):

        config_str = 'sycmCommodityTrafficSource'
        self.get_config(config_str)

        if self.get_config_bool is False:
            print('# error：配置项读取失败~')
            return

        # 访问生意参谋
        res = self.visit_sycm()

        if res is False:
            return

        # 清空 备用
        self.log_arr.clear()
        self.email_msg = ''

        res = self.login_sycm(task_name='【商品数据来源】')

        if res is False:
            return

        # 创建数据存储的文件夹
        res = self.create_storage_data_folder()

        if res is False:
            return

        # 下载数据
        res = self.commodity_flow_data(automatic_date=False)

        if res is False:
            return

        # 写入数据库
        # res = self.insert_data_in_db(task_name='【商品数据来源】')
        #
        # if res is False:
        #     return
        #
        # self.email_msg += '任务执行完毕：执行详细过程请查看log日志\n'
        # self.email_msg += '******************************\n'
        #
        # self.send_emails(theme='商品数据来源')
        #
        # # 写入日志
        # self.log_(self.log_arr, task_name='【商品数据来源】')

        pass

    def run(self):
        # self.sycm_commodity_everyday_data()
        # self.sycm_shop_flow_source()
        self.commodity_data_source()
        pass


if __name__ == '__main__':
    test = labipaiRPA()
    test.run()


