"""
    万相台 - 关键词报表
"""
import random
import json
import os
import re
import shutil
import zipfile
import glob
import pandas as pd
import calendar
from .base_action import base_action
from io import BytesIO
from datetime import datetime, timedelta
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions


class wanxiangtable_audience_everyday:

    def __init__(self, config) -> None:
        self.base = base_action()
        self.port = self.base.get_port()
        self.page = None
        self.task_name = "[万相台][人群]"
        self.get_config_bool = self.base.get_configs('wanxiang_audience_day', config_name=config)
        self.create_folder_bool = self.base.create_folder(
            "D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        self.down_load_date = ''
        # 数据库表名
        self.table_name = 'wanxiang_audience'
        self.add_col = {
            # 'shop_id': self.base.config_obj['shop_id'],
            # 'shop_name': self.base.config_obj['shop_name'],
            # 'src_type': '手淘搜索'
        }
        self.csrfId = ''
        self.check_url = self.base.config_obj['check_url']

    # 判断数据是否是以 b 开头
    def is_bytes_string(self, data):
        # 判断数据是否以字节字符串的形式表示
        return isinstance(data, bytes)

    def write_log(self):

        self.base.log_(self.base.log_arr)

    def send_email(self):

        self.base.send_emails()

    def visit_alimama(self):
        
        try:
        
            port = self.base.config_obj['port']    
        
            co = self.base.set_ChromiumOptions()

            co.set_address(f'127.0.0.1:{port}')

            page = WebPage(chromium_options=co)
        
            # res = self.base.whether_the_url_exists_in_the_browser(page=page, url_str='one.alimama.com')
            res = page.get_tab(url=self.check_url)
        
            if res is not None:
                # 已访问
                self.page = res
            else:
                pageTab = page.new_tab(self.base.config_obj['url'])
                self.page = pageTab

            return True
        
        except Exception as e:
            
            print(f'{self.base.config_obj["shop_name"]}: <error> [阿里妈妈][人群] 访问失败！')
            return False
        

    def login_alimama(self):

        if 'login' in self.page.url:
            
            iframe = self.page.get_frame(1)
            # 需要登录
            iframe("#fm-login-id").input(self.base.config_obj["user_name"])
            iframe(
                "#fm-login-password").input(self.base.config_obj["pass_word"])
            # 这里可以做一个判断，用于新老登录界面的异常捕获
            res = iframe(".fm-button fm-submit password-login").click()
            # print(f'res{res}')
            self.page.wait(5)
            
            # 检查是否需要发送验证码
            iframe_1 = self.page.get_frame(1)
            iframe_2 = iframe_1.get_frame(1)
            code_ = iframe_2('#J_GetCode')
            if code_:
                code_.click()
                # 开始等待用户输入验证码
                input('请在页面上输入验证码以后，输入随意字符继续任务：')

    # 构建post 访问数据地址
    def post_create_download_task(self):

        try:
            
            self.page.change_mode('s')

            # 获取 csrfId
            url = 'https://one.alimama.com/member/checkAccess.json?bizCode=universalBP'

            # 设置 JSON payload
            data_ = {
                "bizCode": "universalBP"
            }

            # 构建下载任务的请求
            down_load_data = {
                "lite2": False,
                "excelName": "人群报表_20240417",
                "queryFieldIn": [
                    "adPv", "click", "charge", "ctr", "ecpc", "alipayInshopAmt",
                    "alipayInshopNum", "cvr", "cartInshopNum", "itemColInshopNum",
                    "shopColDirNum", "colNum", "itemColInshopCost", "wwNum", "ecpm",
                    "prepayInshopAmt", "prepayInshopNum", "prepayDirAmt", "prepayDirNum",
                    "prepayIndirAmt", "prepayIndirNum", "gmvInshopNum", "gmvInshopAmt",
                    "alipayDirAmt", "alipayIndirAmt", "alipayDirNum", "alipayIndirNum",
                    "roi", "alipayInshopCost", "alipayInshopUv", "alipayInshopNumAvg",
                    "alipayInshopAmtAvg", "cartDirNum", "cartIndirNum", "cartRate",
                    "shopColInshopCost", "colCartNum", "colCartCost", "itemColCart",
                    "itemColCartCost", "itemColInshopRate", "cartCost", "itemColDirNum",
                    "itemColIndirNum", "couponShopNum", "shoppingNum", "shoppingAmt",
                    "inshopPv", "inshopUv", "inshopPotentialUv", "inshopPotentialUvRate",
                    "inshopPvRate", "deepInshopPv", "avgAccessPageNum", "rhRate",
                    "rhNum", "hySgUv", "hyPayAmt", "hyPayNum", "newAlipayInshopUv",
                    "newAlipayInshopUvRate"
                ],
                "pageSize": 100,
                "offset": 0,
                "havingList": [],
                "endTime": "2024-04-10",
                "unifyType": "zhai",
                "effectEqual": 15,
                "startTime": "2024-04-10",
                "splitType": "day",
                "filterAppendSubwayChannel": True,
                "filterNullCrowdSubwayTag": True,
                "vsType": "week",
                "vsTime": "2024-04-16",
                "searchValue": "",
                "searchKey": "strategyTargetTitleLike",
                "queryDomains": ["crowd", "promotion", "date", "campaign", "adgroup", "date"],
                "fieldType": "all",
                "rptType": "crowd",
                "parentAdcName": "report_frame_crowd",
                "byPage": False,
                "fromRealTime": False,
                "source": "async_dowdload",
                "csrfId": "a9c1095bded56535a3212a187acc72a3_1_1_1",
                "bizCode": "universalBP"
            }

            self.page.post(url=url, show_errmsg=True)

            data = json.loads(self.page.raw_data)

            csrfId = data['data']['accessInfo']['csrfId']

            self.csrfId = csrfId

            down_load_data['csrfId'] = csrfId

            # 获取时间 修改参数
            date_data = self.computed_date_time()

            if date_data['mark'] is False:
                print(f'{self.base.config_obj["shop_name"]}: <error> {date_data["msg"]}: {date_data["errmsg"]}')
                return False

            date_format = '%Y-%m-%d'

            date_arr = date_data['data']

            date_range = pd.date_range(date_arr[0], date_arr[1])
            
            task = []

            for date_item in date_range:

                date_item = date_item.strftime(date_format)
                down_load_data['excelName'] = f'人群报表_{date_item}'
                down_load_data['endTime'] = date_item
                down_load_data['startTime'] = date_item
                down_load_data['vsTime'] = self.base.get_before_day_datetime()

                url = 'https://one.alimama.com/report/createDownLoadTask.json'
                self.page.post(url=url, data=down_load_data, show_errmsg=True)

                print(f'{self.base.config_obj["shop_name"]}: 万相台 - 人群报表， 推送 {date_item} 的下载任务！')
                data = json.loads(self.page.raw_data)
                
                task_ = [data['data']['taskId'], date_item]
                task_tup = tuple(task_)
                task.append(task_tup)
                
                print(f'{self.base.config_obj["shop_name"]}: 万相台 - 人群报表， {date_item} 任务推送成功！')
                self.page.wait(random.randint(1, 3))

            with open('./config/download_wanxiang_audience_task_id.txt', 'w') as f:
                for task_id, date in task:
                    f.write(f"{task_id}----{csrfId}----{date}\n")
                    
                # print(f"one.alimama.com: created task list --- end")
                # res = self.base.pandas_insert_data(
                #     data_arr, f"{self.base.source_path}/[万相台][关键词报表]&&{self.down_load_date}&&{self.down_load_date}.xlsx")
           
            return True
        
        except Exception as e:
            
            print(f'{self.base.config_obj["shop_name"]}: 万相台 - 人群报表， <error> 创建下载任务失败 {str(e)}')
            return False
        
    def download_excel_file_RPA(self):
        
        data = ''
        with open('./config/download_wanxiang_audience_task_id.txt', 'r') as f:
            data = f.read().split('\n')
        
        self.page.change_mode('d')
        
        self.page.get(r"https://one.alimama.com/index.html#!/report/download-list")
        
        self.page.wait(5)
        
        # 将每页变成 60
        # self.page.wait.ele_loaded('@mxv=sizeStrs')
        
        # pageSize = self.page('@mxv=sizeStrs')
        
        # pageSize.click()
        
        # self.page.wait.ele_loaded('@title=60条/页')
        
        # self.page('@title=60条/页').click()
        
        page_num = 1
        
        while True:
        
            self.page.wait.ele_loaded('tag: tbody')
            # 找到line 元素 设置属性
            eles = self.page.eles('@mx-stickytable-operation=line')
            
            # print(eles)
            
            for i in range(0, len(eles)):
                eles[i].set.attr('mx-stickytable-operation', 'line-open')
            
            try:
                eles[0].set.attr('mx-stickytable-operation', 'line-open')
            except Exception as e:
                pass
            
            eles_ = self.page.eles('@mx-stickytable-operation=line-open')
            
            self.page.set.download_path(self.base.source_path)
            
            count = 0
            
            for ele_ in eles_:
                ele_.set.attr('mx-stickytable-operation', 'line-open')
                # self.page.set.download_file_name(f'test{count+1}')
                file_name = ele_.prev().child(3).text
                btn = ele_.child(1).child(1).child(1)
                btn.click()
                self.page.wait.download_begin()
                self.page.wait.downloads_done()
                # print(f'{task}----{file_name}')
                count += 1
                # 改文件名字
                files = os.listdir(self.base.source_path)
                paths = [os.path.join(self.base.source_path, basename) for basename in files]
                paths.sort(key=os.path.getmtime)
                new_name = f'{file_name}.zip'
                new_file_path = os.path.join(self.base.source_path, new_name)
                os.rename(paths[-1], new_file_path)
            
            # 翻页
            try:
                page_num += 1
                self.page(f"xpath: //a[text()='{page_num}']").click()     
                print(f'{self.base.config_obj["shop_name"]}: <info> 翻页：第{page_num}页！')
            except Exception as e:
                print(f'{self.base.config_obj["shop_name"]}: <info> 可能已经是最后一页了！')
                print(f'{self.base.config_obj["shop_name"]}: <info> 开始删除列表中的任务！')
                # '//input[@type="checkbox" and contains(@mx-change, "onebpaZ()")]'
                while True:
                    try:
                        self.page('xpath: //input[@type="checkbox" and contains(@mx-change, "onebpaZ()")]').click()
                        self.page('text=批量删除').parent().click()
                        self.page('text=确定').parent().click()
                        page_num -= 2
                        if page_num <= 0:
                            print(f'{self.base.config_obj["shop_name"]}: <info> 列表任务已删除完毕！')
                            break
                        self.page(f"xpath: //a[text()='{page_num}']").click()
                        print(f'{self.base.config_obj["shop_name"]}: <info> 回翻页删除任务：第{page_num}页！')
                           
                    except Exception as e:
                        # input("删除任务列表时，出错了！请手动删除列表中的任务，并按回车继续：")
                        print(f'{self.base.config_obj["shop_name"]}: <error> 删除任务列表时，出错了！')
                break
        
        pass
    
    # 批量解压压缩文件
    def unzip_files(self):
        file_list = [
            file
            for file in os.listdir(self.base.source_path)
            if os.path.isfile(os.path.join(self.base.source_path, file))
            and "人群报表_" in file
            and file.endswith(".zip")
        ]
        for file in file_list:
            self.unzip_file(f"{self.base.source_path}/{file}", self.base.source_path)
            os.remove(f"{self.base.source_path}/{file}")
        
    # 解析压缩文件
    def unzip_file(self, zip_path, extract_to):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    
    def get_excel_data_insert_db(self):

        excel_url = self.base.source_path
        file_list = []

        file_list = [
            file
            for file in os.listdir(excel_url)
            if os.path.isfile(os.path.join(excel_url, file))
            and "[关键词报表]" in file
            and file.endswith("xlsx")
        ]

        clean_df = None

        for file in file_list:
            clean_df = pd.read_excel(f"{self.base.source_path}/{file}")
            res = self.clean_and_transform_wanxiang_keywords_data(clean_df)
            if res['mark'] is False:
                print(f'{res["data"]} 数据解析失败！: {res["msg"]}')
                return {
                    'mark': False,
                    'data': '',
                    'msg': '数据写入终止！，原因是因为数据解析失败。'
                }

            self.base.insert_data(df_cleaned=res['data'], table_name=self.table_name, key=[
                                  'product_id', 'datetimekey', 'plan_id', 'keyword_type', 'keyword_name'])

    # 关键词报表
    def clean_and_transform_wanxiang_keywords_data(self, df):
        global cn
        try:
            # 字符串 转 整数， 去除 逗号, 去除 \n 字符
            columns_to_convert = [
                'pre_sell_count',
                'dir_pre_sell_count',
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
                'pre_sell_amount',
                'dir_pre_sell_amount',
                'dir_sell_amount',
                'idr_sell_amount',
                'gmv',
                'take_order_amount',
                'coupon_count',
                'recharge_amount',
                'wangwang_count',
                'guided_visits',
                'enrollment_count',
                'deep_visits',
                'members_gmv',
            ]

            for cn in columns_to_convert:
                df[cn] = (
                    df[cn]
                    .replace({",": "", "n": 0, "N": 0}, regex=True)
                    .astype("float64", errors="ignore")
                )

            return {
                'mark': True,
                'data': df,
                'msg': '数据清洗成功！'
            }
        except Exception as e:
            print("# 数据清洗失败:", cn, df[cn], e)
            # self.log_([f"error/shs/【{self.get_date_time()}】: 关键词报表 清洗失败", f'{str(e)}'])
            return {
                'mark': False,
                'data': df[cn],
                'msg': f'数据清洗失败！: {str(e)}'
            }

    def computed_date_time(self):

        try:
            if self.base.config_obj['automatic_date'] == '自动计算前一天':
                date_1 = self.base.get_before_day_datetime()
                date_2 = date_1
            else:
                date_1 = self.base.config_obj['start_date']
                date_2 = self.base.config_obj['end_date']

                return {
                    'mark': True,
                    'data': [date_1, date_2],
                    'msg': '成功计算时间！'
                }
        except Exception as e:

            return {
                'mark': False,
                'data': [],
                'msg': '计算时间失败！',
                'errmsg': str(e)
            }

    def run(self):
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 阿里妈妈 人群报表！')

        res = self.visit_alimama()
        
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 阿里妈妈，访问成功！')

        self.page.wait(10)

        self.login_alimama()
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 阿里妈妈，登录成功！')

        self.page.wait(5)

        res = self.post_create_download_task()
        
        if res is False:
            return
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 阿里妈妈，推送任务列表成功！')
        
        self.download_excel_file_RPA()
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 阿里妈妈，下载数据文件成功！')
        
        self.unzip_files()
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 阿里妈妈，解压zip文件成功！')
        
        self.page.wait(3)

        self.base.wanxiang_table(table_name="wanxiang_audience")
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 人群报表写入成功！')

    def test(self):

        self.down_load_excel()


if __name__ == "__main__":
    test = wanxiangtable_audience_everyday()
    test.run()
