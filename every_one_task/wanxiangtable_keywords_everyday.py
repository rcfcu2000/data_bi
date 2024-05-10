"""
    万相台 - 关键词报表
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
from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions


class wanxiangtable_keywords_everyday:

    def __init__(self, config) -> None:
        self.base = base_action()
        self.port = self.base.get_port()
        self.page = None
        self.task_name = "[万相台][关键词]"
        self.get_config_bool = self.base.get_configs('wanxiang_keywords_day', config_name=config)
        self.create_folder_bool = self.base.create_folder(
            "D:", self.base.config_obj['excel_storage_path'])
        self.data = ''
        self.data_bool = False
        self.down_load_date = ''
        # 数据库表名
        self.table_name = 'wanxiang_keywords'
        self.add_col = {
            # 'shop_id': self.base.config_obj['shop_id'],
            # 'shop_name': self.base.config_obj['shop_name'],
            # 'src_type': '手淘搜索'
        }

    # 判断数据是否是以 b 开头
    def is_bytes_string(self, data):
        # 判断数据是否以字节字符串的形式表示
        return isinstance(data, bytes)

    def write_log(self):

        self.base.log_(self.base.log_arr)

    def send_email(self):

        self.base.send_emails()

    def visit_alimama(self):

        port = self.base.config_obj['port']   
        # co = ChromiumOptions()
        
        co = self.base.set_ChromiumOptions()

        co.set_address(f'127.0.0.1:{port}')

        page = WebPage(chromium_options=co)
        
        res = self.base.whether_the_url_exists_in_the_browser(page=page, url_str='sycm.taobao.com')
        
        if res['mark']:
            # 已访问
            pageTab = page.new_tab(self.base.config_obj['url'])
            self.page = pageTab
        else:
            page.get(self.base.config_obj['url'])
            self.page = page
        
        
        # print(f'2. browser_id: {page._browser_id} && tab_id: {page.tab_id} && browser_url: {page._browser_url}')

        # page.get(self.base.config_obj['url'])

        # page.set.window.max()

        # self.page = page

        # print(self.page.url)

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
            
            checkCode_Iframe = self.page.get_frame(1).get_frame(1)
            # 检查是否需要发送验证码
            code_ = checkCode_Iframe('#J_GetCode')
            if code_:
                code_.click()
                # 开始等待用户输入验证码
                input('请在页面上输入验证码以后，输入随意字符继续任务：')

    # 构建post 访问数据地址
    def post_(self):

        self.page.change_mode('s')

        cookie = self.page.cookies()
        # cookie =json.loads(cookie)
        # print(f"cookie: {cookie}")

        # 设置目标 URL
        url = 'https://one.alimama.com/member/checkAccess.json?bizCode=universalBP'

        # 设置请求头部 (暂时不用)
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'BX-V': '2.5.11',
            'Content-Type': 'application/json',
            'Cookie': 'cna=wSyhHsUmFHMCAX1GssD25aE8; xlly_s=1; lid=%E8%9C%A1%E7%AC%94%E6%B4%BE%E5%AE%B6%E5%B1%85%E6%97%97%E8%88%B0%E5%BA%97%3A%E5%8D%8E; __wpkreporterwid_=f5741c73-567f-47a6-b795-d3c63e4d6cca; _tb_token_=c1d358f7-1f2b-4d1d-bdeb-40d5827f4a3a; t=b8d6fef0389774aeb057e3421c2295da; _tb_token_=37be657735abe; cookie2=1b718b8c494f125acf8b84c9e272318c; sgcookie=E100DMVMwYXFVptf9ylqmwihF9ojjzJ4ODYKuhWT3AzaEYpkZUuP2f2sJ6PDLIXp9I%2Bj75wiYo5FbqF%2Fl5aKWNyOpAEnt27orbwcZ1Ki5VUTJpqPaGfPy1zd0CBI2dV4ckea; uc1=cookie14=UoYfoxTHOWZWuA%3D%3D&cookie21=UtASsssmfufd; cancelledSubSites=empty; csg=18701db5; unb=2217140767009; sn=%E8%9C%A1%E7%AC%94%E6%B4%BE%E5%AE%B6%E5%B1%85%E6%97%97%E8%88%B0%E5%BA%97%3A%E5%8D%8E; tfstk=f--rhc2Sp0nr1W8hgssF7PKw_JjR9Gh_qH1CKpvhF_flVpZeuBODFagJFM82tIQ5ZU9CLkJRGBZBFB1V0svBP6s5R9A2TQ-QNg_CL8K2_kZSw4p3YMsn1fisfLp5vMcsqVksh_Bpn6XhhQrveMInG-Zn5cJJ_aGoQ2Sn3iXCByj3q9DVo96Nxub3qrVcM9jhtBfliSXFE6qur92mipOHwo5VrbGj_nXiQObPsKuTxkJzfa5MEsrnsnWr61vlgkqhw6CT1KJSalpf2hRNCQi04CJ9DIWDsDlVvILHt9RmXWsJ5QKOuK3gmKsPMNYhuYmk3g5P5n74pzsy-QKFyUyKBKjkMFCOrqhA33t181Q4ibvX3_bw8QGYx_TwnI76cSZ1vILHt9R0agz09tjGlHLzty7lHt5s3x4RwJ2lKtUVXyUduqBV1AHTJyQlHt5s3xzLJZ-O31MtB',
            'Origin': 'https://one.alimama.com',
            'Referer': 'https://one.alimama.com/index.html',
            'Sec-CH-UA': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': "Windows",
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        # 设置 JSON payload
        data_ = {
            "lite2": False,
            "bizCode": "universalBP",
            "fromRealTime": False,
            "source": "baseReport",
            "byPage": True,
            "totalTag": True,
            "rptType": "bidword",
            "pageSize": 100,
            "offset": 0,
            "havingList": [],
            "endTime": "2024-04-14",
            "unifyType": "zhai",
            "effectEqual": 15,
            "startTime": "2024-04-14",
            "splitType": "day",
            "bizCodeIn": [
                "onebpSearch"
            ],
            "productIdNotIn": [
                "101016001"
            ],
            "queryFieldIn": ["adPv", "click", "charge", "ctr", "ecpc", "alipayInshopAmt", "alipayInshopNum", "cvr", "cartInshopNum", "itemColInshopNum", "shopColDirNum", "colNum", "itemColInshopCost", "avgRank", "wwNum", "ecpm", "prepayInshopAmt", "prepayInshopNum", "prepayDirAmt", "prepayDirNum", "prepayIndirAmt", "prepayIndirNum", "alipayDirAmt", "alipayIndirAmt", "alipayDirNum", "alipayIndirNum", "roi", "alipayInshopCost", "cartDirNum", "cartIndirNum", "cartRate", "shopColInshopCost", "colCartNum", "colCartCost", "itemColCart", "itemColCartCost", "itemColInshopRate", "cartCost", "itemColDirNum", "gmvInshopNum", "gmvInshopAmt", "itemColIndirNum", "couponShopNum", "shoppingNum", "shoppingAmt", "inshopPv", "inshopUv", "inshopPotentialUv", "inshopPotentialUvRate", "inshopPvRate", "deepInshopPv", "avgAccessPageNum", "rhRate", "rhNum", "hySgUv", "hyPayAmt", "hyPayNum", "newAlipayInshopUv", "newAlipayInshopUvRate", "alipayInshopUv", "alipayInshopNumAvg", "alipayInshopAmtAvg"],
            "searchValue": "",
            "searchKey": "strategyBidwordNameLike",
            "queryDomains": [
                "word",
                "date",
                "campaign",
                "adgroup"
            ],
            "csrfId": "a9c1095bded56535a3212a187acc72a3_1_1_1"
        }

        self.page.post(url=url, show_errmsg=True)

        data = json.loads(self.page.raw_data)

        csrfId = data['data']['accessInfo']['csrfId']

        data_['csrfId'] = csrfId

        # 计算时间
        date_data = self.computed_date_time()

        if date_data['mark'] is False:
            print(f'{date_data["msg"]}: {date_data["errmsg"]}')
            return False

        date_format = '%Y-%m-%d'

        date_arr = date_data['data']

        date_range = pd.date_range(date_arr[0], date_arr[1])

        for date_item in date_range:
            
            data_arr = []
            date_item = date_item.strftime(date_format)
            data_['offset'] = 0
            data_['startTime'] = date_item
            data_['endTime'] = date_item
            self.down_load_date = data_['endTime']

            while True:
                url = f"https://one.alimama.com/report/query.json"
                self.page.post(url=url, data=data_, show_errmsg=True)
                print(f"{self.base.config_obj['shop_name']}: <info> 开始下载{date_item}的关键词报表, 第{int(data_['offset']/100)+1}页") 
                data = json.loads(self.page.raw_data)
                # print(data['data']['list'])
                if data['data'] is None:
                    print(f'{self.base.config_obj["shop_name"]}: <error> 发生错误， {data["info"]["errorCode"]}, {data["info"]["message"]}')
                    print(f'{self.base.config_obj["shop_name"]}: <info> 随机3秒后准备重试...')
                    self.page.wait(random.randint(1, 3))
                    continue
                    
                if len(data['data']['list']) == 0:
                    print(f"{self.base.config_obj['shop_name']}: <info> 下载{date_item}的关键词报表完毕！")
                    break

                for item in data['data']['list']:

                    obj = {
                        'datetimekey': date_item,  # 日期键，用于标记这些数据的时间点
                    }
                    # 使用.get()安全地访问每个字段，并为不存在的键指定空字符串''为默认值
                    obj['promotion_id'] = item.get('sceneId', '')  # 场景ID，唯一标识一个场景
                    obj['promotion_name'] = item.get('scene1Name', '')  # 场景名称，描述场景的名字
                    obj['promotion_type'] = item.get('scene1Name', '')  # 场景类型，通常与场景名称相同
                    obj['plan_id'] = item.get('campaignId', '')  # 计划ID，用于标识特定的广告计划
                    obj['plan_name'] = item.get('campaignName', '')  # 计划名称，描述广告计划的名称
                    obj['unit_id'] = item.get('adgroupId', '')  # 单元ID，用于标识广告单元
                    obj['unit_name'] = item.get('adgroupName', '')  # 单元名称，描述广告单元的名称
                    obj['product_id'] = item.get('blackCreativePromotionId', '')  # 宝贝ID，标识特定的宣传产品
                    obj['product_name'] = item.get('promotionName', '')  # 宝贝名称，描述宣传产品的名称
                    obj['keyword_type'] = '关键词包'  # 关键词类型，这里假设为关键词包
                    obj['keyword_id'] = item.get('bidwordPkgId', '')  # 关键词ID，词包ID，唯一标识关键词包
                    obj['keyword_name'] = item.get('bidwordPkgName', '')  # 关键词名称/词包名称，描述关键词或词包的名称
                    obj['impressions'] = item.get('adPv', '')  # 展现量，展示广告被看到的次数
                    obj['clicktraffic'] = item.get('click', '')  # 点击量，用户对广告的点击次数
                    obj['spend'] = item.get('charge', '')  # 花费，广告的总花费
                    obj['pre_sell_amount'] = item.get('prepayDirAmt', '')  # 总预售成交金额，直接预售的成交金额
                    obj['pre_sell_count'] = item.get('prepayDirNum', '')  # 总预售成交笔数，直接预售的成交次数
                    obj['dir_pre_sell_amount'] = item.get('prepayDirAmt', '')  
                    obj['dir_pre_sell_count'] = item.get('prepayDirNum', '')  
                    obj['dir_sell_amount'] = item.get('alipayDirAmt', '')  # 直接成交金额，直接通过正常购买方式的成交金额
                    obj['idr_sell_amount'] = item.get('alipayIndirAmt', '')  # 间接成交金额，间接通过正常购买方式的成交金额
                    obj['gmv'] = item.get('alipayInshopAmt', '')  # 总成交金额，所有通过店铺成交的金额
                    obj['gmv_count'] = item.get('alipayInshopNum', '')  # 总成交笔数，所有通过店铺成交的次数
                    obj['dir_sell_count'] = item.get('alipayDirNum', '')  # 直接成交笔数，直接通过店铺成交的次数
                    obj['idr_sell_count'] = item.get('alipayIndirNum', '')  # 间接成交笔数，间接通过店铺成交的次数
                    obj['shopcart_count'] = item.get('cartInshopNum', '')  # 总购物车数，所有加入购物车的次数
                    obj['dir_shopcart_count'] = item.get('cartDirNum', '')  # 直接购物车数，直接加入购物车的次数
                    obj['idr_shopcart_count'] = item.get('cartIndirNum', '')  # 间接购物车数，间接加入购物车的次数
                    obj['coll_prod_count'] = item.get('itemColCart', '')  # 收藏宝贝数，收藏特定商品的次数
                    obj['coll_shop_count'] = item.get('shopColDirNum', '')  # 收藏店铺数，收藏店铺的次数
                    obj['coll_add_count'] = item.get('colCartNum', '')  # 总收藏加购数，总的商品收藏和加购的次数
                    obj['coll_add_prod_count'] = item.get('itemColInshopNum', '')  # 宝贝收藏加购数，特定商品收藏加购的次数
                    obj['coll_count'] = item.get('colNum', '')  # 总收藏数，所有商品的收藏次数
                    obj['take_order_count'] = item.get('gmvInshopNum', '')  # 拍下订单数，通过店铺成交的订单次数
                    obj['take_order_amount'] = item.get('gmvInshopAmt', '')  # 拍下订单金额，通过店铺成交的订单总金额
                    obj['dir_coll_prod_count'] = item.get('itemColDirNum', '')  # 直接收藏宝贝数，直接收藏商品的次数
                    obj['idr_coll_prod_count'] = item.get('itemColIndirNum', '')  # 间接收藏宝贝数，间接收藏商品的次数
                    obj['coupon_count'] = item.get('couponShopNum', '')  # 优惠券领取量，领取优惠券的次数
                    obj['recharge_count'] = item.get('shoppingNum', '')  # 购物金充值笔数，充值购物金的次数
                    obj['recharge_amount'] = item.get('shoppingAmt', '')  # 购物金充值金额，充值购物金的总金额
                    obj['wangwang_count'] = item.get('wwNum', '')  # 旺旺咨询量，通过旺旺进行咨询的次数
                    obj['guided_visits'] = item.get('inshopPv', '')  # 引导访问量，引导到店铺的访问次数
                    obj['guided_visitors'] = item.get('inshopUv', '')  # 引导访问人数，引导到店铺的独立访客数
                    obj['potential_guided_visitors'] = item.get('inshopPotentialUv', '')  # 引导访问潜客数，潜在的被引导访问的人数
                    # obj['enrollment_rate'] = item.get('rhRate', '')  # 入会率，成为会员的比率
                    obj['enrollment_count'] = item.get('rhNum', '')  # 入会量，成为会员的总人数
                    obj['deep_visits'] = item.get('deepInshopPv', '')  # 深度访问量，进行深度访问的次数
                    obj['new_customers'] = item.get('newAlipayInshopUv', '')  # 成交新客数，新客户成交的人数
                    obj['first_buy_members'] = item.get('hySgUv', '')  # 会员首购人数，首次购买的会员人数
                    obj['members_gmv'] = item.get('hyPayAmt', '')  # 会员成交金额，会员的成交总金额
                    obj['members_gmv_count'] = item.get('hyPayNum', '')  # 会员成交笔数，会员成交的总次数
                    obj['buyer_count'] = item.get('alipayInshopUv', '')  # 成交人数，完成购买的总人数

                    data_arr.append(obj)

                data_['offset'] += 100
                self.page.wait(random.uniform(0.1, 0.3))
            
            if len(data_arr) > 0:    
                res = self.base.pandas_insert_data(
                    data_arr, f"{self.base.source_path}/[万相台][关键词报表]&&{self.down_load_date}&&{self.down_load_date}.xlsx")
  
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
                print(f'{self.base.config_obj["shop_name"]}: <error> {res["data"]} 数据解析失败！: {res["msg"]}')
                shutil.move(
                    f"{self.base.source_path}/" + file,
                    f"{self.base.failure_path}/" + file,
                )
                return {
                    'mark': False,
                    'data': '',
                    'msg': '数据写入终止！，原因是因为数据解析失败。'
                }
            
            mark = self.base.insert_data(df_cleaned=res['data'], table_name=self.table_name, key=['product_id', 'datetimekey', 'plan_id', 'keyword_type', 'keyword_name'])        
            if mark:
                shutil.move(
                    f"{self.base.source_path}/" + file,
                    f"{self.base.succeed_path}/" + file,
                )
            else:
                shutil.move(
                    f"{self.base.source_path}/" + file,
                    f"{self.base.failure_path}/" + file,
                )
            
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
            print(f"{self.base.config_obj['shop_name']}: <error> 数据清洗失败:", cn, df[cn], e)
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
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 开始执行 阿里妈妈 关键词报表！')

        self.visit_alimama()

        self.page.wait(10)

        self.login_alimama()

        self.page.wait(5)

        self.post_()
        
        self.get_excel_data_insert_db()
        
        print(f'{self.base.config_obj["shop_name"]}: <info> 执行完毕 阿里妈妈 关键词报表！')

    def test(self):

        self.visit_alimama()


if __name__ == "__main__":
    test = wanxiangtable_keywords_everyday()
    test.run()
