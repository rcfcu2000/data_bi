"""
    店铺客户的数据采集
"""
import random
import json
import re
import pandas as pd
import calendar
import base_action
from datetime import datetime, timedelta


class shop_customer:
    def __init__(self):
        self.base_action_instance = base_action.base_action()
        self.inst_ = self.base_action_instance
        self.start_month = ''
        pass
    
    def get_config(self):
        
        mark = False
        
        res = self.inst_.get_configs('shopCustomer')

        if res:
            # print(self.inst_.config_obj)
            mark = True
        else:
            print('# 获取配置信息失败!')
        
        return mark
    
    # 创建存储数据的文件
    def create_folder(self):

        res = self.inst_.create_folder("D:", self.inst_.config_obj["excel_storage_path"])

        return res

     # 访问生意参谋
    def visit_sycm(self):

        res = self.get_config()

        if res is False:
            print('# error: 读取配置文件出错，请检查。')
            return False

        res = self.create_folder()

        if res is False:
            print('# error: 创建存储文件出错，请检查。')
            return False

        res = self.inst_.visit_sycm(task_name="[店铺客户]")

        if res is False:
            print('# 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.inst_.sycm_login(task_name='[店铺客户]')
        if res is False:
            print('# 登录失败，请检查！')
            return False

        return True
    
    # 访问人群top10, 拿到Json 数据
    def get_shop_customer_data(self, calc_date):
        
        mark = False
        # 存储每一个 crowd type 的数据
        obj_arr = []
        
        res = self.visit_sycm()
        
        if res is False:
            print('# shop_customer: 生意参谋访问失败~ ')
            return mark
        
        """
            先修改 url 再进行访问
            只拿上一月份的数据
            获取月份最后一天
        """
        
        indexCodes = ['']
        # 转换模式
        self.inst_.page.change_mode('s')

        data_url_part1 = 'https://sycm.taobao.com/domain/oneQuery.json?domainCode=tao.shop.customer.overview&dateType=day&dateRange='

        data_url_part2 = '&bizCode=sycm_pc&showType=trend&device=0&indexCodes=statDate%2CshopCustomer%2CnewVisitorCnt%2CshopCustomerAvgGood%2CnewVisitorCntAvgGood%2ChasPurchaseCntAvgGood%2CnoPurchaseCntAvgGood%2CnewVisitorBuyCnt%2CnewVisitorInShopCnt%2CnewVisitorPayRate%2CnewVisitorPct%2CnewVisitorVipRate%2CnewVisitorFansRate%2CnewVisitorPayAmtRatio%2CnewVisitorReCall%2CnoPurchaseCnt%2CnoPurchaseBuyCnt%2CnoBuyInShopCnt%2CnoPurchasePayRate%2CnoPurchasePct%2CnoPurchaseFansRate%2CnoPurchaseVipRate%2CnoPurchasePayAmtRatio%2CnoPurchaseReCall%2ChasPurchaseCnt%2ChasPurchaseUbyCnt%2ChasBuyInShopCnt%2ChasPurchasePayRate%2ChasPurchasePayAmtRatio%2ChasPurchasePct%2ChasPurchaseFansRate%2ChasPurchaseVipRate%2ChasPurchaseReCall%2CnoPurchaseBuyCntRate%2ChasPurchaseUbyCntRate'

        data_url = data_url_part1 + calc_date + "%7C" + calc_date +  data_url_part2

        self.inst_.page.get(data_url)
        
        if self.inst_.page.raw_data:

            data_str = self.inst_.page.raw_data
            data = json.loads(data_str)

            if data['code'] == 0:
                my_data = data["data"]
                df = pd.DataFrame(my_data)
                df['statDate'] = pd.to_datetime(df['statDate'], unit='ms').dt.strftime('%Y-%m-%d')
                df['shop_name'] = self.inst_.config_obj['shop_name']
                df['shop_id'] = self.inst_.config_obj['shop_id']

            columns_to_drop = ['noPurchaseCntAvgGood', 'newVisitorCntAvgGood', 'shopCustomerAvgGood', 
                               'hasPurchaseCntAvgGood', 'noPurchaseVipRate', 'newVisitorFansRate', 
                               'noPurchaseReCall', 'noPurchaseBuyCntRate', 'noPurchaseFansRate',
                               'newVisitorReCall', 'hasPurchaseVipRate', 'hasPurchaseFansRate', 
                               'newVisitorVipRate', 'hasPurchaseReCall', 'hasPurchaseUbyCntRate']
            
            df.drop(columns=columns_to_drop, inplace=True)

            df.rename(columns={
                    "statDate": "statistic_date",
                    "shopCustomer": "total_customers",
                    "newVisitorCnt": "new_visits",
                    "newVisitorBuyCnt": "new_visit_conversions",
                    "newVisitorInShopCnt": "new_visit_non_conversions",
                    "newVisitorPayRate": "new_visit_payment_conversion_rate",
                    "newVisitorPayAmtRatio": "new_visit_payment_amount_percentage",
                    "newVisitorPct": "new_visit_average_order_value",
                    "noPurchaseCnt": "non_purchase_return_visits",
                    "noPurchaseBuyCnt": "return_visit_conversions_non_purchasers",
                    "noBuyInShopCnt": "return_visit_non_conversions_non_purchasers",
                    "noPurchasePayRate": "return_visit_payment_conversion_rate_non_purchasers",
                    "noPurchasePayAmtRatio": "return_visit_payment_amount_percentage_non_purchasers",
                    "noPurchasePct": "return_visit_average_order_value_non_purchasers",
                    "hasPurchaseCnt": "purchased_customer_return_visits",
                    "hasPurchaseUbyCnt": "repeat_purchases",
                    "hasBuyInShopCnt": "unpaid_repeat_purchases",
                    "hasPurchasePayRate": "return_visit_payment_conversion_rate_purchasers",
                    "hasPurchasePayAmtRatio": "return_visit_payment_amount_percentage_purchasers",
                    "hasPurchasePct": "return_visit_average_order_value_purchasers",
                }, inplace=True)
            
            table_name = 'biz_shop_customers'
            # engine = self.inst_.create_engine()

            transfersql = self.inst_.insert_data_sql_(df, table_name,
                                        key=['statistic_date', 'shop_id', 'shop_name'])

            if transfersql is False:
                return False
            
            return df
        else:
            return None
    
    def run(self):
        self.get_config()
        calc_date = "2024-03-31"
        self.get_shop_customer_data(calc_date)

    def test(self):
        self.inst_.page.get('https://www.baidu.com')
        pass
        
    
if __name__ == '__main__':
    test = shop_customer()
    test.run()