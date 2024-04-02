"""
    人群的数据采集
"""
import random
import json
import re
import pandas as pd
import calendar
import base_action
from datetime import datetime, timedelta


class crowd:
    def __init__(self):
        self.base_action_instance = base_action.base_action()
        self.inst_ = self.base_action_instance
        self.start_month = ''
        # crowd_type
        self.crowd_type = {
            'townmold': '小镇中老年',
            'srwhcol': '资深中产',
            'newwhcol': '新锐白领',
            'else': '其他',
            'exqmom': '精致妈妈',
            'urbsrz': '都市银发',
            'townyth': '小镇青年',
            'urbbucol': '都市蓝领',
            'genz': 'GenZ'
        }
        pass

    def get_config(self):

        mark = False

        res = self.inst_.get_configs('sycmCrowd')

        if res:
            # print(self.inst_.config_obj)
            mark = True
        else:
            print('# 获取配置信息失败!')

        return mark

    # 创建存储数据的文件
    def create_folder(self):

        res = self.inst_.create_folder(
            "D:", self.inst_.config_obj["excel_storage_path"])

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

        res = self.inst_.visit_sycm(task_name="[人群]")

        if res is False:
            print('# 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.inst_.sycm_login(task_name='[人群]')
        if res is False:
            print('# 登录失败，请检查！')
            return False

        return True

    # 访问人群top10, 拿到Json 数据
    def get_crowd_data(self):

        mark = False
        # 存储每一个 crowd type 的数据
        obj_arr = []

        res = self.visit_sycm()

        if res is False:
            print('# crowd: 生意参谋访问失败~ ')
            return mark

        """
            先修改 url 再进行访问
            只拿上一月份的数据
            获取月份最后一天
        """
        # 获取当前日期
        current_date = datetime.now()
        # print(current_date)
        year = current_date.year
        month = current_date.month

        # 获取上个月的年份和月份
        if current_date.month == 1:  # 如果当前月份是一月份，则上个月的年份减1，月份变为12
            last_year = year - 1
            last_month = 12
        else:  # 否则上个月份的年份不变，月份减1
            last_year = year
            last_month = month - 1

        # 获取当前月份的最后一天
        last_day = calendar.monthrange(last_year, last_month)[1]

        if last_month < 10:
            month_str = f'0{str(last_month)}'
        else:
            month_str = str(last_month)

        if last_day < 10:
            last_day_str = f'0{str(last_day)}'
        else:
            last_day_str = str(last_day)

        year_str = str(last_year)
        
        if False:
            year_str = '2024'
            month_str = '01'
            last_day_str = '31'

        url = self.inst_.config_obj['second_level_url']
        re_str = r"dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})"
        modified_url = re.sub(
            re_str, f"dateRange={year_str}-{month_str}-01%7C{year_str}-{month_str}-{last_day_str}", url)

        # print(modified_url)

        # 转换模式
        self.inst_.page.change_mode('s')

        self.inst_.page.get(modified_url)

        if self.inst_.page.raw_data:

            data_str = self.inst_.page.raw_data
            data = json.loads(data_str)

            if data['code'] == 0 and data['message'] == '操作成功':

                for i in range(0, len(data['data'])):
                    
                    obj = {'shop_id': self.inst_.config_obj['shop_id'],
                           'shop_name': self.inst_.config_obj['shop_name'],
                           'year_month': f'{year_str}-{month_str}',
                           'crowd_type': self.crowd_type[data['data'][i]['crowdId']['value']],
                    }
                    
                    try:
                        obj['visitors'] = data['data'][i]['uv']['value']
                    except KeyError as e:
                        pass
                    
                    try:
                        obj['add_to_cart_count'] = data['data'][i]['cartByrCnt']['value']
                    except KeyError as e:
                        pass
                    
                    try:
                        obj['paid_buyers'] = data['data'][i]['payByrCnt']['value']
                    except KeyError as e:
                        pass
                    
                    try:
                        obj['paid_amount'] = data['data'][i]['payAmt']['value']
                    except KeyError as e:
                        pass
                    
                    obj_arr.append(obj)

                print(f'# 数据总预览: {obj_arr}')

                # 将拿到的数据写入到本地存储
                self.inst_.pandas_insert_data(obj_arr,
                                              f'{self.inst_.source_path}/[生意参谋平台][人群]&&'
                                              f'{year_str}-{month_str}.xlsx')

                # 写入DB
                res = self.inst_.engine_insert_data(task_name='[人群]')

                if res is False:
                    print(f'# 数据写入失败!')
                else:
                    mark = True

        return mark

    def run(self):
        self.get_crowd_data()

    def test(self):
        self.inst_.page.get('https://www.baidu.com')
        pass


if __name__ == '__main__':
    test = crowd()
    test.run()
