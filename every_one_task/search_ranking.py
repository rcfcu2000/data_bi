"""
    关于 市场模块 → 搜索排行的 数据采集任务
"""
import random
import re
import pandas as pd
from datetime import datetime, timedelta
from .base_action import base_action


class search_ranking:
    def __init__(self, config):
        self.base_action_instance = base_action()
        self.inst_ = self.base_action_instance
        self.config = config
        self.start_date = ''
        self.end_date = ''
        pass

    # 读取配置文件
    def get_config(self):
        res = self.inst_.get_configs('sycmSearchRanking', config_name=self.config)
        self.start_date = res.get('start_date')
        self.end_date = res.get('end_date')
        return res

    # 创建存储数据的文件
    def create_folder(self):

        res = self.inst_.create_folder("D:", self.inst_.config_obj["excel_storage_path"])

        # print(self.inst_.failure_path)

        return res

    # 访问生意参谋
    def visit_sycm(self):

        res = self.get_config()

        if res is None:
            print('# error: 读取配置文件出错，请检查。')
            return False

        res = self.create_folder()

        if res is False:
            print('# error: 创建存储文件出错，请检查。')
            return False

        res = self.inst_.visit_sycm(task_name="【搜索排行】", config=self.config)

        if res is False:
            print('# 访问生意参谋失败，请检查。')
            return False

        # 登录
        res = self.inst_.sycm_login(task_name='【搜索排行】')
        if res is False:
            print('# 登录失败，请检查！')
            return False

        return True

    # 访问市场模块的搜索排行
    def visit_search_ranking(self):

        res = self.visit_sycm()
        date_ = ''

        if not res:
            return

        url = self.inst_.config_obj['second_level_url']
        modified_url = ''
        """
           retry: 重试
           interval：间隔
           timeout: 超时时间
           可以不给， 
           默认模式：常规模式，会等待页面加载完毕，超时自动重试或停止，默认使用此模式
        """

        unit_count = self.inst_.compute_count(self.start_date, self.end_date)

        if self.inst_.config_obj['automatic_date'] == '自动计算前一天':

            re_str = r"dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})"
            date = self.inst_.get_before_day_datetime()
            modified_url = re.sub(re_str, f"dateRange={date}%7C{date}", url)
            unit_count = 1
        else:
            date_ = self.start_date
            re_str = r"dateRange=(\d{4}-\d{2}-\d{2})%7C(\d{4}-\d{2}-\d{2})"
            modified_url = re.sub(re_str, f"dateRange={date_}%7C{date_}", url)
            pass

        for i in range(0, unit_count):

            while True:
                
                self.inst_.page.get(modified_url)
                self.inst_.page.wait.load_start()
                self.inst_.page.wait.doc_loaded()
                # 获取页面崩溃不出数据的元素
                ele = self.inst_.page('xpath: //span[text()="亲，人山人海，生意参谋压力山大，请您稍后再试吧！"]')
                if ele:
                    self.inst_.page.refresh()
                else:
                    break

            self.inst_.page.wait.ele_loaded('.ant-select-selection-selected-value')

            # 拿到每页条数做判断
            every_page_count = self.inst_.page.eles('.ant-select-selection-selected-value')
            print(f'# 当前页, 每页{every_page_count[1].text}条数据!')

            if every_page_count[1] != '100':
                # 点击 每页[100]
                ele = self.inst_.page.eles('xpath: //div[@class="ant-select-selection__rendered"]')
                # print(ele[0])
                if ele:
                    ele[1].click()
                else:
                    print('# error: 元素【每页条数选项框】没找到。')
                    return False

                # 选择 100
                # //li[text()="100"]
                ele = self.inst_.page('xpath: //li[text()="100"]')

                if ele:
                    ele.click()
                else:
                    print('# error: 元素【每页 100 条数据】没找到。')
                    return False

            # 开始拿数据
            res = self.get_list_data()

            if not res:
                print('# error: 取数据失败')
                return False

            # 计算下一个日期
            if unit_count > 1:
                date_format = "%Y-%m-%d"
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
                # 随机等待
                # num = random.randint(5, 10)
                # self.inst_.page.wait(num)

        return True

    # 拿列表数据
    def get_list_data(self):

        mark = False

        data_list = []

        self.inst_.page.wait.load_start()
        self.inst_.page.wait.doc_loaded()
        self.inst_.page.wait.ele_loaded('.ant-table-tbody')
        self.inst_.page.wait.ele_displayed('.ant-table-tbody')
        self.inst_.page.wait.ele_loaded('.oui-date-picker-current-date')
        self.inst_.page.wait.ele_displayed('.oui-date-picker-current-date')
        # .ant-pagination oui-pagination
        self.inst_.page.wait.ele_loaded('.ant-pagination oui-pagination')
        self.inst_.page.wait.ele_displayed('.ant-pagination oui-pagination')
        self.inst_.page.wait(3)

        # 拿日期时间
        ele = self.inst_.page('.oui-date-picker-current-date')
        if not ele:
            print('# error: 日期时间元素未找到！')
            return False

        date_text = ele.raw_text.split(' ')[1]
        print(f"# 日期时间：{date_text}")

        # 拿类目数据
        category = '居家布艺'

        # 词类型
        word_type = '搜索词'

        # 关键词
        # 先拿到 tbody
        t_body = self.inst_.page('.ant-table-tbody').s_ele()

        if not t_body:
            print('# tbody 未找到，请检查！')
            return False

        # 拿到 TR
        tr_arr = t_body.children('tag:tr')

        if not tr_arr:
            print('# tr 未找到，请检查！')
            return False

        # print(f"tr 已找到: {tr_arr}")
        # print(len(tr_arr))

        # 拿到页数
        page_count_ul = self.inst_.page('.ant-pagination oui-pagination')
        if not page_count_ul:
            print('# 页数获取失败，请检查！')
            return False

        page_count_li = page_count_ul.child(-2)
        next_page = page_count_ul.child(-1)

        if page_count_li:
            page_count_li_text = page_count_li.text
            page_count = int(page_count_li_text)
            print(f"# 页数为：{page_count_li_text}")
        else:
            print('# 页码获取失败，请检查！')
            return False

        for i in range(0, page_count):

            if i > 0:
                self.inst_.page.wait(2)
                pass

            for ele_item in tr_arr:
                obj = {}

                tds = ele_item.children('tag:td')
                # 搜索词
                keywords = tds[0].child('tag:div').child('tag:span').text
                # 排名
                rank = tds[1].child('tag:div').child('tag:span').child('tag:span').text
                # 搜索人气
                visiter_count = tds[2].child('tag:div').child('tag:span').child('tag:span').text
                # 点击人气
                click_count = tds[3].child('tag:div').child('tag:span').child('tag:span').text
                # 点击率
                click_rate = tds[4].child('tag:div').child('tag:span').child('tag:span').text
                # 支付转化率
                conversion_rate = tds[5].child('tag:div').child('tag:span').child('tag:span').text

                # print(f'关键词：{keywords}， 排名：{rank}， 搜索人气：{visiter_count}， '
                #       f'点击人气：{click_count}， 点击率：{click_rate}， 支付转化率：{conversion_rate}')

                # 日期时间
                obj['statistic_date'] = date_text
                # 类目1
                obj['category_lv1'] = category
                # 类目2
                obj['category_lv2'] = ''
                # 类目3
                obj['category_lv3'] = ''
                # 词类型
                obj['k_type'] = word_type
                # 关键词
                obj['keyword'] = keywords
                # 排名
                obj['k_rank'] = rank
                # 搜索人气
                obj['visitor_count'] = visiter_count
                # 点击人气
                obj['click_count'] = click_count
                # 点击率
                obj['click_rate'] = click_rate
                # 支付转化率
                obj['conversion_rate'] = conversion_rate

                data_list.append(obj)

            # 点击翻页
            next_page.click()

        print(f"# data: {data_list}")
        print(f"# 数据量: {len(data_list)}")
        res = self.inst_.pandas_insert_data(data_list, f'{self.inst_.source_path}/[生意参谋平台][搜索排行]&&{date_text}.xlsx')
        
        if res['mark']:
            print(f"[人群top10], {res['msg']}")
            mark = True

        return mark

    # 写入数据库
    def insert_data(self):
        self.inst_.engine_insert_data(task_name='[搜索排行]')

    def run(self):

        res = self.get_config()

        if res is None:
            print('# error: 读取配置文件出错，请检查。')
            return False

        res = self.create_folder()

        if res is False:
            print('# error: 创建存储文件出错，请检查。')
            return False
        
        res = self.visit_search_ranking()
        if not res:
            print('# 爬取数据失败！')
            return
        
        print('[搜索排行] 数据爬取成功！')

        self.insert_data()

        pass


if __name__ == '__main__':

    search_ranking = search_ranking()

    search_ranking.run()
