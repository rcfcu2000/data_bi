"""
 stone 宝藏类
"""
import os
import re
import socket
import time
# 自动管理浏览器驱动的包
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome, ChromeOptions
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service


class stone(object):
    def __init__(self):
        # 端口号数组
        self.port_list = []
        self.chrome_driver_path = '\\driver\\chromedriver-win64_123\\'
        self.driver = None
        pass

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

    # 创建开启浏览器所需的文件夹
    def __create_folder(self, tag, drive="D"):

        res = {}

        try:
            
            if not os.path.exists(f'{drive}:'):
                # 证明没有D盘
                drive = 'C'
                
            folder_path = rf"{drive}:\selenium_{tag}"
            # 使用os.makedirs()创建文件夹，如果它不存在
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            res['mark'] = True
            res['result'] = folder_path

            return res

        except Exception as e:

            res['mark'] = False
            res['result'] = str(e)

            return res

    def open_browser(self, folder_tag, browser="chrome"):

        res = {}

        try:
            # 使用不同的调试器端口
            port = self.__find_free_port()

            if port["mark"] is False:
                res['mark'] = False
                res['result'] = f"错误: {port['error']}"
                return res

            folder_path = self.__create_folder(folder_tag)

            if folder_path["mark"] is False:
                res['mark'] = False
                res['result'] = f"错误: {folder_path['error']}"
                return res

            # 启动Chrome浏览器
            cmd = rf'start {browser} --remote-debugging-port={port["result"]} --user-data-dir="{folder_path["result"]}"'
            os.system(cmd)

            res['mark'] = True
            res['result'] = {"port": port["result"], "folder": folder_path["result"]}

        except Exception as e:

            res['mark'] = False
            res['result'] = str(e)

        return res

    def open_existing_browser(self, folder_tag, port=9222, browser="chrome"):
        
        obj = {
            'mark': False,
            'port': 0,
            'browser': browser,
            'folder_path': None
        }
        
        try:
        
            folder_path = self.__create_folder(folder_tag)
                
            if folder_path['mark']:
                
                cmd = rf'start {browser} --remote-debugging-port={port} --user-data-dir="{folder_path["result"]}"'
                os.system(cmd)

            else:
                
                print('info: 创建文件夹失败。')
                print(f'info: 浏览器启动失败。')
                return obj
        
        except Exception as e:
            
            print(f'info: 浏览器启动失败\n {str(e)}')
            return obj
        
        obj['mark'] = True
        obj['port'] = port
        obj['browser'] = browser
        obj['folder_path'] = folder_path["result"] 
        
        return obj
    

    def dispose_cookie(self, cookie_file, domain):

        res = {}

        try:
            # 读取Excel文件
            cookie_arr = []
            df = pd.read_excel(cookie_file, sheet_name='Sheet1')
            df_filled = df.fillna("")

            for index, row in df_filled.iterrows():
                cookie = {}  # 创建一个新的字典用于存储每行的数据

                for column in df_filled.columns:
                    value = row[column]  # 获取每列的值

                    if column == "domain":

                        # value = domain
                        cookie[column] = value

                    elif column == "httpOnly":
                        if value == "":
                            value = False
                        else:
                            value = True

                        cookie[column] = value

                    elif column == "secure":
                        if value == "":
                            value = False
                        else:
                            value = True

                        cookie[column] = value

                    # elif column == "sameSite":
                    #     if value == "":
                    #         value = "None"
                    #     else:
                    #         pass
                    #     print(f"sameSite: {value}")
                    #     cookie[column] = value

                    # elif column == "expires/max-age":
                    #
                    #     pass
                    #
                    # elif column == "partition key":
                    #
                    #     pass
                    #
                    # elif column == "size":
                    #
                    #     pass
                    #
                    # # priority
                    # elif column == "priority":
                    #
                    #     pass

                    else:
                        cookie[column] = value # 使用列名作为字典的键

                cookie_arr.append(cookie)

                res["mark"] = True
                res["result"] = cookie_arr

            print(res["result"])
            pass
        except Exception as e:

            res["mark"] = False
            res["result"] = str(e)
            pass

        return res

    # 处理 txt 的 cookies
    def get_txt_cookies(self, txt_path: str):

        cookie = {}

        with open(txt_path, "r") as f:
            data = f.read().split(";")
            print(data)

        for item in data:
            item = re.sub(r'^\s+|\s+$', '', item)
            data_ = item.split("=")
            print(data_)
            cookie[f"{data_[0]}"] = data_[1]

        return cookie
    
    def visit_(self, folder_tag, port=9222, browser="chrome", open_browser_mode='old', address='www.baidu.com'):
        
        obj = {
            'mark': False
        }
        
        if open_browser_mode == 'old':
            
            folder_path = self.__create_folder(folder_tag)
            
            if not folder_path['mark']:
                return obj
            
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")  # 禁用控制提示
            options.add_argument(f"--remote-debugging-port={port}")  # 例如指定端口为 9222
            chrome_driver_path = ChromeDriverManager().install()
            
            # driver_path = webdriver.Chrome(options=options)
            service = Service(chrome_driver_path)  # 指定驱动程序的路径
            driver = webdriver.Chrome(service=service, options=options)
            
            driver.get(address)
            
        else:
            pass
        pass

    def visit_website(self, tag, port=None, cookie_file="",
                      address="https://www.baidu.com", domain=".baidu.com", sh=False, tzsb=False):
        res = {}
        toggle = ""
        try:
            if port is None:
                port = self.open_browser(tag)

                if port["mark"] is False:
                    res['mark'] = False
                    res['result'] = port["result"]
                    return res

            options = Options()
            options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port['result']['port']}")
            chrome_driver_path = ChromeDriverManager().install()
            service = Service(chrome_driver_path)
            # service = Service('D:/data_automation/driver/chromedriver.exe')  # 指定驱动程序的路径
            driver = webdriver.Chrome(service=service, options=options)

            if sh is False and tzsb is False:

                if cookie_file != "":
                    # 清除所有cookie
                    driver.delete_all_cookies()
                    # 处理cookies
                    cookies = self.dispose_cookie(cookie_file, domain)
                    print(f"cookies: {cookies}")
                    driver.get(address)

                    res['mark'] = True
                    res['result'] = {"driver": driver, "port": port['result']['port']}

                    # 添加 cookie
                    for item in cookies:
                        driver.add_cookie(item)

                    driver.get(address)

                    res['mark'] = True
                    res['result'] = {"driver": driver, "port": port['result']['port']}

                    pass
                else:
                    print(address)
                    driver.get(address)

                    res['mark'] = True
                    res['result'] = {"driver": driver, "port": port['result']['port']}

                driver.maximize_window()
                self.driver = driver
            
            elif sh:
                driver.get(address)

                cookie_ = [
                    {'domain': 'www.shuenhang.top', 'httpOnly': False, 'name': 'ECSCP_ID', 'path': '/',
                     'sameSite': 'Lax', 'secure': False, 'value': 'af78f11ae17043456870ed02c20393cdf8f0bdca'},
                    {'domain': 'www.shuenhang.top', 'expiry': 1693808129, 'httpOnly': False,
                     'name': 'ECSCP[admin_pass]', 'path': '/admin', 'sameSite': 'Lax', 'secure': False,
                     'value': 'd88082dd4995e7d021042ace75f223e6'},
                    {'domain': 'www.shuenhang.top', 'expiry': 1693808129, 'httpOnly': False, 'name': 'ECSCP[admin_id]',
                     'path': '/admin', 'sameSite': 'Lax', 'secure': False, 'value': '179'},
                    {'domain': 'www.shuenhang.top', 'httpOnly': False, 'name': 'income', 'path': '/admin',
                     'sameSite': 'Lax', 'secure': False, 'value': '5'}]

                for item in cookie_:

                    if "expiry" in item:
                        del item["expiry"]

                    driver.add_cookie(item)

                driver.get(address)

                res['mark'] = True
                res['result'] = {"driver": driver, "port": port['result']['port']}

                driver.maximize_window()

            else:
                driver.get(address)

                cookies = self.get_txt_cookies("D:/data_automation/tasb_cookie.txt")

                # cookies = {
                #     'Hm_lvt_e8002ef3d9e0d8274b5b74cc4a027d08': '1699858205',
                #     'Admin-Token': 'eyJhbGciOiJIUzUxMiJ9.eyJsb2dpbl91c2VyX2tleSI6IjEwMDAwMjk0MjUifQ.6GiZbk_UPPrioVgDP50aksQIS0XQYN3Kz78Wzzw8g1weVzO787S7xHvbqoygrx7_6nZ7GeAm8oFMtcra_MQVAg',
                #     'sidebarStatus': '0',
                #     'SECKEY_ABVK': 'cm4nXDL9rtt8mRZS/l+FtnCFwhYw0MPUaPtWq8RSn1U%3D',
                #     'Hm_lpvt_e8002ef3d9e0d8274b5b74cc4a027d08': '1699865312',
                #     'JSESSIONID': 'A4AA5F22046C9158FDCCDC8797B2E27E',
                # }

                for name, value in cookies.items():
                    driver.add_cookie({'name': name, 'value': value})

                driver.get(address)

                res['mark'] = True
                res['result'] = {"driver": driver, "port": port['result']['port']}

                driver.maximize_window()

        except Exception as e:

            # 判断是否登录成功
            if res['result']['driver'].current_url == address:
                toggle = "finally"

            else:

                res['mark'] = False
                res['result'] = str(e)

        finally:
            if toggle == "finally":
                res['mark'] = True
                res['result'] = {"driver": driver, "port": port['result']['port']}

        return res

    # 封装等待元素加载
    def wait_element(self, driver, str_, remark, waite_time=30, interval_second=0.5, plural=True):
        res = {}
        try:
            if not plural:
                element = WebDriverWait(driver, waite_time, interval_second) \
                    .until(EC.presence_of_element_located((By.XPATH, str_)))

                res['mark'] = True
                res['result'] = {"element": element,
                                 "html": element.get_attribute("outerHTML"),
                                 "text": element.text}

            else:
                element = WebDriverWait(driver, waite_time, interval_second) \
                    .until(EC.presence_of_all_elements_located((By.XPATH, str_)))

                res['mark'] = True
                res['result'] = element

        except TimeoutException as e:

            res['mark'] = False
            res['result'] = f"未找到 {remark}, 错误信息: {str(e)}"

            pass

        return res
        pass

    def action_main(self, dict_: dict):

        res = {}

        # driver
        driver = dict_["driver"]
        # 要查找的字符串 xpath
        ele_str = dict_["str"]
        # 元素下标
        index = dict_.get('index', 0)
        # 操作的动作
        action_ = dict_.get('action', "click")
        # 输入的字符串
        input_str = dict_.get('input_str', "empty")
        # 等待时间
        wait_time = dict_.get('wait_time', 30)
        # 循环查看的间隔时间
        interval_second = dict_.get('interval_second', 0.5)
        # 输入的速度
        insert_speed = dict_.get('insert_speed', 0.1)
        # 备注
        remark = dict_['remark']
        # 是否清空, 针对 input
        is_clear = dict_.get("is_clear", False)
        # is——copy input_type
        is_copy = dict_.get("input_type", "input")

        self.inst([dict_, driver, ele_str, action_, index, wait_time, interval_second],
                  [dict, WebDriver, str, str, int, int, float])

        # 等待, 查找元素
        ele = self.wait_element(driver, ele_str, remark, waite_time=wait_time, interval_second=interval_second)

        if ele['mark'] is False:
            res['mark'] = False
            res['result'] = ele['result']
            return res

        try:

            res['mark'] = True
            res['result'] = {'ele': ele['result'], "msg": f"操作成功: {remark}"}

            if action_ == "click":

                ActionChains(driver).click(ele['result'][index]).perform()

                pass
            elif action_ == "double_lick":

                ActionChains(driver).double_click(ele['result'][index]).perform()

                pass
            elif action_ == "input":

                input_str = dict_['input_str']
                # input_style = dict_['input_style']

                self.inst([input_str], [str])

                print(ele['result'])
                if is_clear:
                    ele['result'][index].clear()

                self.manual_input(driver, ele['result'][index], input_str, speed=insert_speed, input_type=is_copy)

                pass
            elif action_ == "delete":

                input_str = dict_['input_str']
                # input_style = dict_['input_style']

                self.inst([input_str], [str])
                self.manual_input(driver, ele['result'][index], input_str, action_=action_)

                pass
            elif action_ == "selection":

                select = Select(ele['result'][index])
                select_type = dict_['select_type']
                select_val = dict_['select_val']
                self.inst([select_type, select_val], [str, str])

                try:

                    if select_type == "text":
                        select.select_by_visible_text(select_val)
                    elif select_type == "value":
                        select.select_by_value(select_val)
                    else:
                        select.select_by_index(select_val)

                except NoSuchElementException as e:

                    res['mark'] = False
                    res['result'] = str(e)
                    return res

                pass

            elif action_ == 'scroll_click':

                ActionChains(driver).move_to_element(ele['result'][index]).perform()
                ActionChains(driver).click(ele['result'][index]).perform()

                pass

            else:

                pass

        except Exception as e:
            res['mark'] = False
            res['result'] = f"操作失败: {remark}, error: {str(e)}"
            pass

        return res
        pass

    # 封装操作函数
    def action(self, dict_: dict):

        res = {}

        # driver
        driver = dict_["driver"]
        # 要查找的字符串 xpath
        ele_str = dict_["str"]
        # 是否复数查找, 目前只做复数
        plural = dict_['plural']
        # 操作的动作
        action_ = dict_["action"]
        # 备注
        remark = dict_['remark']

        self.inst([dict_, driver, ele_str, plural, action_],
                  [dict, WebDriver, str, bool, str])

        if plural:
            # 获取元素集合
            index = dict_['index']
            self.inst([index], [int])

        # 等待 查找元素
        ele = self.wait_element(driver, ele_str, remark, plural=plural)

        if ele['mark'] is False:
            res['mark'] = False
            res['result'] = ele['result']
            return res

        try:

            res['mark'] = True
            res['result'] = f"成功: {remark}"

            if action_ == "click":

                ActionChains(driver).click(ele['result'][index]).perform()

                pass
            elif action_ == "double_lick":
                ActionChains(driver).double_click(ele['result'][index]).perform()

                pass
            elif action_ == "input":

                input_str = dict_['input_str']
                input_style = dict_['input_style']

                self.inst([input_str, input_style], [str, str])
                self.manual_input(driver, ele['result'][index], input_str, action_=input_style)
                pass
            elif action_ == "selection":

                select = Select(ele['result'][index])
                select_type = dict_['select_type']
                select_val = dict_['select_val']
                self.inst([select_type, select_val], [str, str])

                try:

                    if select_type == "text":
                        select.select_by_visible_text(select_val)
                    elif select_type == "value":
                        select.select_by_value(select_val)
                    else:
                        select.select_by_index(select_val)

                except NoSuchElementException as e:

                    res['mark'] = False
                    res['result'] = str(e)
                    return res

                pass

            elif action_ == 'scroll_click':

                ActionChains(driver).move_to_element(ele['result'][index]).perform()
                ActionChains(driver).click(ele['result'][index]).perform()

                pass

            else:

                pass

        except Exception as e:

            res['mark'] = True
            res['result'] = f"失败: {remark}, error: {str(e)}"
            pass

        return res

    # 模拟人工输入
    def manual_input(self, driver, ele, msg, speed=0.1, action_="input", input_type="input"):
        res = {}

        try:
            # print("ele.click")
            ele.click()

        except Exception as e:
            # print("ActionChains(driver).click(ele)")
            ActionChains(driver).click(ele).perform()

        try:
            tag = "写入"

            if input_type == "input":
                for i in msg:
                    if action_ == "input":

                        # ActionChains(driver).send_keys(Keys.RIGHT).perform()
                        ActionChains(driver).send_keys_to_element(ele, i).perform()

                    else:
                        tag = "删除"
                        ActionChains(driver).click(ele).send_keys(Keys.BACK_SPACE).perform()

                    time.sleep(speed)
            else:
                # 复制
                ele.send_keys(msg)
                pass

            res['mark'] = True
            res['result'] = f"成功: {msg} 成功 {tag}"

        except Exception as e:

            res['mark'] = False
            res['result'] = f"失败: {str(e)}"

        return res
        pass

    # 切换窗口句柄
    def switch_to_tab(self, driver, remark, index=0):
        res = {}

        try:
            all_window_handles = driver.window_handles

            driver.switch_to.window(all_window_handles[index])

            res['mark'] = True
            res['result'] = f"成功: tab_ID {index}, {remark}, 切换成功"

        except Exception as e:

            res['mark'] = False
            res['result'] = f"失败: tab_ID {index}, {remark}, 切换失败."

        return res
        pass

    # 进入iframe 或者 出iframe
    def iframe_(self, driver, remark, iframe_tag=None, index=0):

        res = {}

        if iframe_tag is None:
            # 出IFRAME
            driver.switch_to.default_content()
            res['mark'] = True
            res["result"] = driver

            pass
        else:
            # 找IFRAME
            try:
                iframe = driver.find_elements(By.XPATH, iframe_tag)
                driver.switch_to.frame(iframe[index])

                res['mark'] = True
                res["result"] = driver

            except Exception as e:
                res['mark'] = False
                res["result"] = f"失败: {remark}, {str(e)}"

                pass

        return res
        pass

    # 计时等待
    def wait_for_second(self, ele, remark, second):
        second_ = second
        tag = "等待计时"
        self.log_(ele, f"秒", tag=f"{remark} - 开始计时")
        for i in range(0, second):
            self.log_(ele, f"等待: {second_} 秒", tag="计时")
            second_ -= 1
            time.sleep(1)
        self.log_(ele, f"秒", tag=f"{remark} - 结束计时")
        pass

    def log_(self, ele, msg, tag="成功"):

        ele.insert(1.0, tag + " :  " + msg + "\n")
        # self.tk_text_lmrdrurf.insert(1.0, "∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽∽\n")

        self.change_style(ele, tag=tag)

        pass

        # 改变字体粗细, 大小, 颜色

    def change_style(self, ele, tag="成功"):

        if tag == "成功":
            typeface_ = "Helvetica"
            color_ = "#42b983"
            size_ = 11
            thickness_ = "bold"
        elif tag == "计时":
            typeface_ = "Helvetica"
            color_ = "SlateBlue"
            size_ = 11
            thickness_ = "bold"
        elif tag == "警告":
            typeface_ = "Helvetica"
            color_ = "#FFA500"
            size_ = 11
            thickness_ = "bold"
        elif tag == "错误":
            typeface_ = "Helvetica"
            color_ = "red"
            size_ = 11
            thickness_ = "bold"
        else:
            typeface_ = "Helvetica"
            color_ = "#EEB422"
            size_ = 11
            thickness_ = "bold"

        end_ = str(1 + round(len(tag) / 10, 1))

        ele.tag_config(tag, font=(typeface_, size_, thickness_), foreground=color_)
        ele.tag_add(tag, "1.0", end_)

        pass

    # 封装判断传入类型的函数
    def inst(self, var, type_):
        res = {}

        if not isinstance(var, list) or not isinstance(type_, list):
            raise TypeError("inst 传入的参数只能为 list")

        if len(var) == len(type_):

            for i in range(0, len(var)):

                if not isinstance(var[i], type_[i]):
                    raise TypeError(f"inst 传入的参数 {var[i]} 只能为 {type_[i]}")

        else:

            raise IndexError("inst: 传入的list长度必须相等")

        pass

    # 定时执行
    def timing_execute(self, start_time=None, end_time=None):

        mark = False

        current_time = time.localtime()

        if start_time is None and end_time is None:
            raise ValueError("请至少传入一个参数")

        if start_time is not None and end_time is not None:
            raise ValueError("每次只能传入一个参数")

        if end_time is None:
            # 检查是否已到达开始执行的时间
            if current_time >= start_time:
                # 开始执行定时任务
                mark = True
        else:
            # 检查是否已到达停止执行的时间
            if current_time >= end_time:
                mark = True

        return mark

        pass

    # 调用 ADS
    def open_ads(self):

        pass

    # 弹出框
    def message_box(self, messagebox, hint, msg):
        result = messagebox.showinfo("提示框", hint, icon=messagebox.QUESTION,
                                     type=messagebox.YESNO, detail=msg)
        if result == "yes":
            print("用户选择了自定义的 Yes")
        elif result == "no":
            print("用户选择了自定义的 No")
        else:
            print("用户选择了自定义的 Cancel")

        return result
        pass
