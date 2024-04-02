import sys
import os
import time

# 获取当前脚本所在的目录的父目录（父包的路径）
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 将父包路径添加到系统路径中
sys.path.append(parent_dir)


from DrissionPage import WebPage, ChromiumOptions, ChromiumPage, SessionOptions
from factory.factory import stone
from selenium.webdriver.common.by import By
"""
    测试 page 与 selenium 的互动与连接
"""
st = stone()

res = st.visit_website(tag='test_selenium_drrinsionPage')

co = ChromiumOptions()

print(res["result"]["port"])

time.sleep(1)

co.set_address(f'127.0.0.1:{res["result"]["port"]}')

page = WebPage(chromium_options=co)

page.get('http://www.taobao.com')

page('#q').input('python')

ipt = st.driver.find_element(By.XPATH, '//input[@id="q"]')

ipt.send_keys('java')

page('.btn-search tb-bg').click()

page.wait(3)

st.iframe_(st.driver, 'test', '//iframe[contains(@src, "https://login.taobao.com/member/login.jhtml?")]')

ele = st.driver.find_element(By.XPATH, '//button[text()="快速进入"]')

ele.click()

st.iframe_(st.driver, 'test')

item = page('xpath: //div[@data-name="itemExp"]')

page_new_tab = item.click.for_new_tab()

page_new_tab.wait.load_start()

tab = page_new_tab
print(f'tab: {tab}')

# 获取所有窗口句柄
window_handles = st.driver.window_handles

# 根据索引选择要切换的窗口
window_handle = window_handles[-1]

# 切换到指定窗口
# st.driver.switch_to.window(window_handle)
st.driver.switch_to.window(window_handle)

time.sleep(3)

text = st.driver.find_element(By.XPATH, '//div[contains(@class, "ItemHeader--root--")]').text

print(f"text: {text}")
