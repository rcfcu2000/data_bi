# Python program to demonstrate 
# type: ignore

import os
import sys
import urllib.request

from time import sleep
from retry import retry

# import webdriver 
from selenium import webdriver 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import WebDriverException

use_chrome = False

if use_chrome:
    
    options = webdriver.ChromeOptions()  #创建浏览器
    #options.add_argument('user-data-dir=e:\\sycm')
    #options.add_argument('profile-directory=Profile 1')
    #设定下载文件的保存目录为D盘
    prefs = {"download.default_directory": default_file_path}
    options.add_experimental_option("prefs", prefs)

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--enable-chrome-browser-cloud-management")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-webgl")

    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option("excludeSwitches", ['enable-automation'])
    driver = webdriver.Chrome(options=options)  #创建浏览器对象
else:
    # 指定GeckoDriver的路径
    # gecko_driver_path = 'D:/geckodriver/geckodriver.exe'
    
    options = Options()
    # options.add_argument('-profile')
    # options.add_argument(r"d:\91jiafang")
    # options.add_argument(gecko_driver_path)
    #firefox_profile = FirefoxProfile(r"d:\91jiafang");
    #firefox_profile.set_preference("javascript.enabled", True)
    #print(firefox_profile.profile_dir)
    #options.profile = firefox_profile
    driver = webdriver.Firefox(options=options)

driver.get('https://tauacgr5lqv.feishu.cn/docx/ZnPedHE3loaQ5MxwE7TctC6PnNh')

# 使用JavaScript获取浏览器环境信息
browser_info = driver.execute_script("""
    return {
        userAgent: navigator.userAgent,
        browserVersion: navigator.appVersion,
        platform: navigator.platform,
        language: navigator.language,
        screenWidth: screen.width,
        screenHeight: screen.height,
        plugins: Array.from(navigator.plugins).map(plugin => ({ name: plugin.name, filename: plugin.filename })),
        cookieEnabled: navigator.cookieEnabled,
        javaEnabled: navigator.javaEnabled(),
        viewportWidth: window.innerWidth,
        viewportHeight: window.innerHeight,
        localStorageEnabled: window.localStorage != null,
        sessionStorageEnabled: window.sessionStorage != null,
        webRTCEnabled: navigator.mediaDevices != null && typeof navigator.mediaDevices.getUserMedia === 'function'
    };
""")

print(browser_info)