import re
import requests
import threading
from queue import Queue

import time
import random
import pymysql
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'

profile = webdriver.FirefoxProfile()
# ip = self.ip
# port = int(self.port)

settings = {
    'network.proxy.type': 1,  # 0: 不使用代理；1: 手动配置代理
    'network.proxy.http': '122.143.134.248',
    'network.proxy.http_port': 4278,
    'network.proxy.ssl': '122.143.134.248',  # https的网站,
    'network.proxy.ssl_port': 4278,
    'general.useragent.override': user_agent
}
for key, value in settings.items():
    profile.set_preference(key, value)
profile.update_preferences()
options = Options()
browser = webdriver.Firefox(firefox_profile=profile,
                            options=options)
wait = WebDriverWait(browser, 6)
browser.get(
    'https://www.citeab.com/antibodies/2408005-564028-bd-horizon-bv786-rat-anti-mouse-igm/publications?page=1')
input("通过验证码跳转后:")
