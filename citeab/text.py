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

user_agent = 'MQQBrowser/3.7/Mozilla/5.0 (Linux; U; Android 4.0.3; zh-cn; M9 Build/IML74K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 Normal Mode'
page_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'www.citeab.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.citeab.com/antibodies/search?q=p53&page=4',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'MQQBrowser/3.7/Mozilla/5.0 (Linux; U; Android 4.0.3; zh-cn; M9 Build/IML74K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 Normal Mode',
}

profile = webdriver.FirefoxProfile()
ip = ip
port = int(port)
settings = {
    'network.proxy.type': 1,  # 0: 不使用代理；1: 手动配置代理
    'network.proxy.http': ip,
    'network.proxy.http_port': port,
    'network.proxy.ssl': ip,  # https的网站,
    'network.proxy.ssl_port': port,
    'general.useragent.override': user_agent
}
for key, value in settings.items():
    profile.set_preference(key, value)
profile.update_preferences()
options = Options()
browser = webdriver.Firefox(firefox_profile=profile,
                            options=options)
wait = WebDriverWait(browser, 6)
browser.get(url)
input("通过验证码跳转后:")
