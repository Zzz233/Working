from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

# driver = webdriver.Chrome()
driver = webdriver.Firefox(executable_path="geckodriver")
webdriver.FirefoxOptions()
driver.get(
    'https://www.citeab.com/antibodies/2041744-11-5773-foxp3-monoclonal-antibody-fjk-16s-fitc-e/publications?page=11')
# WebDriverWait(driver, 240, poll_frequency=0.5).until(
#             EC.presence_of_element_located((By.CLASS_NAME, "cell large-9"))
#         )
input("input:")

cookie_dict = {}
print(driver.get_cookies())
for cookie in driver.get_cookies():
    # print("%s -> %s" % (cookie['name'], cookie['value']))
    single_dict = {cookie['name']: cookie['value']}
    print(single_dict)
    cookie_dict.update(single_dict)
    # print(cookie['name'] + '=' + cookie['value'])
# print(cookie_dict)
cookie_str = '_citeab-live-session=' + cookie_dict[
    '_citeab-live-session'] + '; ' \
             + '__utma=' + cookie_dict['__utma'] + '; ' \
             + '__utmb=' + cookie_dict['__utmb'] + '; ' \
             + '__utmc=' + cookie_dict['__utmc'] + '; ' \
             + '__utmz=' + cookie_dict['__utmz'] + '; ' \
             + '__utmt=' + cookie_dict['__utmt']
print(cookie_str)
# driver.quit()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Cookie': cookie_str,
    'Host': 'www.citeab.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.citeab.com/antibodies/2041744-11-5773-foxp3-monoclonal-antibody-fjk-16s-fitc-e/publications?page=11',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
}

with requests.Session() as s:
    # proxies = {'http': '49.88.245.164:45257',
    #            'https': '49.88.245.164:45257'}
    resp = s.get(
        url='https://www.citeab.com/antibodies/2041744-11-5773-foxp3-monoclonal-antibody-fjk-16s-fitc-e/publications?page=11',
        headers=headers)
    print(resp.text)

# options = webdriver.FirefoxOptions()
# options.add_argument(('--proxy-server=' + self.proxies))
# browser = webdriver.Firefox(executable_path="geckodriver", options=options)
# wait = WebDriverWait(browser, 6)
# browser.maximize_window()
# browser.get(url)
