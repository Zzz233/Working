import time
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
# chrome_options.add_argument("--headless")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
)
from_web = "114.230.64.208:4278"
proxies = {"http": "http://" + from_web, "https": "https://" + from_web}

chrome_options.add_argument(("--proxy-server=" + from_web))
driver = Chrome(
    "D:\\Dev\\bio_work\\new_venv\\Scripts\\chromedriver", options=chrome_options
)

with open("D:\\Dev\\bio_work\\stealth.min.js") as f:
    js = f.read()

driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": js})
driver.get(
    "https://www.citeab.com/antibodies/223835-ab19026-anti-adam-10-antibody-ct/publications?page=4"
)
input("输入继续")
# driver.save_screenshot("webdriver/walkaround.png")

# 你可以保存源代码为 html 再双击打开，查看完整结果
# source = driver.page_source
# with open("webdriver/result.html", "w") as f:
#     f.write(source)

driver.quit()


# import time
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.firefox.options import Options

# user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'

# profile = webdriver.FirefoxProfile()
# # ip = self.ip
# # port = int(self.port)

# settings = {
#     'network.proxy.type': 1,  # 0: 不使用代理；1: 手动配置代理
#     'network.proxy.http': '122.143.134.248',
#     'network.proxy.http_port': 4278,
#     'network.proxy.ssl': '122.143.134.248',  # https的网站,
#     'network.proxy.ssl_port': 4278,
#     'general.useragent.override': user_agent
# }
# for key, value in settings.items():
#     profile.set_preference(key, value)
# profile.update_preferences()
# options = Options()
# browser = webdriver.Firefox(firefox_profile=profile,
#                             options=options)
# wait = WebDriverWait(browser, 6)
# browser.get(
#     'https://www.citeab.com/antibodies/2408005-564028-bd-horizon-bv786-rat-anti-mouse-igm/publications?page=1')
# input("通过验证码跳转后:")
