import requests

# username = 'ELEEEBY5r1JiN3ep'
# key = 'cLSj17Kl5Zr4&num=10&cityId=all'
# resp = requests.get(
#     f'http://httpbapi.dobel.cn/User/query&account={username}&accountKey={key}')
# print(resp.json()['remain'])

# proxies = {'http': '183.162.144.192:24639', 'https': '183.162.144.192:24639'}
# url = 'https://www.citeab.com/antibodies/2271508-arh03-anti-rhoa-antibody-mouse-mab-control/publications?page=3'
# print(requests.get(url=url, proxies=proxies))
# coding=utf-8
import requests

# 请求地址
targetUrl = "https://www.baidu.com"

# 代理服务器
proxyHost = "ip"
proxyPort = "port"

proxyMeta = "http://%(host)s:%(port)s" % {

    "host": proxyHost,
    "port": proxyPort,
}

# pip install -U requests[socks]  socks5
# proxyMeta = "socks5://%(host)s:%(port)s" % {

#     "host" : proxyHost,

#     "port" : proxyPort,

# }

proxies = {

    "http": proxyMeta,
    "https": proxyMeta
}

resp = requests.get(targetUrl, proxies=proxies)
print(resp.status_code)
print(resp.text)
