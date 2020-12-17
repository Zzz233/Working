import redis
import requests
from lxml import etree
import math

UA = {
    "Host": "fund.keyanzhiku.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": f"http://fund.keyanzhiku.com/Index/index/start_year/1987/end_year/1987/xmid/0/search/1/px_year/desc/p/2.html",
    "Cookie": "PHPSESSID=djcet75ohhmtadbgq3lp20r2c3",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=5)
r = redis.Redis(connection_pool=pool)
for year in range(1988, 2020):
    url = f"http://fund.keyanzhiku.com/Index/index/start_year/{year}/end_year/{year}/xmid/0/search/1/px_year/desc/p/1.html"
    resp = requests.get(url=url, headers=UA)
    lxml = etree.HTML(resp.text)
    sum = int(lxml.xpath('//span[@style="color:#be1a21"][1]/text()')[0].strip())
    if sum % 20 == 0:
        pages = (sum / 20) + 1
    else:
        pages = (math.ceil(sum / 20)) + 1
    for i in range(1, pages):
        item = str(year) + "," + str(i)
        r.rpush("keyanzhiku_pagenum", item)
        print("done")
pool.disconnect()
