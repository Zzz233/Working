import random
import requests
from lxml import etree
import redis
import time


# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=4)
r = redis.Redis(connection_pool=pool)

headers = {
    "Host": "www.letpub.com.cn",
    "Connection": "keep-alive",
    "Content-Length": "183",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.letpub.com.cn",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.letpub.com.cn/index.php?page=grant",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    # "Cookie": "_ga=GA1.3.197927283.1606913066; __utmz=189275190.1606913067.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _gid=GA1.3.1985560273.1607948592; PHPSESSID=m7u74d594u4df0s6nd0nbrip72; __utma=189275190.197927283.1606913066.1608012077.1608078793.5; __utmc=189275190; __utmt=1; __utmb=189275190.4.10.1608078793",
}


def get_proxy():
    proxy_url = "http://http.tiqu.letecs.com/getip3?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4"
    res = requests.get(proxy_url)
    proxy_text = res.text.replace("\r\n", "")
    print(proxy_text)
    proxies = {
        "http": "http://" + proxy_text,
        "https": "http://" + proxy_text,
    }
    return proxies


proxies = {
    "http": "http://" + "223.244.175.184:41626",
    "https": "http://" + "223.244.175.184:41626",
}

for i in range(100):
    # while r.exists("academy"):
    # data_redis = r.lpop("academy")
    # s1 = data_redis.split(",")[0]
    # s2 = data_redis.split(",")[1]
    # if s2 == "None":
    #     s2 = ""
    # s3 = data_redis.split(",")[2]
    # if s3 == "None":
    #     s3 = ""
    # s4 = data_redis.split(",")[2]
    # if s4 == "None":
    #     s4 = ""
    s1 = "C"
    s2 = "C19"
    s3 = "C1901"
    s4 = "C190105"
    data = {
        "page": "",
        "name": "",
        "person": "",
        "no": "",
        "company": "",
        "addcomment_s1": s1,
        "addcomment_s2": s2,
        "addcomment_s3": s3,
        "addcomment_s4": s4,
        "money1": "",
        "money2": "",
        "startTime": "1997",
        "endTime": "2019",
        "subcategory": "",
        "searchsubmit": "true",
    }

    url = "https://www.letpub.com.cn/nsfcfund_search.php?mode=advanced&datakind=list&currentpage=1"
    with requests.Session() as s:
        try:
            resp = s.post(url=url, data=data, headers=headers)
            lxml = etree.HTML(resp.text)
            # print(resp.text)
            div = lxml.xpath('//div[contains(text(), "搜索条件匹配：")]/b/text()')[0].strip()
            # final_result = data_redis + "," + div
            r.rpush("academy_result", div)
            print(div)
        except Exception as e:
            # r.rpush("academy_bak", data_redis)
            # proxies = get_proxy()
            # print("换ip了")
            print(e)
    # print(data)

    # time.sleep(random.uniform(2.0, 2.5))
