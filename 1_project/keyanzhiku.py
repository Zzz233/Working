import requests
from lxml import etree
import time

headers = {
    # "Host": "fund.keyanzhiku.com",
    # "Connection": "keep-alive",
    # "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    # "Referer": "http://fund.keyanzhiku.com/Index/index.html?title=&fname=&danwei=&pzh=&xk_name=%E5%BE%AE%E7%94%9F%E7%89%A9%E5%AD%A6&xkid=834&start_year=0&end_year=0&xmid=0&start_money=&end_money=&search=1&px_year=&px_money=",
    # "Accept-Encoding": "gzip, deflate",
    # "Accept-Language": "zh-CN,zh;q=0.9",
    # "Cookie": "PHPSESSID=j56i8i39kk9c24jbh75te2dko0",
}
for i in range(2, 3):
    url = f"http://fund.keyanzhiku.com/Index/index/xk_name/%E4%B8%8D%E9%99%90/xkid/0/start_year/0/end_year/0/xmid/0/search/1/p/{i}.html"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        try:
            lxml = etree.HTML(resp.text)
            li_tags = lxml.xpath(
                '//ul[@class="layuiadmin-card-status layuiadmin-home2-usernote"]/a/li'
            )
        except Exception as e:
            print("出错了", e)
            time.sleep(1)
            continue
        for a in li_tags:
            # 标题
            title = a.xpath(".//h3/text()")[0].strip()
            # 负责人
            try:
                headhead = (
                    a.xpath('.//span[contains(text(), "负责人：")]/text()')[0]
                    .split("负责人：")[-1]
                    .strip()
                )
            except Exception:
                headhead = None
            # 申请单位
            try:
                organization = (
                    a.xpath('.//span[contains(text(), "申请单位：")]/text()')[0]
                    .split("申请单位：")[-1]
                    .strip()
                )
            except Exception:
                organization = None
            # 研究类型
            try:
                research_type = (
                    a.xpath('.//span[contains(text(), "研究类型：")]/text()')[0]
                    .split("研究类型：")[-1]
                    .strip()
                )
            except Exception:
                research_type = None
            # 项目批准号
            try:
                approve_num = (
                    a.xpath('.//span[contains(text(), "项目批准号：")]/text()')[0]
                    .split("项目批准号：")[-1]
                    .strip()
                )
            except Exception:
                approve_num = None
            # 批准年度：
            try:
                year = (
                    a.xpath('.//span[contains(text(), "批准年度：")]/text()')[0]
                    .split("批准年度：")[-1]
                    .strip()
                )
            except Exception:
                year = None
            # 金额
            try:
                cash = (
                    a.xpath('.//span[contains(text(), "金额：")]/text()')[0]
                    .split("金额：")[-1]
                    .strip()
                )
            except Exception:
                cash = None
            # 关键字
            try:
                key_word = (
                    a.xpath('.//span[contains(text(), "关键词：")]/text()')[0]
                    .split("关键词：")[-1]
                    .strip()
                )
            except Exception:
                key_word = None
            print(
                title,
                headhead,
                organization,
                research_type,
                approve_num,
                year,
                cash,
                key_word,
            )
            print(i, "done")
