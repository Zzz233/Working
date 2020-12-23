from collections import namedtuple
import requests
from lxml import etree
from requests.api import head
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "raybiotech_kit_list"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
brand = "raybiotech"
headers = {
    "Host": "www.raybiotech.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # "Accept-Encoding": "gzip, deflate, br",
    "X-Moz": "prefetch",
    "Connection": "keep-alive",
    "Referer": "https://www.raybiotech.com/products/elisa/",
    # "Cookie": "__cfduid=dc9577877a83f9370b79ee9fb2f60b1ba1608685173; sid_customer_07a72=c5c9726a007fb1fbffee1a7c0651c58c-1-C; _ga=GA1.2.1317898597.1608685177; _gid=GA1.2.66379256.1608685177; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-site_visit_time=1608685447236; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-visit_count=%7B%22https%3A//*%22%3A4%2C%22website_count%22%3A4%7D; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-ag9zfmNsaWNrZGVza2NoYXRyHAsSD3Byb2FjdGl2ZV9ydWxlcxiAgKCh5suuCwwonce_per_sessionnull=true; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-ag9zfmNsaWNrZGVza2NoYXRyHAsSD3Byb2FjdGl2ZV9ydWxlcxiAgKChn7LFCgwonce_per_sessionnull=true",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
for i in range(1, 339):
    url = f"https://www.raybiotech.com/products/elisa/page-{i}/"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        resp.encoding = "utf-8"
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('//div[@class="ty-product-list__info"]')
        for div in divs:
            name = div.xpath('.//a[@class="product-title"]/@title')[0].strip()
            link = div.xpath('.//a[@class="product-title"]/@href')[0].strip()
            catanum = div.xpath(
                './/label[@class="ty-control-group__label"]/following-sibling::span[@class][@id]/text()'
            )[0].strip()
            # print(catanum, name, link)
            new_data = Data(
                Brand=brand, Catalog_Number=catanum, Detail_url=link, Product_Name=name
            )
            session.add(new_data)
    session.commit()
    session.close()
    print(i, "done")
    time.sleep(random.uniform(1.0, 2.0))
