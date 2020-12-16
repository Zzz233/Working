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
    __tablename__ = "fitzgerald_kit_list"

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

headers = {
    "Host": "www.fitzgerald-fii.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.fitzgerald-fii.com/research-kits.html?applications=81&p=1",
    "Connection": "keep-alive",
    # "Cookie": "__cfduid=d270e0f9495cb5ad09117901733892bc01607995320; frontend=8jahiq9riki8q30midkr6ap6f0; frontend_cid=KYYqh9eKRvi6r76I; __utma=229056011.1395026765.1607995368.1607995368.1607995368.1; __utmb=229056011.5.10.1607995368; __utmc=229056011; __utmz=229056011.1607995368.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); external_no_cache=1; fpestid=o6Pzn-Br8-UeWo9vazGlzVcegorcEqPRNdL_SWzJLbkry4GBRMbCypbnsVr8w6Q7syaIrw",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

brand = "fitzgerald-fii"
for i in range(1, 107):
    url = f"https://www.fitzgerald-fii.com/research-kits.html?applications=81&p={i}"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        resp.encoding = "utf-8"
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('//h2[@class="product-title font clearfix"]')
        for div in divs:
            link = div.xpath(".//a/@href")[0].strip()
            name = div.xpath('.//span[@itemprop="name"]/text()')[0].strip()
            catanum = (
                div.xpath('.//span[@class="catalog-no"]/text()')[0]
                .lstrip("(")
                .rstrip(")")
            )
            print(name, catanum, link)
            new_data = Data(
                Brand=brand, Catalog_Number=catanum, Product_Name=name, Detail_url=link
            )
            try:
                session.add(new_data)
                session.commit()
                session.close()
            except Exception as e:
                session.rollback()
                print(e)
                continue
        print(i, "done")
    time.sleep(random.uniform(1.0, 1.5))
