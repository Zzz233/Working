import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random
import redis

Base = declarative_base()


class Price(Base):
    __tablename__ = "sigmaaldrich_kit_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


# Mysql
engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)
payload = {
    # "productNumber": "RAB0441",
    # "brandKey": "SIGMA",
    # "divId": "pricingContainerMessage",
    # "isRollup": 0,
    "loadFor": "PRD_RS",
}

headers = {
    "Host": "www.sigmaaldrich.com",
    "Connection": "keep-alive",
    "Content-Length": "14",
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "x-dtpc": "5$267355401_696h2vBIKUDDTOVNNHUDAFWUUKEWOOQQAHEQPT-0e3",
    "Origin": "https://www.sigmaaldrich.com",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.sigmaaldrich.com/catalog/product/sigma/rab0441?lang=en&region=HK",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cookie": "SialLocaleDef=CountryCode~CN|WebLang~-7|",
}
while r.exists("sigmaaldrich_price"):
    catano = r.lpop("sigmaaldrich_price")
    print(catano)
    url = f"https://www.sigmaaldrich.com/catalog/PricingAvailability.do?productNumber={catano}&brandKey=SIGMA&divId=pricingContainerMessage&isRollup=0"
    try:
        resp = requests.post(url=url, data=payload, headers=headers)
        lxml = etree.HTML(resp.text)
        # print(resp.text)
        # print(lxml)
        sub_catanum = lxml.xpath("//strong/text()")[0].strip()

        size = lxml.xpath('//td[@class="packSize"]/text()')[0].strip()
        price = "CYN " + lxml.xpath('//td[@class="price"]/p/text()')[0].strip()
        print(sub_catanum, size, price)
        new_price = Price(
            Catalog_Number=catano, sub_Catalog_Number=catano, Size=size, Price=price
        )
    except Exception as e:
        print(e)
        r.rpush("sigmaaldrich_price", catano)
        print("sleeping...")
        time.sleep(30)
        continue
    session.add(new_price)
    session.commit()
    session.close()
    print("done")

    time.sleep(random.uniform(1.0, 1.5))
