import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "adipogen_kit_list"

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
brand = "adipogen"

headers = {
    "Host": "adipogen.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://adipogen.com/elisa-kits.html?p=1",
    "Connection": "keep-alive",
    "Cookie": "mage-translation-storage=%7B%7D; mage-translation-file-version=%7B%7D; PHPSESSID=9bb80934853ffb3babfa13bc7df0ffbc; form_key=iNlLuqb9Ql5ueXfV; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; _ga=GA1.2.658866885.1609297013; _gid=GA1.2.1551139616.1609297013; form_key=iNlLuqb9Ql5ueXfV; store=cn; X-Magento-Vary=0af65036ebb608ce5a0a29a665067cc29db28878; mage-messages=; section_data_ids=%7B%22cart%22%3A1609297037%7D",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}

for i in range(1, 11):
    url = f"https://adipogen.com/elisa-kits.html?p={i}&product_list_limit=25"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        lxml = etree.HTML(resp.text)
        lis = lxml.xpath('//li[@class="item product product-item"]')
        for item in lis:
            link = item.xpath('.//a[@class="product-item-link"]/@href')[0].strip()
            name = item.xpath(
                './/strong[@class="product name product-item-name"]/text()'
            )[0].strip()
            print(name, link)
            new_data = Data(Brand=brand, Product_Name=name, Detail_url=link)
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
