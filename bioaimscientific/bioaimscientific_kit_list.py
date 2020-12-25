import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "bioaimscientific_kit_list"

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
brand = "bioaimscientific"


for i in range(1, 45):
    url = f'https://bioaimscientific.com/product-category/elisa/page/{i}/'
    headers = {
        'Host': 'bioaimscientific.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': f'https://bioaimscientific.com/product-category/elisa/page/{i-1}/',
        # 'Cookie': 'fpestid=aAMzkePsDFGjV2d2FtHNxeZFWutRH_20BPgK916ZPMoAsNQzQbAyB3NkeHbolv3l6V9AIg; _ga=GA1.2.1513056813.1608866763; _gid=GA1.2.345026214.1608866763; wp_woocommerce_session_d1e5af3d299cad260ba9cadc68d52d7d=e95e3e5cd30fe84034386e3fcc741176%7C%7C1609039562%7C%7C1609035962%7C%7Cdc51135a9759f35ba19eb779cf4aa91d',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    with requests.Session() as s:
        resp = s.get(url=url,headers=headers,timeout=60)
        lxml = etree.HTML(resp.text)
        lis = lxml.xpath('//ul[@class="products list"]/li')
        for item in lis:
            name = item.xpath('.//div[@class="product-name"]/h4/a/text()')[0].strip()
            link = item.xpath('.//div[@class="product-name"]/h4/a/@href')[0].strip()
            catano = item.xpath('.//span[@class="sku"]/text()')[0].strip()
            # print(catano,name,link)
            new_data = Data(
                Brand=brand, Catalog_Number=catano, Product_Name=name, Detail_url=link
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
    time.sleep(random.uniform(0.5, 1.0))
