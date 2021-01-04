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
    __tablename__ = "assaypro_kit_list"

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
brand = "assaypro"
headers = {
    "Host": "www.biovision.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.biovision.com/products/elisa-kits/elisa-kits-a-z.html?p=5",
    "Connection": "keep-alive",
    "Cookie": "__cfduid=de57897235ad044a7c7c82b5573aa6fd61609396235; frontend=n3n1inkb494u7f76ifibdg5ac0; frontend_cid=jAOtqLsCOMnUQwnv; es_newssubscriber=1; _ga=GA1.2.267697855.1609396239; _gid=GA1.2.1926174020.1609396239; _fbp=fb.1.1609396240502.1808084058; __hstc=132909585.4ee25c53fd41d06a3175df8341baacc8.1609396241678.1609396241678.1609396241678.1; hubspotutk=4ee25c53fd41d06a3175df8341baacc8; __hssrc=1; __hssc=132909585.4.1609396241679",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
for i in range(1, 6):
    with requests.Session() as s:
        url = f"https://assaypro.com/Products/TypeDetails?type=AssayMax%3F%20ELISA%20Kits&page={i}"
        resp = s.get(url=url)
        # print(resp.text)
        lxml = etree.HTML(resp.text)
        trs = lxml.xpath('//tbody[@id="productListTable"]/tr')
        for item in trs:
            link = (
                "https://assaypro.com"
                + item.xpath('./td[@id="links"]/a/@href')[0].strip()
            )
            name = item.xpath('./td[@id="links"]/a/text()')[0].strip()
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
