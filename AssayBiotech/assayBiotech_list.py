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
    __tablename__ = "assaybiotech_kit_list"

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
brand = "assaybiotechnology"

headers = {
    # "Host": "www.abbexa.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    # "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # "Accept-Encoding": "gzip, deflate, br",
    # "Connection": "keep-alive",
    # "Referer": "https://www.abbexa.com/products/elisa-kits",
    # "Cookie": "PHPSESSID=90554d0188c8e2a7fa47551bdc3065c6; language=en; currency=USD; _gcl_au=1.1.678391277.1608168886; _ga=GA1.2.61988102.1608168887; _gid=GA1.2.713055285.1608168887; _gat_gtag_UA_41647028_1=1",
    # "Upgrade-Insecure-Requests": "1",
    # "Pragma": "no-cache",
    # "Cache-Control": "no-cache",
}
for i in range(1, 133):
    url = f"https://www.assaybiotechnology.com/category-91_97-{i}.html"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=120)
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('//div[@class="sku"]')
        for item in divs:
            link = item.xpath("./a/@href")[0].strip()
            catano = item.xpath("./a/text()")[0].strip()
            # print(link, catano)
            new_data = Data(Brand=brand, Catalog_Number=catano, Detail_url=link)
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