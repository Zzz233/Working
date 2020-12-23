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
    __tablename__ = "taiclone_kit_list"

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


brand = "taiclone"

for i in range(606, 1185):
    url = f"https://taiclone.com/elisa-kits/?page={i}"
    headers = {
        "Host": "taiclone.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
        "Referer": f"https://taiclone.com/elisa-kits/?page={i-1}",
        # "Cookie": "osCsid=qa7icrqqu7tstmno6lijf46fp7; _pk_id.1.e472=511537c78983c42c.1608702646.1.1608702881.1608702646.; _pk_ses.1.e472=1",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, verify=False)
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('.//div[@id="plist"]/div')
        for div in divs:
            name = div.xpath("./span/a/text()")[0].strip()
            link = "https://taiclone.com" + div.xpath("./span/a/@href")[0].strip()
            catanum = div.xpath('./span/span[contains(text(), "Catalog No: ")]/text()')[
                0
            ].split("Catalog No: ")[-1]
            # print(catanum, name, link)
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
    time.sleep(random.uniform(0.5, 1.0))