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
    __tablename__ = "abbkine_kit_list"

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
brand = "abbkine"

for i in range(124, 450):
    url = f"https://www.abbkine.com/product-category/assay-kit/elisa-kit/page/{i}/"
    with requests.Session() as s:
        resp = s.get(url=url)
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('//div[@class="box-rht"]/div[@class="cls-item"]')
        for item in divs:
            link = item.xpath(
                './div[@class="type-text"]/div[@class="text-tit"]/h3/a/@href'
            )[0].strip()
            catano = item.xpath(
                './div[@class="type-text"]/div[@class="text-tit"]/h3/a/span[@class="size"]/text()'
            )[0].strip()
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
    time.sleep(random.uniform(1.5, 3.0))
