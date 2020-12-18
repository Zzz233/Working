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
    __tablename__ = "biomatik_kit_list"

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
brand = "biomatik"


for i in range(1, 1855):
    with requests.session() as s:
        url = f"https://www.biomatik.com/elisa-kits/?sort=featured&page={i}"
        resp = s.get(url=url)
        lxml = etree.HTML(resp.text)
        figures = lxml.xpath('//figcaption[@class="prod-desc"]')
        for item in figures:
            link = item.xpath(".//h4/a/@href")[0].strip()
            name = (
                item.xpath(".//h4/a/text()")[0]
                .strip()
                .replace("                  ", " ")
            )
            catano = (
                item.xpath(
                    './/div[@class="prod-summary"]/p/a[@data-product-id]/text()'
                )[0]
                .replace(":", "")
                .strip()
            )
            print(link, name, catano)
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
    time.sleep(random.uniform(1.0, 1.5))