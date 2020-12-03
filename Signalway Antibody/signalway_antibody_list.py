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
    __tablename__ = "bp_sab_list"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Application = Column(String(1000), nullable=True, comment="")
    Antibody_detail_URL = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Antibody_Type = Column(String(100), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
    "x64; rv:82.0) Gecko/20100101 Firefox/82.0",
}

for i in range(359, 4868):
    url = f"https://www.sabbiotech.com.cn/c-46-Products-b0-min0-max0-attr0-{i}-last_update-DESC.html"
    print(url)
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        # print(resp.text)
        html = etree.HTML(resp.text)
        div = html.xpath('//div[@class="item"]')
        objects = []
        for item in div:
            name = item.xpath('.//a[@class="item-title"]/text()')[0].strip()
            link = (
                "https://www.sabbiotech.com.cn/"
                + item.xpath('.//a[@class="item-title"]/@href')[0].strip()
            )
            catano = item.xpath('.//a[@class="item-title"]/span/text()')[0].strip()
            # print(name, catano, link)
            new_data = Data(
                Brand="Signalway Antibody",
                Catalog_Number=catano,
                Product_Name=name,
                Antibody_detail_URL=link,
            )
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
        print("done")

    time.sleep(random.uniform(2.0, 2.5))
