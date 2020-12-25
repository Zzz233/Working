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
    __tablename__ = "neobiolab_kit_list"

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
brand = "neobiolab"

for i in range(1, 52):
    url = f'https://neobiolab.com/sheep-elisa-kits/{i}.html'
    with requests.Session() as s:
        resp = s.get(url=url)
        # print(resp.text)
        lxml = etree.HTML(resp.text)
        trs = lxml.xpath('//table[@class="col-sm-12 table-bordered cf table-hover"]/tbody/tr')
        for item in trs:
            catano = item.xpath('./td[@data-title="Cat No."]/text()')[0].strip()
            try:
                name = item.xpath('./td[@data-title="Name"]/a/text()')[0].strip()
            except Exception:
                continue
            link = 'https://neobiolab.com' + item.xpath('./td[@data-title="Name"]/a/@href')[0].strip()
            # print(catano, name, link)
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

