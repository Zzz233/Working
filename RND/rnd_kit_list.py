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
    __tablename__ = 'r&d_elisa_kit_list'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(100),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Detail_url = Column(String(1000),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

brand = 'R&D Systems'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; '
                  'x64; rv:82.0) Gecko/20100101 Firefox/82.0', }
for i in range(1, 40):
    url = f'https://www.rndsystems.com/cn/search?category=ELISAs&keywords=%2A&submit=Search&numResults=50&page={i}'
    with requests.Session() as s:
        r = s.get(url=url, headers=headers)
        lxml = etree.HTML(r.text)
        objects = []
        for div in lxml.xpath('//div[@class="search_product_title col-xs-12"]'):
            catano = div.xpath('.//a[@data-position]/@data-id')[0].strip()
            name = div.xpath('.//a[@data-position]/@data-name')[0].strip()
            link = div.xpath('.//a[@data-position]/@href')[0].strip()
            new_data = Data(Brand=brand, Catalog_Number=catano,
                            Product_Name=name, Detail_url=link)
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
        print(i)
    time.sleep(random.uniform(1, 2.5))
