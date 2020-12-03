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
    __tablename__ = 'bp_diagenode_list'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Application = Column(String(1000),
                         nullable=True, comment='')
    Antibody_detail_URL = Column(String(500),
                                 nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    Antibody_Type = Column(String(100),
                           nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

for i in range(1):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/'
                      '537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    }
    url = 'https://www.diagenode.com/cn/categories/all-antibodies'
    objects = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        lxml = etree.HTML(resp.text)
        trs = lxml.xpath(
            '//tbody[@class="list"]/tr')
        for tr in trs:
            try:
                name = tr.xpath('.//td[@class="name"]/strong/a/text()')[
                    0].strip()
            except Exception as e:
                name = tr.xpath('.//td[@class="name"]/a/text()')[
                    0].strip()
            catano = tr.xpath('.//td[@class="catalog_number"]/span/text()')[
                0].strip()
            try:
                link = 'https://www.diagenode.com' + \
                       tr.xpath('.//td[@class="name"]/strong/a/@href')[
                           0].strip()
            except Exception as e:
                link = 'https://www.diagenode.com' + \
                       tr.xpath('.//td[@class="name"]/a/@href')[
                           0].strip()
            print(name, catano, link)
            new_data = Data(Brand='diagenode',
                            Product_Name=name,
                            Antibody_detail_URL=link,
                            Catalog_Number=catano)
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
    print(trs)
    break
