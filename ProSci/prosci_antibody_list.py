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
    __tablename__ = 'bp_prosci_list'

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

for i in range(1, 773):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/'
                      '537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    }
    url = f'https://www.prosci-inc.com/primary-antibodies.html?limit=50&p={i}'
    objects = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        lxml = etree.HTML(resp.text)
        lis = lxml.xpath(
            '//ul[@class="product-list solar-search"]/li[@class]')
        for li in lis:
            link = li.xpath('.//span[@class="name"]/a/@href')[0]
            name = li.xpath('.//span[@class="name"]/a/@title')[0]
            catano = \
                li.xpath(
                    './/p[@class="product-sku"]/span[@class="name"]/text()')[
                    0].strip()
            # print(name, catano, link)
            new_data = Data(Brand='prosci',
                            Product_Name=name,
                            Antibody_detail_URL=link,
                            Catalog_Number=catano)
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
    print(i)
    time.sleep(random.uniform(2, 3.5))
