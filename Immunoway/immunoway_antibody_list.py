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
    __tablename__ = 'bp_immunoway_list'

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

for i in range(1, 1277):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; '
                      'x64; rv:82.0) Gecko/20100101 Firefox/82.0',
    }
    url = f'http://www.immunoway.com/index.php/home/product/bigclass/id/9/p/{i}.html'
    objects = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        html_content = etree.HTML(resp.text)
        lxml = html_content.xpath('//div[@class="main"]/div[@class="list"]')
        for item in lxml:
            name = item.xpath('.//p/a/text()')[0].strip()

            link = 'http://www.immunoway.com' + item.xpath('.//p/a/@href')[
                0].strip()
            catano = \
                item.xpath('.//ul/li[contains(text(), "Catalog No.：")]/text()')[
                    0].split('Catalog No.：')[
                    -1].strip()
            print(name, catano, link)
            new_data = Data(Brand='immunoway',
                            Product_Name=name,
                            Antibody_detail_URL=link,
                            Catalog_Number=catano)
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
    time.sleep(1)
