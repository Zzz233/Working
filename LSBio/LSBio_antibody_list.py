import requests
from lxml import etree
import json
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = 'bp_lsbio_list'

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

headers = {
    'Accept': 'text/html, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '240',
    'Content-Type': 'application/json',
    # 'Cookie': 'LifespanSessionId=40jaq2gx0333drdq0kooxv4z; _ga=GA1.2.794530871.1605058822; _gid=GA1.2.494831304.1605058822; _uetsid=dc583f6023be11eba6d20bd3635e402f; _uetvid=dc583be023be11ebb0f62505d1bedd2a; _gat_UA-2011723-1=1',
    'Host': 'www.lsbio.com',
    'Origin': 'https://www.lsbio.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.lsbio.com/products/antibodies/primary-antibodies',
    'TE': 'Trailers',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
    'X-Requested-With': 'XMLHttpRequest',
}
url = 'https://www.lsbio.com/search/pagenext'
for i in range(22000, 22183):
    payload = {"SearchType": 1, "UserQuery": "", "UserFilterQueries": None,
               "Root": None, "ServerFilterQueries": ["Type:Primary"],
               "Prefix": "a",
               "ProductPrefixFilterQueries": None, "Page": i, "PageSize": 25,
               "ResultsFound": 554593, "IsExactSearch": False,
               "GroupSortBy": None}
    with requests.Session() as s:
        resp = s.post(url=url, headers=headers, data=json.dumps(payload))
        # print(resp.text)
        html = etree.HTML(resp.text)
        status = 0

    divs = html.xpath('//div[@class="l-SearchPageResult__Details"]')
    # TODO Brand, Catalog_Number, Product_Name, Application, Antibody_detail_URL,
    #  Crawl_Date, Note, Antibody_Status, Antibody_Type

    objects = []
    for item in divs:
        brand = 'LSBio'
        name = item.xpath('.//a/text()')[0].replace(
            '\r\n                ', '').replace('\r\n            ', '')
        catno = name.split(' ')[-1]
        if item.xpath('.//label[contains(text(), "Applications:")]'):
            application = item.xpath(
                './/label[contains(text(), "Applications:")]/following-sibling::div[1]/text()')[
                0].replace('\r\n\r\n', '').replace(
                '                                ', '')
        else:
            application = None
        detail_url = str(item.xpath('.//a/@href')[0])
        # print(detail_url)
        # print(type(detail_url))
        new_data = Data(Brand=brand,
                        Catalog_Number=catno,
                        Product_Name=name,
                        Application=application,
                        Antibody_detail_URL=detail_url)
        objects.append(new_data)

    try:
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
        print(i, 'done')
        with open('log.txt', 'a') as f:
            f.write(str(i) + '\n')
    except Exception as e:
        session.rollback()
        print(e)

    time.sleep(random.uniform(2, 2.5))
