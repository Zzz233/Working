import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time

Base = declarative_base()


class List(Base):
    __tablename__ = 'bp_enzo_list'

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

for i in range(1, 19):
    url = f'https://www.enzolifesciences.com/navigation/cHash/b598' \
          f'b3006f89251bb0a6674bf9172942/type/1000/facepath/refSEA' \
          f'RCH%2C24540/navlevel/2/count/2682/page/{i}/show/150/newp' \
          f'roducts/0/selectedonly/0/navcountry/46/?navipar%5BrefS' \
          f'EARCH%5D=antibody&cs2_actual_pid=69&code=productlist&&' \
          f'cs2_actual_pid=69'
    headers = {
        'Accept': 'text/javascript, application/javascript, applicatio'
                  'n/ecmascript, application/x-ecmascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en'
                           '-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'www.enzolifesciences.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.enzolifesciences.com/product-listing/',
        'TE': 'Trailers',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Ge'
                      'cko/20100101 Firefox/82.0',
        'X-Requested-With': 'XMLHttpRequest',

    }
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        # print(resp.text)
        html = etree.HTML(resp.text)
        h4 = html.xpath('//h4')
        result = []
        for item in h4:
            link = item.xpath('.//a/@href')[0].replace('\\"', '')
            name = item.xpath('.//a/text()')[0]
            print(link)
            print(name)
            new_list = List(Brand='Enzo Life Sciences',
                            Antibody_detail_URL=link,
                            Product_Name=name)
            result.append(new_list)

        session.bulk_save_objects(result)

        session.commit()
        session.close()
        print('done')
