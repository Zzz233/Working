import requests
from lxml import etree
import redis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = 'bp_origene_list'

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
    'Accept': 'text/html,application/xhtml+xml,application'
              '/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q'
              '=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONIDSITE=F3C7B2694CCE42F35B02C3B3ACB5
    # A833; OrigeneActiveID=XFA0-NTZS-VQGO-SVDO-CSYR-XSIN-HTQI
    # -11H6; ActiveID=4BZF-LBYF-FX1Z-ERF2-TIPR-NE1D-BXVU-95SX; o
    # rg.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=CN',
    'Host': 'www.origene.cn',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Ap'
                  'pleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.'
                  '4240.193 Safari/537.36',
}

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=0)
r = redis.Redis(connection_pool=pool)

brand = 'OriGene Technologies'
while r.exists('test_task'):
    page = r.lpop('test_task')
    print(page, "开始")
    try:
        url = f'https://www.origene.cn/category/antibodies/primary-antibodies?page={page}'
        with requests.Session() as s:
            resp = s.get(url=url, headers=headers, timeout=120)
            # print(resp.text)
            html = etree.HTML(resp.text)
            # TODO 条件
            if html.xpath(
                    '//article[@class="container"]/div[@class="container"]'):
                html_content = html.xpath(
                    '//article[@class="container"]/div[@class="container"]')
                status = 1
            else:
                status = 0
        if status == 1:
            objects = []
            for item in html_content:
                catno = item.xpath('.//a[@class="sku"]/text()')[0]
                # print(catno)
                name = item.xpath('.//a[@class="name"]/text()')[0]
                # print(name)
                detail_url = 'https://www.origene.cn/' + \
                             item.xpath('.//a[@class="name"]/@href')[0]
                # print(url)
                new_data = Data(Brand=brand,
                                Catalog_Number=catno,
                                Product_Name=name,
                                Antibody_detail_URL=detail_url)
                objects.append(new_data)
            try:
                session.bulk_save_objects(objects)
                session.commit()
                session.close()
                print(page, 'done')
            except Exception as e:
                session.rollback()
                print(e)
        else:
            break
    except Exception as e:
        print(e)
        r.rpush('test_task', page)
        # r.close()
    finally:
        r.close()

    time.sleep(random.uniform(2, 2.5))
