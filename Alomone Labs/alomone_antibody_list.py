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
    __tablename__ = 'bp_alomone_list'

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


def hit_man(soup):
    result = []
    for item in soup:
        detail_url = item.xpath('.//../a[@rel="bookmark"]/@href')[0]
        name_list = item.xpath('.//h2[@class="title entry-title "]//text()')
        name = ''.join(n for n in name_list)
        catano = item.xpath('.//strong[contains(text(), "Cat #:")]/../text()')[
            1].strip()
        if item.xpath(
                './/span[@class="search-meta altname"]'):
            alter_list = item.xpath(
                './/span[@class="search-meta altname"]//text()')[2:]
            alter = ''.join(l for l in alter_list).strip().replace('\xa0', ' ')
        else:
            alter = None
        result.append([detail_url, name, catano, alter])
        return result


# 3页开始 13页结束
if __name__ == '__main__':
    brand = 'Alomone Labs'
    for i in range(3, 14):
        url = f'https://www.alomone.com/?s=antibody&submit=Search&paged={i}&show=all'
        resp = requests.get(url)
        html_content = etree.HTML(resp.text)
        if i == 3:
            htmls = html_content.xpath('//article[@class][position()>6]')
        elif i == 13:
            htmls = html_content.xpath('//article[@class]')[0:59]
        else:
            htmls = html_content.xpath('//article[@class]')
        objects = []
        for html in htmls:
            for single in hit_man(html):
                new_list = List(Brand=brand,
                                Catalog_Number=single[2],
                                Product_Name=single[1],
                                Antibody_detail_URL=single[0],
                                Note=single[3])
                objects.append(new_list)

        session.bulk_save_objects(objects)
        session.commit()
        session.close()
        print(i, len(objects), 'done')
