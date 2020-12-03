""":cvar
Target Antigen:

immunogen
"""
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
    __tablename__ = 'bp_immuQuest_list'

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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; '
                  'x64; rv:82.0) Gecko/20100101 Firefox/82.0',
}
urls = ['https://immuquest.com/collections/cell-adhesion-marker-antibodies',
        'https://immuquest.com/collections/cell-cycle-antibodies',
        'https://immuquest.com/collections/cell-signalling-antibodies',
        'https://immuquest.com/collections/checkpoint-control-antibodies',
        'https://immuquest.com/collections/chemokine-receptor-antibodies',
        'https://immuquest.com/collections/cytoskeleton-antibodies',
        'https://immuquest.com/collections/disease-related-antibodies',
        'https://immuquest.com/collections/dna-damage-recognition-antibodies',
        'https://immuquest.com/collections/growth-factor-antibodies',
        'https://immuquest.com/collections/hormone-receptor-antibodies',
        'https://immuquest.com/collections/immunology-antibodies',
        'https://immuquest.com/collections/metabolism-antibodies',
        'https://immuquest.com/collections/microbiology-antibodies',
        'https://immuquest.com/collections/neuroscience-antibodies',
        'https://immuquest.com/collections/nuclear-transport-antibodies',
        'https://immuquest.com/collections/nuclear-pore-complex-antibodies',
        'https://immuquest.com/collections/nuclear-transport-antibodies',
        'https://immuquest.com/collections/phototransduction-antibodies',
        'https://immuquest.com/collections/rna-binding-protein-antibodies',
        'https://immuquest.com/collections/tag-antibodies',
        'https://immuquest.com/collections/t-cell-receptor-antibodies',
        'https://immuquest.com/collections/viral-transcription-antibodies', ]
for i in urls:
    objects = []
    with requests.Session() as s:
        resp = s.get(url=i, headers=headers)
        html_content = etree.HTML(resp.text)
        lxml = html_content.xpath(
            '//div[@class="products__single--details"]')
        for item in lxml:
            name = \
                item.xpath(
                    './/h2/text()')[
                    0].strip()

            link = 'https://immuquest.com' + \
                   item.xpath(
                       './/a/@href')[
                       0].strip()
            catano = \
                item.xpath(
                    './/td[contains(text(), "Product Code:")]/following-sibling::td/text()')[
                    0].strip()
            print(name, catano, link)
            new_data = Data(Brand='immuquest',
                            Product_Name=name,
                            Antibody_detail_URL=link,
                            Catalog_Number=catano)
            objects.append(new_data)
        session.bulk_save_objects(objects)
        session.commit()
        session.close()
    time.sleep(1)
