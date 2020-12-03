"""
Antigen -> Immunogen
17b-Estradiol-long linker-BSA
重要字段：
Antibody_Type
Application
Conjugated
Recombinant_Antibody
Modified
KO_Validation
Species_Reactivity
Citations
"""
from sqlalchemy.ext.declarative import declarative_base
import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class List(Base):
    __tablename__ = 'bp_abbiotec_list'

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

    def to_dict(self):
        return {
            'id': self.id,
            'Brand': self.Brand,
            'Catalog_Number': self.Catalog_Number,
            'Product_Name': self.Product_Name,
            'Application': self.Application,
            'Antibody_detail_URL': self.Antibody_detail_URL,
            'Crawl_Date': self.Crawl_Date,
            'Note': self.Note,
            'Antibody_Status': self.Antibody_Status,
            'Antibody_Type': self.Antibody_Type,
        }


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

for i in range(0, 215):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Geck'
                      'o/20100101 Firefox/82.0',
    }
    url = f'https://www.abbiotec.com/antibodies?populate=antibody&field_antib' \
          f'ody_subcategory_tid=All&field_isotype_value=All&field_species_rea' \
          f'ctivity_value=All&field_applications_value=All&field_protein_fami' \
          f'ly_tid=All&page={i}'
    objects = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        html = etree.HTML(resp.text)
        trs = html.xpath(
            '//table[@class="views-table cols-8"]/tbody/tr[@class]')
        for tr in trs:
            catano = tr.xpath(
                './/td[@class="views-field views-field-model"]/text()')[
                0].strip()
            # print(catano)
            name = tr.xpath('.//td[@class="views-field views-field-title"]'
                            '/a/text()')[0].strip()
            link = 'https://www.abbiotec.com' + \
                   tr.xpath('.//td[@class="views-fiel'
                            'd views-field-title"]/a/@href')[0].strip()
            print(name, link)
            unit = tr.xpath(
                './/td[@class="views-field views-field-field-unit"]/text()')[
                0].strip()
            price = tr.xpath('.//span[@class="uc-price"]/text()')[0]
            print(unit, price)
            new_list = List(Brand='abbiotec', Catalog_Number=catano,
                            Product_Name=name, Note=unit + ',' + price,
                            Antibody_detail_URL=link)
            objects.append(new_list)
    session.bulk_save_objects(objects)
    session.commit()
    session.close()
