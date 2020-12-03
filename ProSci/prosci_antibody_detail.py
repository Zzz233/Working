import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time
import random

Base = declarative_base()


class Detail(Base):
    __tablename__ = 'prosci_antibody_detail'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Antibody_Type = Column(String(40),
                           nullable=True, comment='')
    Sellable = Column(String(40),
                      nullable=True, comment='')
    Synonyms = Column(String(3000),
                      nullable=True, comment='')
    Application = Column(String(500),
                         nullable=True, comment='')
    Conjugated = Column(String(200),
                        nullable=True, comment='')
    Clone_Number = Column(String(40),
                          nullable=True, comment='')
    Recombinant_Antibody = Column(String(10),
                                  nullable=True, comment='')
    Modified = Column(String(100),
                      nullable=True, comment='')
    Host_Species = Column(String(20),
                          nullable=True, comment='')
    Reactivity_Species = Column(String(20),
                                nullable=True, comment='')
    Antibody_detail_URL = Column(String(500),
                                 nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    Price_Status = Column(String(20),
                          nullable=True, comment='')
    Citations_Status = Column(String(20),
                              nullable=True, comment='')
    GeneId = Column(String(500),
                    nullable=True, comment='')
    KO_Validation = Column(String(10),
                           nullable=True, comment='')
    Species_Reactivity = Column(String(1000),
                                nullable=True, comment='')
    SwissProt = Column(String(500),
                       nullable=True, comment='')
    Immunogen = Column(String(1000),
                       nullable=True, comment='')
    Predicted_MW = Column(String(200),
                          nullable=True, comment='')
    Observed_MW = Column(String(200),
                         nullable=True, comment='')
    Isotype = Column(String(200),
                     nullable=True, comment='')
    Purify = Column(String(200),
                    nullable=True, comment='')
    Citations = Column(String(20),
                       nullable=True, comment='')
    Citations_url = Column(String(500),
                           nullable=True, comment='')
    DataSheet_URL = Column(String(500),
                           nullable=True, comment='')
    Review = Column(String(20),
                    nullable=True, comment='')
    Price_url = Column(String(500),
                       nullable=True, comment='')
    Image_qty = Column(Integer,
                       nullable=True, comment='')
    Image_url = Column(String(500),
                       nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Application(Base):
    __tablename__ = 'prosci_antibody_application'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Application = Column(String(1000),
                         nullable=True, comment='')
    Dilution = Column(String(2000),
                      nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Citations(Base):
    __tablename__ = 'prosci_antibody_citations'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    PMID = Column(String(40),
                  nullable=True, comment='')
    Application = Column(String(300),
                         nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    Pubmed_url = Column(String(1000),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Price(Base):
    __tablename__ = 'prosci_antibody_price'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    sub_Catalog_Number = Column(String(40),
                                nullable=True, comment='')
    Size = Column(String(50),
                  nullable=True, comment='')
    Price = Column(String(50),
                   nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Images(Base):
    __tablename__ = 'prosci_antibody_images'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Image_url = Column(String(500),
                       nullable=True, comment='')
    Image_description = Column(String(1000),
                               nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


#
# engine = create_engine(
#     'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
# DBSession = sessionmaker(bind=engine)
# session = DBSession()
#
# pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
#                             db=2)
# r = redis.Redis(connection_pool=pool)
class ProSci():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5'
                      '37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                main_content = html_lxml.xpath(
                    '//div[@class="main"]')[0]
            except Exception as e:
                main_content = None
        return main_content

    def brand(self):
        return 'prosci'

    def catalog_number(self, html):
        try:
            catalog_number = html.xpath('.//span[@id="catnum"]/text()')[
                0].replace('Cat. No.: ', '').strip()
        except Exception as e:
            catalog_number = None
        return catalog_number

    def product_name(self, html):
        try:
            product_name = html.xpath('.//h1/text()')[0].strip()
        except Exception as e:
            product_name = None
        return product_name

    def antibody_type(self, html):
        try:
            antibody_type = html.xpath(
                './/td[contains(text(), "CLONALITY:")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        try:
            _ = html.xpath('.//button[@title="Add to Cart"]')
            sellable = 'yes'
        except Exception as e:
            sellable = 'no'
        return sellable

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/td[contains(text(), "ALTERNATE NAMES:")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            synonyms = None
        return synonyms

    def application(self, html):
        try:
            application = html.xpath(
                './/td[contains(text(), "TESTED APPLICATIONS:")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            application = None
        return application

    def conjugated(self, html):
        try:
            conjugated = html.xpath(
                './/td[contains(text(), "CONJUGATE:")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            conjugated = None
        return conjugated

    def clone_number(self):
        return None

    def recombinant_antibody(self):
        return None

    def modified(self, html):
        try:
            modified = html.xpath(
                './/td[contains(text(), "CLONALITY:")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            modified = None
        return modified


if __name__ == '__main__':
    for n in range(1):
        url = 'https://www.prosci-inc.com/rip3-antibody-2283.html'
        lxml = ProSci().format(url)
        catalog_number = ProSci().catalog_number(lxml)
        product_name = ProSci().product_name(lxml)
        antibody_type = ProSci().antibody_type(lxml)
        sellable = ProSci().sellable(lxml)
        synonyms = ProSci().synonyms(lxml)
        application = ProSci().application(lxml)
        conjugated = ProSci().conjugated(lxml)
        modified = ProSci().modified(lxml)
        print(modified)
