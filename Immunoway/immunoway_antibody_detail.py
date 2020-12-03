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
    __tablename__ = 'immunoway_antibody_detail'

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
    __tablename__ = 'immunoway_antibody_application'

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
    __tablename__ = 'immunoway_antibody_citations'

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
    __tablename__ = 'immunoway_antibody_price'

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
    __tablename__ = 'immunoway_antibody_images'

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


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=2)
r = redis.Redis(connection_pool=pool)
"""
Human Gene Id
Human Swiss Prot No

只取human
"""


class Immunoway():
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
                    '//div[@class="main dMain"]')[0]
                price_content = html_lxml.xpath('//div[@class="divAttr"]')[0]
            except Exception as e:
                main_content = None
                price_content = None
        return main_content, price_content

    def brand(self):
        return 'Immunoway'

    def catalog_number(self, url):
        catano = url.split('Home/22/')[-1]
        return catano

    def product_name(self, html):
        name = \
            html.xpath(
                './/div[@class="attrPrdt" or @class="attrPrdt2"]/p/text()')[
                0].strip()
        return name

    def antibody_type(self, name):
        if 'Polyclonal' in name:
            antibody_type = 'Polyclonal'
        elif 'Monoclona' in name:
            antibody_type = 'Monoclonal'
        elif 'mAb' in name:
            antibody_type = 'Monoclonal'
        elif 'pAb' in name:
            antibody_type = 'Polyclonal'
        else:
            antibody_type = None
        return antibody_type

    def sellable(self, sidebar_html):
        try:
            sidebar_html.xpath(
                './/li[@class="title"][contains(text(), "Price$：")]')
            sellable = 'yes'
        except Exception as e:
            sellable = 'no'
        return sellable

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/li[@class="title"][contains(text(), "Other Name：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            synonyms = None
        return synonyms

    def application(self, html):
        try:
            application = \
                html.xpath(
                    './/span[contains(text(), "Applications：")]/../text()')[
                    0].strip()
        except Exception as e:
            application = None
        return application

    def conjugated(self, name):
        return name

    def clone_number(self):
        return None

    def recombinant_antibody(self):
        return None

    def modified(self, name):
        if '(phospho ' in name:
            modified = 'phospho'
        elif '(Acetyl-' in name:
            modified = 'acetyl'
        elif '(Acetyl ' in name:
            modified = 'acetyl'
        else:
            modified = None
        return modified

    def host_species(self, html):
        # Source：
        try:
            host_species = html.xpath(
                './/li[@class="title"][contains(text(), "Source：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            host_species = None
        return host_species

    def reactivity_species(self):
        return None

    def antibody_detail_url(self, url):
        return url

    def antibody_status(self):
        return None

    def price_status(self):
        return None

    def citations_status(self):
        return None

    def geneId(self, html):
        # Human Gene Id：
        try:
            geneId = html.xpath(
                './/li[@class="title"][contains(text(), "Human Gene Id：")]/following-sibling::li[@class="text"]/a[@onclick]/text()')[
                0].strip()
        except Exception as e:
            geneId = None
        return geneId

    def ko_validation(self):
        return None

    def species_reactivity(self, html):
        try:
            species_reactivity = \
                html.xpath(
                    './/span[contains(text(), "Reactivity：")]/../text()')[0]
        except Exception as e:
            species_reactivity = None
        return species_reactivity

    def immunogen(self, html):
        try:
            immunogen = html.xpath(
                './/li[@class="title"][contains(text(), "Immunogen：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            immunogen = None
        return immunogen

    def swissprot(self, html):
        try:
            swissprot = html.xpath(
                './/li[@class="title"][contains(text(), "Human Swiss Prot No：")]/following-sibling::li[@class="text"]/a/text()')[
                0].strip()
        except Exception as e:
            swissprot = None
        return swissprot

    def predicted_mw(self, html):
        try:
            predicted_mw = html.xpath(
                './/li[@class="title"][contains(text(), "MolecularWeight(Da)：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            predicted_mw = None
        return predicted_mw

    def observed_mw(self, html):
        try:
            observed_mw = html.xpath(
                './/li[@class="title"][contains(text(), "Observed Band(KD)：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            observed_mw = None
        return observed_mw

    def isotype(self):
        return None

    def purify(self, html):
        try:
            purify = html.xpath(
                './/li[@class="title"][contains(text(), "Purification：")]/following-sibling::li[@class="text"]/text()')[
                0].strip()
        except Exception as e:
            purify = None
        return purify

    def citations(self, html):
        try:
            citations = html.xpath(
                './/a[@style and @href][contains(text(),"References (")]/text()')[
                0].split('( ')[1].split(' )')[0]
        except Exception as e:
            citations = '0'
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        try:
            dataSheet_url = 'http://www.immunoway.com' + html.xpath(
                './/a[contains(text(),"Data Sheet")][@href]/@href')[0]
        except Exception as e:
            dataSheet_url = None
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        try:
            image_qty = len(html.xpath('.//li[@class="img"]'))
        except Exception as e:
            image_qty = 0
        return image_qty

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        results = []
        try:
            text = html.xpath(
                './/li[@class="title"][contains(text(), "Dilution：")]/following-sibling::li[@class="text"]/text()')[
                0].strip().replace(' Not yet tested in other applications.', '')
            for i in text.split('.'):
                if len(i) > 0:
                    sub_app = i.split(': ')[0]
                    sub_dil = i.split(': ')[1]
                    results.append([sub_app, sub_dil])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # price表
    def sub_price(self, sidebar_html):
        results = []
        try:
            size_list = sidebar_html.xpath(
                './/li[@class="title"][contains(text(), "Size")]/following-sibling::li[@class="text"]/text()')[
                0]

            if ' ' in size_list:
                # 多规格
                for i, size in enumerate(size_list.split(' ')):
                    price = sidebar_html.xpath(
                        './/li[@class="title"][contains(text(), "Price")]/following-sibling::li[@class="text"]/text()')[
                        0].split(' ')[i]
                    results.append([size, price])
            else:
                # TODO 没空格 单一规格
                size = sidebar_html.xpath(
                    './/li[@class="title"][contains(text(), "Size")]/following-sibling::li[@class="text"]/text()')[
                    0].strip()
                price = sidebar_html.xpath(
                    './/li[@class="title"][contains(text(), "Price")]/following-sibling::li[@class="text"]/text()')[
                    0].strip()
                results.append([size, price])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # citations表
    def sub_citations(self):
        return None

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        results = []
        try:
            for i in html.xpath('.//ul[@class="imgAttr"]'):
                img_url = 'http://www.immunoway.com' + \
                          i.xpath('.//li[@class="img"]/img/@src')[0]
                img_description = i.xpath('.//li[@class="attr"]/text()')[
                    0].strip()
                results.append([img_url, img_description])
        except Exception as e:
            results = []
        return results


# MolecularWeight(Da)： 预测 Observed Band(KD)：观察
if __name__ == '__main__':
    while r.exists('immunoway_detail'):
        extract = r.rpop('immunoway_detail')
        print(extract)
        try:
            main_lxml, price_lxml = Immunoway().format(extract)
        except Exception as e:
            print(e)
            continue
        if len(main_lxml) and len(price_lxml) > 0:
            brand = Immunoway().brand()
            catalog_number = Immunoway().catalog_number(extract)
            product_name = Immunoway().product_name(main_lxml)
            antibody_type = Immunoway().antibody_type(product_name)
            sellable = Immunoway().sellable(price_lxml)
            synonyms = Immunoway().synonyms(main_lxml)
            application = Immunoway().application(main_lxml)
            host_species = Immunoway().host_species(main_lxml)
            antibody_detail_url = Immunoway().antibody_detail_url(extract)
            geneId = Immunoway().geneId(main_lxml)
            species_reactivity = Immunoway().species_reactivity(main_lxml)
            immunogen = Immunoway().immunogen(main_lxml)
            swissprot = Immunoway().swissprot(main_lxml)
            predicted_mw = Immunoway().predicted_mw(main_lxml)
            observed_mw = Immunoway().observed_mw(main_lxml)
            purify = Immunoway().purify(main_lxml)
            dataSheet_url = Immunoway().dataSheet_url(main_lxml)
            conjugated = Immunoway().conjugated(product_name)
            image_qty = Immunoway().image_qty(main_lxml)
            modified = Immunoway().modified(product_name)
            citations = Immunoway().citations(main_lxml)
            sub_application = Immunoway().sub_application(main_lxml)
            sub_price = Immunoway().sub_price(price_lxml)
            sub_images = Immunoway().sub_images(main_lxml)

            # print(sub_images)
        else:
            print('html is None')
            r.lpush('immunoway_detail', extract)
            continue

        new_detial = Detail(Brand=brand,
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Antibody_Type=antibody_type,
                            Sellable=sellable,
                            Synonyms=synonyms,
                            Application=application,
                            Conjugated=conjugated,
                            # Clone_Number=clone_number,
                            # Recombinant_Antibody=recombinant_antibody,
                            Modified=modified,
                            Host_Species=host_species,
                            # Reactivity_Species=reactivity_species,
                            Antibody_detail_URL=antibody_detail_url,
                            GeneId=geneId,
                            # KO_Validation=ko_validation,
                            Species_Reactivity=species_reactivity,
                            SwissProt=swissprot,
                            Immunogen=immunogen,
                            Predicted_MW=predicted_mw,
                            Observed_MW=observed_mw,
                            # Isotype=isotype,
                            Purify=purify,
                            Citations=citations,
                            DataSheet_URL=dataSheet_url,
                            # Review=review,
                            Image_qty=image_qty,
                            )
        session.add(new_detial)

        if sub_application:
            objects_sub_application = []
            for sub in sub_application:
                sub_application = sub[0]
                sub_dilution = sub[1]
                new_application = Application(
                    Catalog_Number=catalog_number,
                    Application=sub_application,
                    Dilution=sub_dilution
                )
                objects_sub_application.append(new_application)
            session.bulk_save_objects(objects_sub_application)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                # sub_price, sub_size
                sub_price = sub[1]
                sub_size = sub[0]
                new_price = Price(
                    Catalog_Number=catalog_number,
                    Price=sub_price,
                    Size=sub_size
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                # sub_price, sub_size
                sub_img_url = sub[0]
                sub_img_des = sub[1]
                new_img = Images(
                    Catalog_Number=catalog_number,
                    Image_url=sub_img_url,
                    Image_description=sub_img_des
                )
                objects_sub_images.append(new_img)
            session.bulk_save_objects(objects_sub_images)
        try:
            session.commit()
            session.close()
            print('done')
        except Exception as e:
            r.lpush('immunoway_detail', extract)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
