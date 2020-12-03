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
    __tablename__ = 'abbiotec_antibody_detail'

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
    __tablename__ = 'abbiotec_antibody_application'

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
    __tablename__ = 'abbiotec_antibody_citations'

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
    __tablename__ = 'abbiotec_antibody_price'

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
    __tablename__ = 'abbiotec_antibody_images'

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
                            db=1)
r = redis.Redis(connection_pool=pool)


class Abbiotec():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Geck'
                      'o/20100101 Firefox/82.0',
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                html_content = html_lxml.xpath(
                    './/div[@class="ds-2col-stacked node node-an'
                    'tibodies view-mode-full clearfix"]')[0]
            except Exception as e:
                html_content = None
        return html_content

    def brand(self):
        return 'Abbiotec'

    def catalog_number(self, web):
        catano = web.xpath(
            './/div[@class="field field-name-product-number f'
            'ield-type-ds field-label-above"]/div/div[@class="fiel'
            'd-item even"]/text()')[0].strip()
        return catano

    def product_name(self, web):
        name = web.xpath('.//div[@property="dc:title"]/h1/text()')[0].strip()
        return name

    def antibody_type(self, web):
        subcategory = web.xpath(
            './/div[@class="field field-name-field-anti'
            'body-subcategory field-type-taxonomy-term-r'
            'eference field-label-hidden"]/div[@class="f'
            'ield-items"]/div[@class]/text()')
        antibody_type = ''.join(i for i in subcategory)
        if 'Polyclonal' in antibody_type:
            return 'Polyclonal'
        elif 'Monoclonal' in antibody_type:
            return 'Monoclonal'

    def sellable(self):
        sellable = 'yes'
        return sellable

    def synonyms(self, web):
        try:
            synonyms = web.xpath(
                './/div[@class="field field-name-field-alternate-names fiel'
                'd-type-text field-label-above"]/div/div[@class="field-item '
                'even"]/text()')[0].strip()
        except Exception as e:
            synonyms = None
        return synonyms

    def application(self, web):
        # (".//span[not(@class) and not(@id)]")
        try:
            application_list = web.xpath(
                './/div[@class="field field-name-field-applicati'
                'ons field-type-list-text field-label-above"]/div'
                '[@class="field-items"]/div[@class]/text()')
            application = ','.join(i for i in application_list)
        except Exception as e:
            application = None
        return application

    def conjugated(self, web):
        # modify 同
        subcategory = web.xpath(
            './/div[@class="field field-name-field-anti'
            'body-subcategory field-type-taxonomy-term-r'
            'eference field-label-hidden"]/div[@class="f'
            'ield-items"]/div[@class]/text()')
        conjugated = ''.join(i for i in subcategory)
        return conjugated

    def clone_number(self, web):
        try:
            clone_number = web.xpath(
                './/div[@class="field field-name-field-clonality field-type-text field-label-above"]/div[@class="field-items"]/div[@class]/text()')[
                0].strip()
        except Exception as e:
            clone_number = None
        return clone_number

    def recombinant_antibody(self):
        return None

    def modified(self, conjugated):
        return conjugated

    def host_species(self, web):
        try:
            subcategory = web.xpath(
                './/div[@class="field field-name-field-antibody-subcategory field-type-taxonomy-term-reference field-label-hidden"]/div[@class="field-items"]/div[@class]/text()')
            for i in subcategory:
                if 'Monoclonal Antibody' in i:
                    host = i.split('Monoclonal Antibody')[0].strip()
                elif 'Polyclonal Antibody' in i:
                    host = i.split('Polyclonal Antibody')[0].strip()
        except Exception as e:
            host = None
        return host

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

    def geneId(self):
        return None

    def ko_validation(self):
        return None

    def species_reactivity(self, web):
        try:
            field_item = web.xpath(
                './/div[@class="field field-name-field-species-reactivity field-type-list-text field-label-above"]/div[@class="field-items"]/div[@class]/text()')
            species_reactivity = ', '.join(n for n in field_item)
        except Exception as e:
            species_reactivity = None
        return species_reactivity

    def immunogen(self, web):
        # antigen
        try:
            immunogen = web.xpath(
                './/div[@class="field field-name-field-antigen field-type-text field-label-above"]/div/div[@class]/text()')[
                0].strip()
        except Exception as e:
            immunogen = None
        return immunogen

    def swissprot(self, web):
        # 多个
        try:
            field_item = web.xpath(
                './/div[@class="field field-name-accession-no-formatted field-type-ds field-label-above"]/div[@class="field-items"]/div[@class]/a/text()')
            swissprot = ', '.join(i for i in field_item)
        except Exception as e:
            swissprot = None
        return swissprot

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self, web):
        try:
            isotype = web.xpath(
                './/div[@class="field field-name-field-isotype field-type-list-text field-label-above"]/div[@class="field-items"]/div[@class]/text()')[
                0].strip()
        except Exception as e:
            isotype = None
        return isotype

    def purify(self):
        return None

    def citations(self):
        return None

    def citations_url(self):
        return None

    def dataSheet_url(self):
        return None

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self):
        return None

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, web):
        try:
            field_items = web.xpath(
                './/div[@class="field field-name-field-application-notes field-type-text field-label-above"]/div[@class="field-items"]/div[@class]/text()')[
                0].strip()
        except Exception as e:
            return None
        results = []
        application_list = web.xpath(
            './/div[@class="field field-name-field-applicati'
            'ons field-type-list-text field-label-above"]/div'
            '[@class="field-items"]/div[@class]/text()')
        for application in application_list:
            if application in field_items:
                try:
                    sub_application = application
                    sub_dilution = \
                        field_items.split(application + ': ')[1].split('; ')[0]
                    results.append([sub_application, sub_dilution])
                except Exception as e:
                    sub_application = application
                    sub_dilution = field_items.split(application + ':')[-1]
                    results.append([sub_application, sub_dilution])
            else:
                results = []
        return results

    # ======================================================================== #
    # price表
    def sub_price(self, text):
        # from redis
        # https://www.abbiotec.com/antibodies/zyxin-antibody|||0.1 mg,$330.00
        results = []
        sub_price = text.split(',')[1]
        sub_size = text.split(',')[0]
        results.append([sub_price, sub_size])
        return results


if __name__ == '__main__':

    while r.exists('abbiotec_detail'):
        extract = r.rpop('abbiotec_detail')
        url = extract.split('|||')[0]
        price_text = extract.split('|||')[1]
        print(url, price_text)
        # for i in range(1):
        try:
            html = Abbiotec().format(url)
        except Exception as e:
            print(1, e)
            r.lpush('abbiotec_detail', extract)
            continue
        if html is not None:
            catalog_number = Abbiotec().catalog_number(html)
            product_name = Abbiotec().product_name(html)
            antibody_type = Abbiotec().antibody_type(html)
            sellable = Abbiotec().sellable()
            synonyms = Abbiotec().synonyms(html)
            application = Abbiotec().application(html)
            conjugated = Abbiotec().conjugated(html)
            clone_number = Abbiotec().clone_number(html)
            modified = Abbiotec().modified(conjugated)
            host_species = Abbiotec().host_species(html)
            antibody_detail_url = Abbiotec().antibody_detail_url(url)
            species_reactivity = Abbiotec().species_reactivity(html)
            immunogen = Abbiotec().immunogen(html)
            swissprot = Abbiotec().swissprot(html)
            isotype = Abbiotec().isotype(html)
            sub_application = Abbiotec().sub_application(html)
            sub_price = Abbiotec().sub_price(price_text)


        else:
            print('html is None')
            r.lpush('abbiotec_detail', extract)
            continue

        new_detail = Detail(Brand='abbiotec',
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Antibody_Type=antibody_type,
                            Sellable=sellable,
                            Synonyms=synonyms,
                            Application=application,
                            Conjugated=conjugated,
                            Clone_Number=clone_number,
                            Host_Species=host_species,
                            Modified=modified,
                            Antibody_detail_URL=antibody_detail_url,
                            # KO_Validation=ko_validation,
                            Species_Reactivity=species_reactivity,
                            SwissProt=swissprot,
                            # GeneId=geneid,
                            Immunogen=immunogen,
                            Isotype=isotype)
        session.add(new_detail)

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
                sub_price = sub[0]
                sub_size = sub[1]
                new_price = Price(
                    Catalog_Number=catalog_number,
                    Price=sub_price,
                    Size=sub_size
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)
        try:
            session.commit()
            session.close()
            print('done')
        except Exception as e:
            r.lpush('abbiotec_detail', extract)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
