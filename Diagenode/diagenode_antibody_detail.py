# https://www.diagenode.com/cn/p/h3k4me3-polyclonal-antibody-premium-50-ug-50-ul
# 图 多对一 一对一 同级 包含
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
    __tablename__ = 'diagenode_antibody_detail'

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
    __tablename__ = 'diagenode_antibody_application'

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
    __tablename__ = 'diagenode_antibody_citations'

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
    __tablename__ = 'diagenode_antibody_price'

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
    __tablename__ = 'diagenode_antibody_images'

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
                            db=3)
r = redis.Redis(connection_pool=pool)


class Diagenode():
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
                    '//div[@class="medium-9 large-9 columns"]')[0]
            except Exception as e:
                main_content = None
            try:
                bottom_content = html_lxml.xpath(
                    '//div[@class=" spaced-before row"]')[0]
            except Exception as e:
                bottom_content = None
        return main_content, bottom_content

    def brand(self):
        return 'Diagenode'

    def catalog_number(self, catano):
        # from redis
        return catano

    def product_name(self, name):
        # from redis
        return name

    def antibody_type(self, html):
        try:
            antibody_type = html.xpath(
                './/th[@class="text-left"][contains(text(), "Type")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        try:
            html.xpath(
                './/span[@class="label-primary label alert"]')
            sellable = 'yes'
        except Exception as e:
            sellable = 'no'
        return sellable

    def synonyms(self):
        return None

    def application(self, html):
        try:
            text = \
                html.xpath(
                    './/th[contains(text(), "Applications")]/../../following-sibling::tbody/tr')
            application = ', '.join(
                i.xpath('.//td[1]//text()')[0] for i in text)
        except Exception as e:
            application = None
        return application

    def conjugated(self, name):
        return name

    def clone_number(self):
        return None

    def recombinant_antibody(self):
        return None

    def modified(self):
        return None

    def host_species(self, html):
        try:
            host_species = html.xpath(
                './/th[@class="text-left"][contains(text(), "Host")]/following-sibling::td/text()')[
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
        return None

    def ko_validation(self):
        return None

    def species_reactivity(self, html):
        try:
            species_reactivity = html.xpath(
                './/th[@class="text-left"][contains(text(), "Species reactivity")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            species_reactivity = None
        return species_reactivity

    def immunogen(self):
        return None

    def swissprot(self):
        return None

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self):
        return None

    def purify(self, html):
        try:
            purify = html.xpath(
                './/th[@class="text-left"][contains(text(), "Purity")]/following-sibling::td/text()')[
                0].strip()
        except Exception as e:
            purify = None
        return purify

    def citations(self, html):
        try:
            citations = len(html.xpath(
                './/div[@id="publication"]/table/tr[position()>1]'))
        except Exception as e:
            citations = 0
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        try:
            dataSheet_url = 'https://www.diagenode.com' + html.xpath(
                './/a[contains(text(),"Datasheet")][@href]/@href')[0]
        except Exception as e:
            dataSheet_url = None
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        # TODO
        return None

    def image_qty(self, html):
        try:
            image_qty = len(html.xpath('.//div[@id="info1"]//img'))
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
                './/th[contains(text(), "Applications")]/../../following-sibling::tbody/tr')
            for i in text:
                sub_app = i.xpath('.//td[1]//text()')[0]
                sub_dil = i.xpath('.//td[2]//text()')[0]
                results.append([sub_app, sub_dil])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # price表
    def sub_price(self, html):
        results = []
        try:
            sub_pri = html.xpath(
                './/span[@class="label-primary label alert"]/text()')[0].strip()
            sub_siz = html.xpath(
                './/span[@class="label-primary label alert"]/../../div[@class="small-4 medium-4 large-4 columns text-center"]/text()')[
                0].strip()
            results.append([sub_siz, sub_pri])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # citations表
    def sub_citations(self, html):
        results = []
        try:
            cites = html.xpath(
                './/div[@id="publication"]/table/tr[position()>1]')
            for i in cites:
                title = i.xpath('.//td/p/strong/a/text()')[0].strip()
                if 'pubmed.gov/' in i.xpath('.//td/p/a/@href')[0].split('/'):
                    pm_id = i.xpath('.//td/p/a/@href')[0].split('/')[-1].strip()
                    pm_url = 'https://pubmed.ncbi.nlm.nih.gov/' + pm_id
                else:
                    pm_id = None
                    pm_url = i.xpath('.//td/p/a/@href')[0]
                results.append([pm_id, title, pm_url])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        results = []
        try:
            rows = html.xpath(
                './/div[@id="info1"][@class="content active"]/div[@class="row"]')
        except Exception as e:
            return results
        # print(rows)
        for row in rows:
            try:
                div_img = row.xpath(
                    './/div[@class][1]//img[@alt]/@src')
                div_des = row.xpath(
                    './/div[@class][2]//small/text()')

                if div_img:
                    # print(div_img, div_des)
                    img_url = ','.join(i for i in div_img)
                    img_des = '\n\n'.join(i for i in div_des)
                    results.append([img_url, img_des])
            except Exception as e:
                pass
        return results


# MolecularWeight(Da)： 预测 Observed Band(KD)：观察
if __name__ == '__main__':
    while r.exists('diagenode_detail'):
        extract = r.lpop('diagenode_detail')
        extract_url = extract.split('|||')[2]
        extract_catano = extract.split('|||')[0]
        extract_name = extract.split('|||')[1]
        print(extract)
        try:
            main_lxml, bottom_lxml = Diagenode().format(extract_url)
        except Exception as e:
            r.lpush('diagenode_detail', extract)
            print(e)
            continue
        if main_lxml and bottom_lxml is not None:
            brand = Diagenode().brand()
            catalog_number = Diagenode().catalog_number(extract_catano)
            product_name = Diagenode().product_name(extract_name)
            antibody_type = Diagenode().antibody_type(main_lxml)
            sellable = Diagenode().sellable(main_lxml)
            application = Diagenode().application(main_lxml)
            host_species = Diagenode().host_species(main_lxml)
            antibody_detail_url = Diagenode().antibody_detail_url(extract_url)
            species_reactivity = Diagenode().species_reactivity(main_lxml)
            purify = Diagenode().purify(main_lxml)
            citations = Diagenode().citations(bottom_lxml)
            dataSheet_url = Diagenode().dataSheet_url(bottom_lxml)
            image_qty = Diagenode().image_qty(bottom_lxml)
            sub_application = Diagenode().sub_application(main_lxml)
            sub_price = Diagenode().sub_price(main_lxml)
            sub_citations = Diagenode().sub_citations(bottom_lxml)
            sub_images = Diagenode().sub_images(bottom_lxml)
            print(sub_images)

        else:
            print('html is None')
            r.lpush('immunoway_detail', extract)
            continue

        new_detial = Detail(Brand=brand,
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Antibody_Type=antibody_type,
                            Sellable=sellable,
                            # Synonyms=synonyms,
                            Application=application,
                            # Conjugated=conjugated,
                            # Clone_Number=clone_number,
                            # Recombinant_Antibody=recombinant_antibody,
                            # Modified=modified,
                            Host_Species=host_species,
                            # Reactivity_Species=reactivity_species,
                            Antibody_detail_URL=antibody_detail_url,
                            # GeneId=geneId,
                            # KO_Validation=ko_validation,
                            Species_Reactivity=species_reactivity,
                            # SwissProt=swissprot,
                            # Immunogen=immunogen,
                            # Predicted_MW=predicted_mw,
                            # Observed_MW=observed_mw,
                            # Isotype=isotype,
                            Purify=purify,
                            Citations=str(citations),
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

        if sub_citations:
            objects_sub_citations = []
            for sub in sub_citations:
                # sub_price, sub_size
                sub_pmid = sub[0]
                sub_title = sub[1]
                sub_url = sub[2]
                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pmid,
                    Article_title=sub_title,
                    Pubmed_url=sub_url
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)
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
            r.lpush('diagenode_detail', extract)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
