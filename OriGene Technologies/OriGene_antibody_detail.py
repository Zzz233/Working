"""
Conjugated
Modified
待处理
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
import redis

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=0)
r = redis.Redis(connection_pool=pool)
Base = declarative_base()


class Detail(Base):
    __tablename__ = 'origene_antibody_detail'

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
    __tablename__ = 'origene_antibody_application'

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
    __tablename__ = 'origene_antibody_citations'

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
    __tablename__ = 'origene_antibody_price'

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
    __tablename__ = 'origene_antibody_images'

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


class Origene():
    headers = {
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,i'
        #           'mage/avif,image/webp,image/apng,*/*;q=0.8,application/si'
        #           'gned-exchange;v=b3;q=0.9',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Cache-Control': 'max-age=0',
        # 'Connection': 'keep-alive',
        # 'Cookie': 'OrigeneActiveID=XFA0-NTZS-VQGO-SVDO-CSYR-XSIN-HTQI-11H6; '
        #           'org.springframework.web.servlet.i18n.CookieLocaleResolver.L'
        #           'OCALE=CN; Hm_lvt_3edf31df29cb41b05124e59e6257d775=16054935'
        #           '30; Hm_lpvt_3edf31df29cb41b05124e59e6257d775=1605497216; _u'
        #           'etsid=03a7662027b311ebbf2f1da7fe5cf816; _uetvid=03a7804027b'
        #           '311eb980f3f207f8485f9; cookieconsent_status=dismiss; JSESSI'
        #           'ONIDSITE=C33E946609865CF276A56A95A1DC6485; ActiveID=PF7P-'
        #           '5BFE-NW0R-XMJL-BAXR-TKWX-RMSW-R6F2',
        # 'Host': 'www.origene.cn',
        # 'Referer': 'https://www.origene.cn/category/antibodies/p'
        #            'rimary-antibodies?page=5',
        # 'Sec-Fetch-Dest': 'document',
        # 'Sec-Fetch-Mode': 'navigate',
        # 'Sec-Fetch-Site': 'none',
        # 'Sec-Fetch-User': '?1',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWe'
                      'bKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.1'
                      '93 Safari/537.36',
    }

    def format_html(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            # print(resp.text)
            html = etree.HTML(resp.text)
            # TODO 条件
            if html.xpath('//section[@id="product"]'):
                html_content = html.xpath('//section[@id="product"]')[0]
                status = 1
            else:
                status = 0
                html_content = None
        return status, html_content

    # ======================================================================== #
    # detail表
    def brand(self):
        return 'OriGene Technologies'

    def catalog_number(self, html):
        catano = html.xpath(
            './/h2[@class="sku"]/text()')[0].split(': ')[-1]
        return catano

    def product_name(self, html):
        name = html.xpath('.//h1[@class="name"]/text()')[0]
        return name

    def antibody_type(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Cl'
                'onality")]'):
            antibody_type = \
                html.xpath(
                    './/tr[@class="attribute"]/td[contains(text(), "Cl'
                    'onality")]/following-sibling::td/text()')[0]
        else:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        if html.xpath('.//div[@class="price h2"]'):
            sellable = 'yes'
        else:
            sellable = 'no'
        return sellable

    def synonyms(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Sy'
                'nonyms")]'):
            synonyms = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Sy'
                'nonyms")]/following-sibling::td/text()')[0].replace('; ', ',')
        else:
            synonyms = None
        return synonyms

    def application(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "App'
                'lications")]/following-sibling::td'):
            application = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "App'
                'lications")]/following-sibling::td/text()')[
                0]
        else:
            application = None
        return application

    def conjugated(self):
        # 没有
        # 没有
        return None

    #
    def clone_number(self, html):
        # 单抗有 多抗没有
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Clone N'
                'ame")]'):
            clone_number = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Clone N'
                'ame")]/following-sibling::td/text()')[0]
        else:
            clone_number = None
        return clone_number

    #
    def recombinant_antibody(self):
        # TODO
        return None

    def modified(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Modifi'
                'cations")]/following-sibling::td'):
            modified = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Modifi'
                'cations")]/following-sibling::td/text()')[0]
        else:
            modified = None
        return modified

    def host_species(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Host'
                '")]'):
            host_species = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Host'
                '")]/following-sibling::td/text()')[0]
        else:
            host_species = None
        return host_species

    def reactivity_species(self):
        # TODO 来源？
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

    def ko_validation(self, html):
        '''有KO图片写yes'''
        if html.xpath('.//li[@class="ko-validated image"]'):
            ko_validation = 'yes'
        else:
            ko_validation = 'no'

        return ko_validation

    def species_reactivity(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Reactivi'
                'ty")]/following-sibling::td'):
            species_reactivity = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Reactivi'
                'ty")]/following-sibling::td/text()')[0]
        else:
            species_reactivity = None
        return species_reactivity

    def swissprot(self):
        return None

    def immunogen(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Immuno'
                'gen")]'):
            if html.xpath(
                    './/tr[@class="attribute"]/td[contains(text(), "Immuno'
                    'gen")]/following-sibling::td/text()'):
                immunogen = html.xpath(
                    './/tr[@class="attribute"]/td[contains(text(), "Immuno'
                    'gen")]/following-sibling::td/text()')[0]
            else:
                immunogen = None
        else:
            immunogen = None
        return immunogen

    def predicted_mw(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Predicte'
                'd Protein Size")]'):
            predicted_mw = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Predicte'
                'd Protein Size")]/following-sibling::td/text()')
            if predicted_mw:
                predicted_mw = predicted_mw[0]
            else:
                predicted_mw = None
        else:
            predicted_mw = None
        return predicted_mw

    def observed_mw(self):
        return None

    def isotype(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Isot'
                'ype")]'):
            isotype = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Isot'
                'ype")]/following-sibling::td/text()')[0]
        else:
            isotype = None
        return isotype

    def purify(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Purific'
                'ation")]'):
            if html.xpath(
                    './/tr[@class="attribute"]/td[contains(text(), "Purific'
                    'ation")]/following-sibling::td/text()'):
                purify = html.xpath(
                    './/tr[@class="attribute"]/td[contains(text(), "Purific'
                    'ation")]/following-sibling::td/text()')[0]
            else:
                purify = None
        else:
            purify = None
        return purify

    def citations(self, html):
        if html.xpath(
                './/a[@href="#tab-citations"]'):
            citations = html.xpath(
                './/a[@href="#tab-citations"]/span/text()')[0]
        else:
            citations = 0
        num = int(citations)
        return num

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        dataSheet_url = html.xpath(
            './/a[@class="datasheet-link icon-link"]/@href')[0]
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        if html.xpath('.//img[@class="img-thumbnail"]'):
            image_qty = html.xpath('.//img[@class="img-thumbnail"]')
            num = len(image_qty) + 1
        else:
            num = 0
        return num

    def image_url(self):
        return None

    def Note(self):
        return '0'

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        if html.xpath(
                './/tr[@class="attribute"]/td[contains(t'
                'ext(), "Recommend Dilution")]'):
            if html.xpath(
                    './/tr[@class="attribute"]/td[contains(t'
                    'ext(), "Recommend Dilution")]/following-sibling::td/text()'):
                td = html.xpath(
                    './/tr[@class="attribute"]/td[contains(t'
                    'ext(), "Recommend Dilution")]/following-sibling::td/text()')[
                    0]
                result = []
                if ', ' in td:
                    for item in td.split(', '):
                        sub_application = item.split(' ')[0]
                        if ' ' in item:
                            sub_dilution = item.split(' ')[1]
                        else:
                            sub_dilution = None
                        result.append([sub_application, sub_dilution])
                elif ' ' in td:
                    sub_application = td.split(' ')[0]
                    sub_dilution = td.split(' ')[1]
                    result.append([sub_application, sub_dilution])
                else:
                    sub_application = td
                    sub_dilution = None
                    result.append([sub_application, sub_dilution])
            else:
                result = []
        else:
            result = []
        return result

    # ======================================================================== #
    # citations表
    def sub_citations(self, html):
        if html.xpath(
                './/table[@class="nav-table table table-stri'
                'ped table-bordered table-hover"]'):
            sub_citations = html.xpath(
                './/table[@class="nav-table table table-stri'
                'ped table-bordered table-hover"]/tbody/tr')
            result = []
            for item in sub_citations:
                article_title = item.xpath('.//span/text()')[0].strip()
                if item.xpath('.//span[contains(text(), "PubMed ID")]'):
                    pmid = item.xpath('.//span[contains(text(), "Pub'
                                      'Med ID")]/text()')[0].split(' ID ')[1]
                else:
                    pmid = None
                pubmed_url = item.xpath('.//a/@href')[0]
                if item.xpath('.//b/span[contains(text(), "[")]'):
                    species = item.xpath('.//b/span[contains(text()'
                                         ', "[")]/text()')[0].replace(
                        '[', '').replace(']', '')
                else:
                    species = None
                result.append([article_title, pmid, pubmed_url, species])
        else:
            result = []
        return result

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        if html.xpath(
                './/div[@class="product-main-image"]/a'):
            mian_image_url = html.xpath(
                './/div[@class="product-main-image"]/a/@src')[0]
            if html.xpath('.//div[@class="produ'
                          'ct-main-image"]/a/img/@title'):
                mian_decription = html.xpath('.//div[@class="produ'
                                             'ct-main-image"]/a/img/@title')[0]
            else:
                mian_decription = None
            result = []
            result.append([mian_image_url, mian_decription])

            sub_images = html.xpath('.//ul[@class="d-flex flex-wrap"]/li')
            for item in sub_images:
                image_url = item.xpath('.//a/@src')[0]
                if item.xpath('.//a/img/@title'):
                    image_decription = item.xpath('.//a/img/@title')[0]
                else:
                    image_decription = None
                result.append([image_url, image_decription])
        else:
            result = []
        return result

    # ======================================================================== #
    # price表

    def sub_price(self, html):
        orders = html.xpath(
            './/div[@class="checkout-container checkout-data-container mb-3"]')
        result = []
        for i, item in enumerate(orders):
            sub_catalog_number = None
            sub_size = item.xpath('.//option[@data-'
                                  'product-option-value]/text()')[0]
            sub_price = item.xpath('.//div[@class="price h2"]/p/text()')[0]

            result.append([sub_catalog_number, sub_size, sub_price])
        return result


def main():
    while r.exists('origene_detail'):
        extract1 = r.rpop('origene_detail')
        extract = extract1.replace('www.origene.cn//catalog/',
                                   'www.origene.cn/catalog/')
        print(extract)
        # url = 'https://www.origene.cn/catalog/antibodies/primary-antibodies/ta342379/styk1-rabbit-polyclonal-antibody'
        try:
            status, html = Origene().format_html(extract)
        except Exception as e:
            time.sleep(2)
            continue
        if html == []:
            r.lpush('origene_detail', extract1)
            continue
        try:
            brand = Origene().brand()
            catalog_number = Origene().catalog_number(html)
            product_name = Origene().product_name(html)
            antibody_type = Origene().antibody_type(html)
            sellable = Origene().sellable(html)
            synonyms = Origene().synonyms(html)
            application = Origene().application(html)
            clone_number = Origene().clone_number(html)
            host_species = Origene().host_species(html)
            antibody_detail_url = Origene().antibody_detail_url(extract)
            ko_validation = Origene().ko_validation(html)
            species_reactivity = Origene().species_reactivity(html)
            immunogen = Origene().immunogen(html)
            predicted_mw = Origene().predicted_mw(html)
            isotype = Origene().isotype(html)
            purify = Origene().purify(html)
            citations = Origene().citations(html)
            modified = Origene().modified(html)
            dataSheet_url = Origene().dataSheet_url(html)
            image_qty = Origene().image_qty(html)
            sub_application = Origene().sub_application(html)
            sub_citations = Origene().sub_citations(html)
            sub_images = Origene().sub_images(html)
            sub_price = Origene().sub_price(html)
        except Exception as e:
            print(e)
            continue
        new_detail = Detail(Brand=brand,
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Antibody_Type=antibody_type,
                            Sellable=sellable,
                            Synonyms=synonyms,
                            Application=application,
                            # Conjugated=conjugated,
                            Modified=modified,
                            Clone_Number=clone_number,
                            Host_Species=host_species,
                            Antibody_detail_URL=antibody_detail_url,
                            KO_Validation=ko_validation,
                            Species_Reactivity=species_reactivity,
                            # SwissProt=swissprot,
                            Immunogen=immunogen,
                            Predicted_MW=predicted_mw,
                            Isotype=isotype,
                            Purify=purify,
                            Citations=str(citations),
                            # Review=review,
                            DataSheet_URL=dataSheet_url,
                            Image_qty=image_qty)
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

        if sub_citations:
            objects_sub_citations = []
            for sub in sub_citations:
                sub_article_title = sub[0]
                sub_pmid = sub[1]
                sub_pubmed_url = sub[2]
                sub_species = sub[3]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pmid,
                    Application=application,
                    Species=sub_species,
                    Article_title=sub_article_title,
                    Pubmed_url=sub_pubmed_url,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)
        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                sub_image_url = sub[0]
                sub_description = sub[1]

                new_images = Images(Catalog_Number=catalog_number,
                                    Image_url=sub_image_url,
                                    Image_description=sub_description)
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                sub_catalog_number = sub[0]
                sub_size = sub[1]
                sub_price = sub[2]

                new_price = Price(Catalog_Number=catalog_number,
                                  sub_Catalog_Number=sub_catalog_number,
                                  Size=sub_size,
                                  Price=sub_price)
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)
        try:
            session.commit()
            session.close()
            print('done')
        except Exception as e:
            session.rollback()
            print(e)
        time.sleep(random.uniform(2, 2.5))


if __name__ == '__main__':
    main()
