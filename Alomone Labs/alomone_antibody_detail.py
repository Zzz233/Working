from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, create_engine
import requests
from lxml import etree
from sqlalchemy_sql import Detail, Citations, Application, Price, Images
import redis
import time
import random

# pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
#                             db=1)
# r = redis.Redis(connection_pool=pool)
#
engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Alomone():
    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, timeout=120)
            html_parsed = etree.HTML(resp.text)
            if html_parsed.xpath('.//div[@id="inner-wrapper"]'):
                html = html_parsed.xpath('.//div[@id="inner-wrapper"]')[0]
                status = 'heihei'
            if len(html) == 0:
                html = None
                status = None
        return html, status

    # ======================================================================== #
    # detail表
    def brand(self):
        return 'Alomone Labs'

    def catalog_number(self, html):
        catano = html.xpath('.//span[@class="sku"][@ite'
                            'mprop="sku"]/text()')[0].strip()
        return catano

    def product_name(self, html):
        name = html.xpath('.//h1[@id="producttitle"]/text()')[0].strip()
        return name

    def antibody_type(self, html):
        if html.xpath(
                './/span[@class="ctextlabel "][contains(text(), "Type: ")]'):
            antibody_type = html.xpath(
                './/span[@class="ctextlabel "][contains(text(), "Type: '
                '")]/following-sibling::span/text()')[0]
        else:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        if html.xpath('.//span[@class="woocommerce-Price-currencySymbol"]'):
            sellable = 'yes'
        else:
            sellable = 'no'
        return sellable

    def synonyms(self, html):
        if html.xpath('.//div[@class="ctextfield ab_altname1"]'):
            synonyms_list = html.xpath(
                './/div[@class="ctextfield ab_altname1"]/span//text()')
            synonyms = ''.join(i for i in synonyms_list)
        else:
            synonyms = None
        return synonyms

    def application(self, html):
        # (".//span[not(@class) and not(@id)]")
        if html.xpath(
                './/div[@class="ab_applications" and no'
                't(@class="ab_applications2")]'):
            application = html.xpath(
                './/div[@class="ab_applications" and not(@class="ab_applic'
                'ations2")]/span[@class="cmultivalue"]/text()')[0].strip()
        else:
            application = None
        return application

    def conjugated(self):
        return None

    def clone_number(self):
        return None

    def recombinant_antibody(self):
        return None

    def modified(self):
        return None

    def host_species(self, html):
        if html.xpath('.//div[@class="ctextfield ab_source"]'):
            host_species = html.xpath(
                './/div[@class="ctextfield ab_source'
                '"]/span[@class="ctextvalue"]/text()')[0].strip()
        else:
            host_species = None
        return host_species

    def reactivity_species(self):
        # TODO
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
        if html.xpath(
                './/div[@class="ctextfield title ab_uniprotnumber"]'):
            geneid = html.xpath(
                './/div[@class="ctextfield title ab_unipro'
                'tnumber"]/span[@class="ctextvalue "]/text()')[0].strip()
        else:
            geneid = None
        return geneid

    def ko_validation(self, html):
        if html.xpath('.//img[@alt="KO Validated"]'):
            ko_validation = 'KO Validated'
        else:
            ko_validation = None
        return ko_validation

    def species_reactivity(self, html):
        if html.xpath('.//div[@class="ab_reactivity"]'):
            species_reactivity = html.xpath(
                './/div[@class="ab_reactivity"]/span[@class="cmultivalue"]/text()')[
                0]
        else:
            species_reactivity = None
        return species_reactivity

    def immunogen(self, html):
        if html.xpath(
                './/span[@class="ctextlabel"][conta'
                'ins(text(), "Immunogen")]'):
            immunogen_list = html.xpath(
                './/span[@class="ctextlabel"][contains(text(), "Immuno'
                'gen")]/following-sibling::span[@class="ctextvalue"]//text()')
            immunogen = ''.join(i for i in immunogen_list)
        else:
            immunogen = None
        return immunogen

    def swissprot(self, html):
        if html.xpath(
                './/div[@class="ctextfield title ab_immunogen–accessionnumber"]'):
            swissprot_str = html.xpath(
                './/div[@class="ctextfield title ab_immunogen–acc'
                'essionnumber"]/span[@class="ctextvalue "]/text()')
            swissprot = ''.join(i for i in swissprot_str)
        else:
            swissprot = None
        return swissprot

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self, html):
        # <div class='ctextfield title ab_isotype'>
        if html.xpath('.//div[@class="ctextfield title ab_isotype"]'):
            isotype_str = html.xpath(
                './/div[@class="ctextfield title ab_isotype"]/span[@class="ctextvalue "]/text()')
            isotype = ''.join(i for i in isotype_str)
        else:
            isotype = None
        return isotype

    def purify(self, html):
        if html.xpath('.//div[@class="ctextfield title ab_purity"]'):
            purify_str = html.xpath(
                './/div[@class="ctextfield title ab_purity"]/span[@class="ctextvalue "]/text()')
            purify = ''.join(i for i in purify_str)
        else:
            purify = None
        return purify

    def citations(self, html):
        if html.xpath(
                './/div[@class="show-multifield multifield2 ctextfie'
                'ld ab_references "]'):
            citations = len(html.xpath(
                './/div[@class="show-multifield multifield2 ctextfield ab_refer'
                'ences "]//li[@class]'))
        else:
            citations = 0
        return citations
        # if html.xpath(
        #         './/span[@class="ctextlabel"][contains(text(), "References")]'):
        #     print(1)

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        return None

    def review(self):
        return None

    def price_url(self, html):
        if html.xpath('//script[@id="add-to-cart-country-js-extra"]'):
            price_url = html.xpath(
                '//script[@id="add-to-cart-country-js-extra"]/text()')[0]
            nonce = price_url.split('"nonce":"')[1].split('","')[0]
            product_id = price_url.split('"product_id":"')[1].split('","')[0]
            price_url = ','.join([nonce, product_id])
        else:
            price_url = None
        return price_url

    def image_qty(self, html):
        if html.xpath('.//figure[@class="line-img"]'):
            image_qty = len(html.xpath('.//figure[@class="line-img"]//img'))
        else:
            image_qty = 0
        return image_qty

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, application, html):
        results = []
        if application:
            sub_application = html.xpath(
                './/div[@class="ab_applications" and not(@class="ab_applic'
                'ations2")]/span[@class="cmultivalue"]/text()')[0].strip()
            if ',' in sub_application:
                for sub in sub_application.split(','):
                    results.append([sub.strip()])
            else:
                results.append([sub_application])
        else:
            results = []
        return results

    # ======================================================================== #
    # citations表
    def sub_citations(self, citations, html):
        if citations > 0:
            sub_citations = html.xpath(
                './/div[@class="show-multifield multifield2 ctextfield ab_refer'
                'ences "]//li[@class]')
            results = []
            for sub in sub_citations:
                pubmed_url = sub.xpath('.//a[@data-wpel-li'
                                       'nk="external"]/@href')[0]
                if 'http://www.ncbi.nlm.nih.gov/pubmed/' in pubmed_url:
                    pmid = pubmed_url.split('gov/pubmed/')[1]
                else:
                    pmid = None
                results.append([pubmed_url, pmid])
        else:
            results = []
        return results

    # ======================================================================== #
    # images表
    def sub_images(self, image_qty, html):
        if image_qty > 0:
            sub_images = html.xpath('.//figure[@class="line-img"]')
            results = []
            for sub in sub_images:
                image_url = \
                    sub.xpath('.//img[@loading="lazy"]/@src')[0]
                if sub.xpath('.//img[@loading="lazy"]'):
                    image_decription = \
                        sub.xpath('.//img[@loading="lazy"]/@alt')[0]
                else:
                    image_decription = None
                results.append([image_url, image_decription])
        else:
            results = []
        return results

    # ======================================================================== #
    # price表


def main():
    for i in range(1):
        url = 'https://www.alomone.com/p/anti-gipc1/APZ-045'
        html, status = Alomone().format(url)
        if status:
            brand = Alomone().brand()
            catalog_number = Alomone().catalog_number(html)
            product_name = Alomone().product_name(html)
            antibody_type = Alomone().antibody_type(html)
            sellable = Alomone().sellable(html)
            synonyms = Alomone().synonyms(html)
            application = Alomone().application(html)
            host_species = Alomone().host_species(html)
            antibody_detail_url = Alomone().antibody_detail_url(url)
            geneid = Alomone().geneId(html)
            swissprot = Alomone().swissprot(html)
            ko_validation = Alomone().ko_validation(html)
            species_reactivity = Alomone().species_reactivity(html)
            immunogen = Alomone().immunogen(html)
            isotype = Alomone().isotype(html)
            purify = Alomone().purify(html)
            citations = Alomone().citations(html)
            image_qty = Alomone().image_qty(html)
            sub_application = Alomone().sub_application(application, html)
            sub_citations = Alomone().sub_citations(citations, html)
            sub_images = Alomone().sub_images(citations, html)
            price_url = Alomone().price_url(html)
            print(swissprot)

            new_detail = Detail(Brand=brand,
                                Catalog_Number=catalog_number,
                                Product_Name=product_name,
                                Antibody_Type=antibody_type,
                                Sellable=sellable,
                                Synonyms=synonyms,
                                Application=application,
                                Host_Species=host_species,
                                Antibody_detail_URL=antibody_detail_url,
                                KO_Validation=ko_validation,
                                Species_Reactivity=species_reactivity,
                                SwissProt=swissprot,
                                GeneId=geneid,
                                Immunogen=immunogen,
                                Isotype=isotype,
                                Purify=purify,
                                Citations=str(citations),
                                Image_qty=image_qty,
                                Price_url=price_url)
            session.add(new_detail)

            if sub_application:
                objects_sub_application = []
                for sub in sub_application:
                    sub_application = sub[0]
                    new_application = Application(
                        Catalog_Number=catalog_number,
                        Application=sub_application,
                    )
                    objects_sub_application.append(new_application)
                session.bulk_save_objects(objects_sub_application)

            if sub_citations:
                objects_sub_citations = []
                for sub in sub_citations:
                    sub_pmid = sub[1]
                    sub_pubmed_url = sub[0]

                    new_citations = Citations(
                        Catalog_Number=catalog_number,
                        PMID=sub_pmid,
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

            try:
                session.commit()
                session.close()
                print('done')
            except Exception as e:
                session.rollback()
                print(e)
            time.sleep(random.uniform(1, 1.5))


if __name__ == '__main__':
    main()
