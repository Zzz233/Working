from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, create_engine
import requests
from lxml import etree
from sqlalchemy_sql import Detail, Citations, Application, Price, Images
import time
import random
import redis

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=1)
r = redis.Redis(connection_pool=pool)

engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Enzo():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,appl'
                  'ication/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-H'
                           'K;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'www.enzolifesciences.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.enzolifesciences.com/product-listing/',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; r'
                      'v:82.0) Gecko/20100101 Firefox/82.0',

    }

    def format_html(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, timeout=120)
            html_parsed = etree.HTML(resp.content)
            # TODO 条件
            if html_parsed.xpath('//div[@class="tx-cs2smdnavi-pi3"]'):
                html_content = html_parsed.xpath(
                    '//div[@class="tx-cs2smdnavi-pi3"]')[0]
                for bad in html_content.xpath(
                        './/div[@class="tx-cs2smdnavi-pi5"]'):
                    bad.getparent().remove(bad)

                status = 1
            else:
                status = 0
                html_content = None
        return status, html_content

    # ======================================================================== #
    # detail表
    def brand(self):
        return 'Enzo Life Sciences'

    def catalog_number(self, url):
        catano = url.split('enzolifesciences.com/')[1].split('/')[0]
        # print(catano)
        return catano

    def product_name(self, html):
        name = html.xpath('.//h1//text()')
        result = ''.join(i for i in name)
        return result

    def antibody_type(self, html):
        if html.xpath('.//b[contains(text(), "Clone:")]'):
            antibody_type = "Monoclonal"
        else:
            antibody_type = "Polyclonal"
        return antibody_type

    def sellable(self):
        return None

    def synonyms(self, html):
        if html.xpath('.//b[contains(text(), "Alternative Name:")]'):
            synonyms = html.xpath('.//b[contains(text(), "Alternative Name:'
                                  '")]/../following-sibling::td/text()')[0]
        else:
            synonyms = None
        return synonyms

    def application(self, html):
        if html.xpath(
                './/b[contains(text(), "Applications:")]'):
            application = html.xpath(
                './/b[contains(text(), "Applications:")]/../following-sibling::td/text()')[
                0]
        else:
            application = None
        return application

    def conjugated(self, product_name):
        if 'Biotin' in product_name:
            conjugated = 'Biotin'
        elif 'FITC' in product_name:
            conjugated = 'FITC'
        elif 'PE' in product_name:
            conjugated = 'PE'
        elif 'HRP' in product_name:
            conjugated = 'HRP'
        elif 'DyLight™ 488' in product_name:
            conjugated = 'DyLight™ 488'
        elif 'Alexa Fluor® 647' in product_name:
            conjugated = 'Alexa Fluor® 647'
        elif 'Alexa Fluor® 546' in product_name:
            conjugated = 'Alexa Fluor® 546'
        elif 'Alexa Fluor® 488' in product_name:
            conjugated = 'Alexa Fluor® 488'
        elif 'R-PE' in product_name:
            conjugated = 'R-PE'
        elif 'Alkaline Phosphatase' in product_name:
            conjugated = 'Alkaline Phosphatase'
        elif 'DY-682' in product_name:
            conjugated = 'DY-682'
        elif 'Fluorescein' in product_name:
            conjugated = 'Fluorescein'
        elif 'DY-800' in product_name:
            conjugated = 'DY-800'
        elif 'ATTO 590' in product_name:
            conjugated = 'ATTO 590'
        else:
            conjugated = None
        return conjugated

    def clone_number(self, html):
        if html.xpath('.//b[contains(text(), "Clone:")]'):
            clone_number = html.xpath(
                './/b[contains(text(), "Clone:")]/../following::td/text()')[0]
        else:
            clone_number = None
        return clone_number

    def recombinant_antibody(self, product_name):
        if 'Recombinant' in product_name:
            recombinant_antibody = 'yes'
        else:
            recombinant_antibody = None
        return recombinant_antibody

    def modified(self):
        return None

    def host_species(self, html):
        if html.xpath('.//b[contains(text(), "Host:")]'):
            host_species = html.xpath('.//b[contains(text(), "Host:'
                                      '")]/../following-sibling::td/text()')[0]
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

    def geneId(self):
        return None

    def ko_validation(self, html):
        return None

    def species_reactivity(self, html):
        if html.xpath('.//b[contains(text(), "Species reactivity:")]'):
            species_reactivity = html.xpath('.//b[contains(text(), "Species rea'
                                            'ctivity:")]/../following-siblin'
                                            'g::td/text()')[0]
        else:
            species_reactivity = None
        return species_reactivity

    def immunogen(self, html):
        if html.xpath('.//b[contains(text(), "Immunogen:")]'):
            immunogen = html.xpath('.//b[contains(text(), "Immunogen:'
                                   '")]/../following-sibling::td/text()')[0]
        else:
            immunogen = None
        return immunogen

    def swissprot(self, html):
        if html.xpath('.//b[contains(text(), "UniProt ID:")]'):
            swissprot = html.xpath('.//b[contains(text(), "UniProt I'
                                   'D:")]/../following-sibling::td/text()')[0]
        else:
            swissprot = None
        return swissprot

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self, html):
        if html.xpath('.//b[contains(text(), "Isotype:")]'):
            isotype = html.xpath('.//b[contains(text(), "Isotype:'
                                 '")]/../following-sibling::td/text()')[0]
        else:
            isotype = None
        return isotype

    def purify(self, html):
        if html.xpath('.//b[contains(text(), "Purity Detail:")]'):
            purify = html.xpath('.//b[contains(text(), "Purity Detail:'
                                '")]/../following-sibling::td/text()')[0]
        else:
            purify = None
        return purify

    def citations(self, html):
        if html.xpath('.//div[@class="litreference"]'):
            citations = len(html.xpath('.//div[@class="litreference"]'))
        else:
            citations = 0
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        if html.xpath('.//a[contains(text(), "Datasheet")]'):
            dataSheet_url = html.xpath('.//a[contains(text(), '
                                       '"Datasheet")]/@href')[0]
        else:
            dataSheet_url = None
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        if html.xpath('.//div[@class="prodthumbs"]'):
            image_qty = len(html.xpath('.//div[@class="prodthumbs"]/img'))
        else:
            image_qty = 0
        return image_qty

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        if html.xpath(
                './/b[contains(text(), "Recommended Dilutions/Conditions:")]'):
            sub_application = html.xpath(
                './/b[contains(text(), "Recommended Dilutions/Conditions:")]/../following-sibling::td/text()')
            result = []
            for sub in sub_application:
                if '(' in sub:
                    # sub_application = sub.split('(')[0]
                    sub_dilution = sub.split('(')[-1].strip(')')
                    sub_application = sub.split(sub_dilution)[0].strip('( ')
                    result.append([sub_application, sub_dilution])
        else:
            result = []
        return result

    # ======================================================================== #
    # citations表
    def sub_citations(self, html):
        if html.xpath('.//div[@class="litreference"]'):
            sub_citations = html.xpath('.//div[@class="litreference"]')
            result = []
            for sub in sub_citations:
                article_title = sub.xpath('.//i/text()')[0]
                if sub.xpath('a[contains(text(), "Abstract")]'):
                    pubmed_url = sub.xpath(
                        'a[contains(text(), "Abstract")]/@href')[0]
                    if 'list_uids=' in pubmed_url:
                        pmid = pubmed_url.split('list_uids=')[-1]
                    elif '?itool' in pubmed_url:
                        pmid = \
                            pubmed_url.split('/pubmed/')[1].split(
                                '?itool=')[0]
                    elif 'ordinalpos=' in pubmed_url:
                        pmid = \
                            pubmed_url.split('/pubmed/')[1].split(
                                '?ordinalpos')[0]

                    elif 'gov/Pubmed/' in pubmed_url:
                        pmid = pubmed_url.split('/Pubmed/')[-1]
                    elif 'gov/pubmed/' in pubmed_url:
                        pmid = pubmed_url.split('/pubmed/')[-1]
                    else:
                        pmid = None
                else:
                    pubmed_url = None
                    pmid = None
                result.append([pubmed_url, pmid, article_title])
        else:
            result = []
        return result

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        if html.xpath('.//div[@class="primages"]'):
            sub_images = html.xpath(
                './/div[@class="primages"]/div[@id and @class]')
            result = []
            for sub in sub_images:
                image_url = sub.xpath('.//img/@src')[0]
                image_decription = \
                    sub.xpath('.//div[@class="literaturelegend"]/text()')
                image_decription = ''.join(i for i in image_decription)
                result.append([image_url, image_decription])
        else:
            result = []
        return result

    # ======================================================================== #
    # price表
    def sub_price(self, html):
        if html.xpath(
                './/tr/td[@style="padding-bottom:2px;"]/b/../..'):
            result = []
            sub_price = html.xpath(
                './/tr/td[@style="padding-bottom:2px;"]/b/../..')
            for sub in sub_price:
                sub_catano = sub.xpath('.//td/b/text()')[0]
                sub_size = sub.xpath(
                    './/td/b/../following-sibling::td[1]/text()')[0].replace(
                    '\\xa0', ' ')
                result.append([sub_catano, sub_size])
        else:
            result = []
        return result


def main():
    # url = 'https://www.enzolifesciences.com/ALX-210-647/adiponectin-receptor-2-mouse-polyclonal-antibody/'
    # for i in range(1):
    while r.exists('Ferrari'):
        extract = r.rpop('Ferrari')
        print(extract)
        flag, html = Enzo().format_html(extract)
        print(flag, html)
        if flag == 0:
            r.lpush('Ferrari', extract)
        brand = Enzo().brand()
        catalog_number = Enzo().catalog_number(extract)
        product_name = Enzo().product_name(html)
        antibody_type = Enzo().antibody_type(html)
        application = Enzo().application(html)
        conjugated = Enzo().conjugated(product_name)
        clone_number = Enzo().clone_number(html)
        recombinant_antibody = Enzo().recombinant_antibody(product_name)
        host_species = Enzo().host_species(html)
        antibody_detail_url = Enzo().antibody_detail_url(extract)
        species_reactivity = Enzo().species_reactivity(html)
        immunogen = Enzo().immunogen(html)
        swissprot = Enzo().swissprot(html)
        isotype = Enzo().isotype(html)
        purify = Enzo().purify(html)
        citations = Enzo().citations(html)
        dataSheet_url = Enzo().dataSheet_url(html)
        image_qty = Enzo().image_qty(html)
        synonyms = Enzo().synonyms(html)

        sub_application = Enzo().sub_application(html)
        sub_citations = Enzo().sub_citations(html)
        sub_images = Enzo().sub_images(html)
        sub_price = Enzo().sub_price(html)

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
                sub_article_title = sub[2]
                sub_pmid = sub[1]
                sub_pubmed_url = sub[0]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pmid,
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
                sub_catano = sub[0]
                sub_size = sub[1]

                new_price = Price(Catalog_Number=catalog_number,
                                  Size=sub_size,
                                  sub_Catalog_Number=sub_catano)
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        new_detail = Detail(Brand=brand,
                            Recombinant_Antibody=recombinant_antibody,
                            Host_Species=host_species,
                            Purify=purify,
                            Synonyms=synonyms,
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Antibody_Type=antibody_type,
                            Application=application,
                            Clone_Number=clone_number,
                            Conjugated=conjugated,
                            Antibody_detail_URL=antibody_detail_url,
                            Species_Reactivity=species_reactivity,
                            SwissProt=swissprot,
                            Immunogen=immunogen,
                            Isotype=isotype,
                            Citations=str(citations),
                            DataSheet_URL=dataSheet_url,
                            Image_qty=image_qty)
        try:
            session.add(new_detail)
            session.commit()
            session.close()
        except Exception as e:
            session.rollback()
            print(e)


if __name__ == '__main__':
    main()
