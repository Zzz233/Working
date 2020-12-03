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


class Sysy():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'SY=9iqieoyzoovo54hauwfztxkba8kl6p7h; PHPSESSID=f5e0bccqh6123iuded6mhce5ep',
        'Host': '_',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
    }

    def get_headers(self, url):
        if 'sysy-histosure.com' in url:
            self.headers['Host'] = 'sysy-histosure.com'
        else:
            self.headers['Host'] = 'www.sysy.com'
        return self.headers

    def format_html(self, url, headers_content):
        with requests.Session() as s:
            resp = s.get(url=url, headers=headers_content, timeout=120)
            # print(resp.text)
            html_parsed = etree.HTML(resp.text)
            # TODO 条件
            if html_parsed.xpath('//div[@class="container product-detail"]'):
                html_content = html_parsed.xpath(
                    '//div[@class="container product-detail"]')[0]
                status = 1
            else:
                status = 0
                html_content = None
        return status, html_content

    # ======================================================================== #
    # detail表
    def brand(self):
        return 'Synaptic Systems'

    def catalog_number(self, url):
        catano = url.split('/')[-1]
        return catano

    def product_name(self, html):
        name = html.xpath('.//div[@class="col"]/h1//text()')[0].strip()
        return name

    def antibody_type(self, html):
        # 有 单抗  无 多抗
        if html.xpath('.//td[@class="firstCell"][contains(text(), "Clone")]'):
            antibody_type = "Monoclonal"
        else:
            antibody_type = "Polyclonal"
        return antibody_type

    def sellable(self, html):
        if html.xpath('.//div[contains(text(), "Price:")]'):
            sellable = 'yes'
        else:
            sellable = 'no'
        return sellable

    def synonyms(self, html):
        return None

    def application(self, html):
        if html.xpath(
                './/td[@class="firstCell"][contains(text(), "Applications")]'):
            application = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Applicatio'
                'ns")]/following-sibling::td[@valign="top"]/b//text()')
            application_str = ', '.join(item for item in application)
        else:
            application_str = None
        return application_str

    def conjugated(self, html):
        # TODO Label!!!!
        if html.xpath(
                './/td[@class="firstCell"][contains(text(),"Label")]'):
            conjugated = html.xpath(
                './/td[@class="firstCell"][contains(text('
                '),"Label")]/following-sibling::td[@valign="top"]/text()')[
                0].strip()
        else:
            conjugated = None
        return conjugated

    def clone_number(self, html):
        # 单抗有 多抗没有
        if html.xpath(
                './/td[@class="firstCell"][contains(text(),"Clone")]'):
            clone_num = html.xpath(
                './/td[@class="firstCell"][contains(text('
                '),"Clone")]/following-sibling::td[@valign="top"]/text()')[
                0].strip()
        else:
            clone_num = None
        return clone_num

    def recombinant_antibody(self):
        # TODO
        return None

    def modified(self):
        # TODO
        return None

    def host_species(self, html):
        # TODO
        return None

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
        # TODO
        return None

    # def ko_validation(self, html):
    #     if html.xpath('.//span[@class="info-pill"][contains(text(), "K.O.")]'):
    #         ko_validation = 'yes'
    #     else:
    #         ko_validation = 'no'
    #     return ko_validation

    def species_reactivity(self, html):
        if html.xpath(
                './/td[@class="firstCell"][contains(text(), "Reactivity")]'):
            species_reactivity = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Re'
                'activity")]/following-sibling::td[@valign="top"]//text()')
            application_str = ', '.join(
                item for item in species_reactivity).strip()
        else:
            application_str = None
        return application_str

    def immunogen(self, html):
        flag = None
        immunogen = None
        if html.xpath(
                './/td[@class="firstCell"][contains(text(), "Immunogen")]'):
            immunogen = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Immunogen")]/following-sibling::td[@valign="top"]/text()')[
                0].strip()
            if '(UniProt Id:' in immunogen:
                immunogen = immunogen.replace('(UniProt Id:', '')
                flag = 1
        return flag, immunogen

    def swissprot(self, html, flag):
        if flag:
            swissprot = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Imm'
                'unogen")]/following-sibling::td[@valign="top"]/a/text()')[0]
        else:
            swissprot = None
        return swissprot

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self, html):
        if html.xpath('.//td[@class="firstCell"][contains(text(), "Subtype")]'):
            isotype = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Subty'
                'pe")]/following-sibling::td[@valign="top"]/text()')[0].strip()
        else:
            isotype = None
        return isotype

    def purify(self, html):
        # TODO
        return None

    def citations(self, html):
        if html.xpath(
                './/div[@class="product-reference-title"]'):
            citations = len(html.xpath(
                './/div[@id="product-reference-list-main"]//div[@class="reference-entry"]'))
        else:
            citations = 0
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        if html.xpath(
                './/td[@class="firstCell"][contains(text(), "Data sheet")]'):
            dataSheet_url = html.xpath(
                './/td[@class="firstCell"][contains(text(), "Data'
                ' sheet")]/following-sibling::td[@valign="top"]/a/@href')[0]
        else:
            dataSheet_url = None
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        if html.xpath('.//div[@class="col-3 thumbnail-box"]'):
            image_qty = len(html.xpath('.//div[@class="col-3 thumbnail-box"]'))
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
        # sub_application = item.split(' ')[0]
        # sub_dilution = item.split(' ')[1]
        if application:
            for bad in html.xpath(
                    './/td[@class="firstCell"][contains(text(),"Applications")]/following-sibling::td[@valign="top"]/a'):
                bad.getparent().remove(bad)
            sub_application = html.xpath(
                './/td[@class="firstCell"][contains(text(),"Applications")]/following-sibling::td[@valign="top"]/b/text()')
            result = []
            for i, sub in enumerate(sub_application):
                sub_dilution = html.xpath(
                    './/td[@class="firstCell"][contains(text(),"Applications")]/following-sibling::td[@valign="top"]/text()')[
                    i + 1].strip().replace(':\xa0', '')
                sub_application = sub.strip()
                result.append([sub_application, sub_dilution])

        else:
            result = []
        return result

    # ======================================================================== #
    # citations表

    def sub_citations(self, citations, html):
        if citations > 0:
            sub_citations = html.xpath('.//div[@id="product-reference-list-ma'
                                       'in"]//div[@class="reference-entry"]')
            result = []
            for item in sub_citations:
                pubmed_url = item.xpath('.//a/@href')[0]
                pmid = pubmed_url.split('/')[-1]
                article_title = item.xpath('.//a/text()')[0].strip()
                if item.xpath('.//b'):
                    b_text = item.xpath('.//b/text()')[0].replace('\xa0', ' ')
                    application = b_text.split(';')[0]
                    if 'tested species:' in b_text:
                        species = b_text.split('tested species:')[1].strip()
                        # print(species)
                    else:
                        species = None
                else:
                    application = None
                    species = None
                result.append(
                    [pubmed_url, pmid, article_title, application, species])
        else:
            result = []
        return result

    # ======================================================================== #
    # images表
    def sub_images(self, image_qty, html):
        if image_qty > 0:
            sub_images = html.xpath('.//div[@class="col-3 thumbnail-box"]')
            result = []
            for sub in sub_images:
                image_url = 'https://sysy-histosure.com/' + \
                            sub.xpath('.//a[@data-fancybox="gallery"]/@href')[0]
                image_decription = sub.xpath(
                    './/a[@data-fancybox="gall'
                    'ery"]/@data-caption')[0].replace('&nbsp;', ' ').replace(
                    '<br>', '').replace('\n', '').split(
                    '</h1>')[1]
                result.append([image_url, image_decription])
        else:
            result = []
        return result

    # ======================================================================== #
    # price表
    def sub_price(self, sellable, html):
        if sellable is 'yes':
            result = []
            sub_size = html.xpath(
                './/div[@class="product-info-pill d-none d-lg-block"]//div[co'
                'ntains(text(), "Quantity:")]/span/text()')[0]
            sub_price = html.xpath(
                './/div[@class="product-info-pill d-none d-lg-block"]//div[co'
                'ntains(text(), "Price:")]/span/text()')[0].replace('\xa0',
                                                                    ' ')
            result.append([sub_size, sub_price])
        else:
            result = []
        return result


if __name__ == '__main__':
    while r.exists('sysy'):

        extract = r.lpop('sysy')
        link = extract.split(',')[0]
        ko_validation = extract.split(',')[1]

        headers = Sysy().get_headers(link)
        flag, html = Sysy().format_html(link, headers)
        if flag == 1:
            brand = Sysy().brand()
            catalog_number = Sysy().catalog_number(link)
            antibody_detail_url = Sysy().antibody_detail_url(link)
            product_name = Sysy().product_name(html)
            antibody_type = Sysy().antibody_type(html)
            sellable = Sysy().sellable(html)
            application = Sysy().application(html)
            clone_number = Sysy().clone_number(html)
            species_reactivity = Sysy().species_reactivity(html)
            flag_2, immunogen = Sysy().immunogen(html)
            swissprot = Sysy().swissprot(html, flag_2)
            isotype = Sysy().isotype(html)
            citations = Sysy().citations(html)
            dataSheet_url = Sysy().dataSheet_url(html)
            image_qty = Sysy().image_qty(html)
            conjugated = Sysy().conjugated(html)
            sub_application = Sysy().sub_application(application, html)
            sub_citations = Sysy().sub_citations(citations, html)
            sub_images = Sysy().sub_images(image_qty, html)
            sub_price = Sysy().sub_price(sellable, html)

            print(sub_application)

            if sub_application:
                objects_sub_application = []
                application_str = []
                for sub in sub_application:
                    sub_application = sub[0]
                    sub_dilution = sub[1]
                    if 'not tested yet' not in sub_dilution:
                        new_application = Application(
                            Catalog_Number=catalog_number,
                            Application=sub_application,
                            Dilution=sub_dilution
                        )
                        objects_sub_application.append(new_application)
                        application_str.append(sub_application)
                session.bulk_save_objects(objects_sub_application)
                application = ', '.join(item for item in application_str)

            if sub_citations:
                objects_sub_citations = []
                for sub in sub_citations:
                    sub_article_title = sub[2]
                    sub_pmid = sub[1]
                    sub_pubmed_url = sub[0]
                    sub_species = sub[4]
                    sub_application = sub[3]

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
                    sub_size = sub[0]
                    sub_price = sub[1]

                    new_price = Price(Catalog_Number=catalog_number,
                                      Size=sub_size,
                                      Price=sub_price)
                    objects_sub_price.append(new_price)
                session.bulk_save_objects(objects_sub_price)

            new_detail = Detail(Brand=brand,
                                Catalog_Number=catalog_number,
                                Product_Name=product_name,
                                Antibody_Type=antibody_type,
                                Sellable=sellable,
                                Application=application,
                                Clone_Number=clone_number,
                                Conjugated=conjugated,
                                KO_Validation=ko_validation,
                                Antibody_detail_URL=antibody_detail_url,
                                Species_Reactivity=species_reactivity,
                                SwissProt=swissprot,
                                Immunogen=immunogen,
                                Isotype=isotype,
                                Citations=str(citations),
                                DataSheet_URL=dataSheet_url,
                                Image_qty=image_qty)
            session.add(new_detail)

            session.commit()
            session.close()
            print('done')
        else:
            r.rpush('sysy', link)
            break
