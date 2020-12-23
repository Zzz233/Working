"""
citations url ——> note
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
import json
import re

Base = declarative_base()


class Detail(Base):
    __tablename__ = "raybiotech_kit_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Kit_Type = Column(String(100), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Tests = Column(String(200), nullable=True, comment="")
    Assay_type = Column(String(200), nullable=True, comment="")
    Detection_Method = Column(String(200), nullable=True, comment="")
    Sample_type = Column(String(1000), nullable=True, comment="")
    Assay_length = Column(String(200), nullable=True, comment="")
    Sensitivity = Column(String(200), nullable=True, comment="")
    Assay_range = Column(String(200), nullable=True, comment="")
    Specificity = Column(String(200), nullable=True, comment="")
    Target_Protein = Column(String(200), nullable=True, comment="")
    GeneId = Column(String(500), nullable=True, comment="")
    SwissProt = Column(String(500), nullable=True, comment="")
    DataSheet_URL = Column(String(500), nullable=True, comment="")
    Review = Column(String(50), nullable=True, comment="")
    Image_qty = Column(Integer, nullable=True, comment="")
    Citations = Column(Integer, nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Conjugate = Column(String(200), nullable=True, comment="")
    Species_Reactivity = Column(String(200), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "raybiotech_kit_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    PMID = Column(String(40), nullable=True, comment="")
    Species = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(1000), nullable=True, comment="")
    Sample_type = Column(String(100), nullable=True, comment="")
    Pubmed_url = Column(String(1000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Images(Base):
    __tablename__ = "raybiotech_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "raybiotech_kit_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


# Mysql
engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)


class Raybiotech(object):
    headers = {
        "Host": "www.raybiotech.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        # "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.raybiotech.com/products/elisa/",
        "Connection": "keep-alive",
        # "Cookie": "__cfduid=dc9577877a83f9370b79ee9fb2f60b1ba1608685173; sid_customer_07a72=c5c9726a007fb1fbffee1a7c0651c58c-1-C; _ga=GA1.2.1317898597.1608685177; _gid=GA1.2.66379256.1608685177; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-site_visit_time=1608700247107; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-visit_count=%7B%22https%3A//*%22%3A48%2C%22website_count%22%3A48%7D; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-ag9zfmNsaWNrZGVza2NoYXRyHAsSD3Byb2FjdGl2ZV9ydWxlcxiAgKCh5suuCwwonce_per_sessionnull=true; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-ag9zfmNsaWNrZGVza2NoYXRyHAsSD3Byb2FjdGl2ZV9ydWxlcxiAgKChn7LFCgwonce_per_sessionnull=true; all_RyEgsSBXVzZXJzGICAoMGnnI8LDA-clickdesk_referrer=https%3A//www.raybiotech.com/products/elisa/page-338/; _gat=1",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@class="tygh-content clearfix"]')[0]
            text_html = resp.text
        return content, text_html

    def brand(self):
        return "raybiotech"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, catanum):
        return catanum

    def product_name(self, html):
        try:
            name = html.xpath('.//h1[@class="ty-product-block-title"]/bdi/text()')[
                0
            ].strip()
            return name
        except Exception:
            return None

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            tests = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Size:")]/following-sibling::div[@class="ty-product-feature__value"]/text()'
            )[0].strip()
            return tests
        except Exception:
            return None

    def assay_type(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Design Principle:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def detection_method(self, html):
        try:
            mora = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(),"Method of Detection:")]/following-sibling::div[@class="ty-product-feature__value"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def sample_type(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Compatible Sample Types:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def assay_length(self, html):
        try:
            assay_length_list = html.xpath(
                './/div[@class="content"][@style="display:none;"]//p/text()'
            )
            for item in assay_length_list:
                if "Assay Time: " in item:
                    assay_length = item.split("Assay Time: ")[-1]
                    if len(assay_length) > 0:
                        return assay_length
                    else:
                        return None
        except Exception:
            return None

    def sensitivity(self, html):
        try:
            mora = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(),"Sensitivity:")]/following-sibling::div[@class="ty-product-feature__value"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_range(self, html):
        try:
            assay_range_list = html.xpath(
                './/div[@class="content"][@style="display:none;"]//p/text()'
            )
            for item in assay_range_list:
                if "Detect Range: " in item:
                    assay_range = item.split("Detect Range: ")[-1]
                    if len(assay_range) > 0:
                        return assay_range
                    else:
                        return None
        except Exception:
            return None

    def specificity(self, html):
        try:
            specificity = html.xpath(
                './/h4[contains(text(), "Application")]/following-sibling::p[1]/text()'
            )[0].strip()
        except Exception:
            return None
        return specificity

    def target_protein(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Gene Symbols:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def geneid(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Gene ID:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Accession Number:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            mora = html.xpath('.//span[contains(text(), "Manual")]/../@href')[0].strip()
            return mora
        except Exception:
            return None

    def review(self, html):
        return 0

    def image_qty(self, html):
        try:
            mora = len(
                html.xpath(
                    './/img[@style="display: block; margin-left: auto; margin-right: auto;"]'
                )
            )
            return mora
        except Exception:
            return 0

    def citations(self, html):
        return 0

    def synonyms(self, html):
        try:
            mora = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(),"Protein Name & Synonyms:")]/following-sibling::div[@class="ty-product-feature__value"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def conjugate(self, html):
        try:
            conjugate = html.xpath(
                './/td[contains(text(),"Detection Antibody Conjugation")]/following-sibling::td/text()'
            )[0].strip()
            return conjugate
        except Exception:
            return None

    def species_reactivity(self, html):
        try:
            mora_text = html.xpath(
                './/span[@class="ty-product-feature__label"][contains(text(), "Species Detected:")]/following-sibling::div[@class="ty-product-feature__value"]//li/text()'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def note(self, html):
        try:
            mora_text = html.xpath(
                './/a[@data-ca-scroll="#tygh_main_container"][@data-ca-page][@class="cm-history ty-pagination__item cm-ajax"][@data-ca-target-id="pagination_contents_citations_"]/@href'
            )
            mora = ",".join(i for i in mora_text)
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    # ======================================================================== #
    # Citations表
    def sub_citations(self, html):
        results = []

        lis = html.xpath(
            './/div[@class="ty-pagination-container cm-pagination-container"][@id="pagination_contents_citations_"]//li'
        )
        try:
            for li in lis:
                title = li.xpath("./a/text()")[0].strip()
                link = li.xpath("./a/@href")[0].strip()
                if "https://www.ncbi.nlm.nih.gov/pubmed/" in link:
                    pmid = link.split("gov/pubmed/")[-1]
                else:
                    pmid = None
                try:
                    species = li.xpath('.//b[contains(text(), "Species:")]/../text()')[
                        0
                    ].strip()
                except Exception:
                    species = None
                try:
                    sampleType = li.xpath(
                        './/b[contains(text(), "Sample type:")]/../text()'
                    )[0].strip()
                except Exception:
                    sampleType = None
                results.append([title, link, species, sampleType, pmid])
            return results
        except Exception:
            return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            imgs = html.xpath(
                './/img[@style="display: block; margin-left: auto; margin-right: auto;"]'
            )
            for img in imgs:
                try:
                    img_url = img.xpath("./@src")[0].strip()
                    img_des = img.xpath("./@title")[0].strip()
                    results.append([img_url, img_des])
                except Exception:
                    pass
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            td_catano = html.xpath(
                './/tbody//td[@class="ty-variations-list__product-elem ty-variations-list__product-elem-options ty-variations-list__sku"]/a/div/span[@class][@id]'
            )
            td_size = html.xpath(
                './/tbody//td[@class="ty-variations-content__product-elem ty-variations-content__product-elem-options"]/bdi/span/span[@class="ty-product-options-content"][contains(text(), "Kit")]'
            )
            td_price = html.xpath(
                './/tbody//td[@class="ty-variations-list__product-elem ty-variations-list__price"]/span[@class][@id]/span[@class][@id]/span[@id][@class]'
            )
        except Exception:
            return results
        if len(td_catano) == len(td_size) == len(td_price):
            for i, item in enumerate(td_catano):
                s_catano = item.xpath("./text()")[0].strip()
                s_size = td_size[i].xpath("./text()")[0].strip()
                s_price = "$" + td_price[i].xpath("./text()")[0].strip()
                print(s_catano, s_size, s_price)
                results.append([s_catano, s_size, s_price])
            return results
        else:
            return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("raybiotech_kit_detail"):
        extract1 = r.rpop("raybiotech_kit_detail")
        catalog = extract1.split(";")[0]
        extract = extract1.split(";")[-1]
        print(extract1)
        try:
            lxml, text_html = Raybiotech().format(extract)
        except Exception as e:
            print(e)
            r.lpush("raybiotech_kit_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = Raybiotech().brand()
            kit_type = Raybiotech().kit_type()
            catalog_number = Raybiotech().catalog_number(catalog)
            product_name = Raybiotech().product_name(lxml)
            detail_url = Raybiotech().detail_url(extract)
            tests = Raybiotech().tests(lxml)
            assay_type = Raybiotech().assay_type(lxml)
            detection_method = Raybiotech().detection_method(lxml)
            sample_type = Raybiotech().sample_type(lxml)
            #     assay_length = Raybiotech().assay_length(lxml)
            sensitivity = Raybiotech().sensitivity(lxml)
            #     assay_range = Raybiotech().assay_range(lxml)
            #     specificity = Raybiotech().specificity(lxml)
            target_protein = Raybiotech().target_protein(lxml)
            geneid = Raybiotech().geneid(lxml)
            swissprot = Raybiotech().swissprot(lxml)
            datasheet_url = Raybiotech().datasheet_url(lxml)
            review = Raybiotech().review(lxml)
            image_qty = Raybiotech().image_qty(lxml)
            citations = Raybiotech().citations(lxml)
            synonyms = Raybiotech().synonyms(lxml)
            #     conjugate = Raybiotech().conjugate(lxml)
            species_reactivity = Raybiotech().species_reactivity(lxml)
            note = Raybiotech().note(lxml)

            sub_citations = Raybiotech().sub_citations(lxml)
            sub_images = Raybiotech().sub_images(image_qty, lxml)
            sub_price = Raybiotech().sub_price(lxml)
            # print(sub_price)

        else:
            r.lpush("raybiotech_kit_detail", extract)
            print("html is none")
            continue
        new_detail = Detail(
            Brand=brand,
            Kit_Type=kit_type,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Detail_url=detail_url,
            Tests=tests,
            Assay_type=assay_type,
            Detection_Method=detection_method,
            Sample_type=sample_type,
            # Assay_length=assay_length,
            # Assay_range=assay_range,
            # Sensitivity=sensitivity,
            # Specificity=specificity,
            Target_Protein=target_protein,
            GeneId=geneid,
            SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            Review=str(review),
            Image_qty=image_qty,
            Citations=citations,
            Synonyms=synonyms,
            # Conjugate=conjugate,
            Species_Reactivity=species_reactivity,
            Note=str(note),
        )
        session.add(new_detail)

        if sub_citations:
            # title, link, species, sampleType, pmid
            objects_sub_citations = []
            for sub in sub_citations:
                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub[0],
                    Species=sub[1],
                    Article_title=sub[2],
                    Sample_type=sub[3],
                    Pubmed_url=sub[4],
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = detail_url + sub[0]
                des = sub[1]

                new_images = Images(
                    Catalog_Number=catalog_number, Image_url=img, Image_description=des
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                sub_c = sub[0]
                sub_s = sub[1]
                sub_p = sub[2]

                new_price = Price(
                    Catalog_Number=catalog_number,
                    sub_Catalog_Number=sub_c,
                    Size=sub_s,
                    Price=sub_p,
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("raybiotech_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
