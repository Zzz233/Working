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
    __tablename__ = "bioaimscientific_kit_detail"

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
    __tablename__ = "bioaimscientific_kit_citations"

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
    __tablename__ = "bioaimscientific_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "bioaimscientific_kit_price"

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


class Bioaimscientific(object):
    headers = {
        "Host": "bioaimscientific.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        # 'Cookie': 'fpestid=aAMzkePsDFGjV2d2FtHNxeZFWutRH_20BPgK916ZPMoAsNQzQbAyB3NkeHbolv3l6V9AIg; _ga=GA1.2.1513056813.1608866763; _gid=GA1.2.345026214.1608866763; wp_woocommerce_session_d1e5af3d299cad260ba9cadc68d52d7d=e95e3e5cd30fe84034386e3fcc741176%7C%7C1609039562%7C%7C1609035962%7C%7Cdc51135a9759f35ba19eb779cf4aa91d',
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@class="items_group clearfix"]')[0]
        return content

    def brand(self):
        return "bioaimscientific"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        try:
            mora = html.xpath(
                './/p[@class="price"][contains(text(), "Catalog No : ")]/text()'
            )[0].split("Catalog No : ")[-1]
            return mora
        except Exception:
            return None

    def product_name(self, html):
        try:
            mora = html.xpath('.//h1[@itemprop="name"]/text()')[0].strip()
            return mora
        except Exception:
            return None

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Format")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Format")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def assay_type(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Assay Type")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Assay Type")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def detection_method(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Detection Method")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Detection Method")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def sample_type(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Sample Volume")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Sample Volume")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def assay_length(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Assay Length")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Assay Length")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def sensitivity(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Sensitivity")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Sensitivity")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def assay_range(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Assay Range")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Assay Range")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
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
            mora_1 = html.xpath(
                './/strong[contains(text(), "Gene ID")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Gene ID")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora = html.xpath(
                './/strong[contains(text(), "Swissprot ID")]/following-sibling::span[@style][1]/a/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            mora = html.xpath('.//li/a[contains(text(), "Manual")]/@href')[0].strip()
            return mora
        except Exception:
            return None

    def review(self, html):
        try:
            mora = int(
                html.xpath('.//li/a[@href="#tab-reviews"]/text()')[0]
                .replace("Reviews (", "")
                .replace(")", "")
            )
            return mora
        except Exception:
            return None

    def image_qty(self, html):
        try:
            mora = len(html.xpath('.//div[@class="thumbnails columns-3"]//img'))
            return mora
        except Exception:
            return 0

    def citations(self, html):
        return 0

    def synonyms(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Gene Names")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Gene names")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
            if len(mora) > 0:
                return mora
            else:
                return None
        except Exception:
            return None

    def conjugate(self, html):
        try:
            conjugate = html.xpath(
                './/div[@class="row"]/div[@class][contains(text(), "Conjugate")]/following-sibling::div[@class]/text()'
            )[0].strip()
            return conjugate
        except Exception:
            return None

    def species_reactivity(self, html):
        try:
            mora_1 = html.xpath(
                './/strong[contains(text(), "Reactivity")]/following-sibling::text()[1]'
            )[0].strip()
            if len(mora_1) > 0:
                return mora_1
            else:
                mora_2 = html.xpath(
                    './/strong[contains(text(), "Reactivity")]/following-sibling::span[@style][1]/text()'
                )[0].strip()
                mora = "".join((mora_1, mora_2))
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
            imgs = html.xpath('.//div[@class="thumbnails columns-3"]//img')
            for img in imgs:
                try:
                    img_url = img.xpath("./@src")[0].strip()
                    img_des = img.xpath("./@alt")[0].strip()
                    results.append([img_url, img_des])
                except Exception:
                    pass
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            divs = html.xpath(
                './/form[@class="variations_form cart"]/@data-product_variations'
            )[0]
            json_data = json.loads(divs)
        except Exception:
            return results
        for item in json_data:
            sub_siz = item["attributes"]["attribute_pa_sizes"]
            sub_pri = item["display_price"]
            results.append([sub_siz, sub_pri])
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("bioaimscientific_kit_detail"):
        extract = r.rpop("bioaimscientific_kit_detail")
        # extract = "https://bioaimscientific.com/product/rat-visfatin-easytest-eia-kit/"
        print(extract)
        try:
            lxml = Bioaimscientific().format(extract)
        except Exception as e:
            print(e)
            r.lpush("bioaimscientific_kit_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = Bioaimscientific().brand()
            kit_type = Bioaimscientific().kit_type()
            catalog_number = Bioaimscientific().catalog_number(lxml)
            product_name = Bioaimscientific().product_name(lxml)
            detail_url = Bioaimscientific().detail_url(extract)
            tests = Bioaimscientific().tests(lxml)
            assay_type = Bioaimscientific().assay_type(lxml)
            detection_method = Bioaimscientific().detection_method(lxml)
            sample_type = Bioaimscientific().sample_type(lxml)
            assay_length = Bioaimscientific().assay_length(lxml)
            sensitivity = Bioaimscientific().sensitivity(lxml)
            assay_range = Bioaimscientific().assay_range(lxml)
            #     specificity = Bioaimscientific().specificity(lxml)
            #     target_protein = Bioaimscientific().target_protein(lxml)
            geneid = Bioaimscientific().geneid(lxml)
            swissprot = Bioaimscientific().swissprot(lxml)
            datasheet_url = Bioaimscientific().datasheet_url(lxml)
            review = Bioaimscientific().review(lxml)
            image_qty = Bioaimscientific().image_qty(lxml)
            citations = Bioaimscientific().citations(lxml)
            synonyms = Bioaimscientific().synonyms(lxml)
            #     conjugate = Bioaimscientific().conjugate(lxml)
            species_reactivity = Bioaimscientific().species_reactivity(lxml)
            #     note = Bioaimscientific().note(lxml)

            #     sub_citations = Bioaimscientific().sub_citations(lxml)
            sub_images = Bioaimscientific().sub_images(image_qty, lxml)
            sub_price = Bioaimscientific().sub_price(lxml)

        else:
            r.lpush("bioaimscientific_kit_detail", extract)
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
            Assay_length=assay_length,
            Sensitivity=sensitivity,
            Assay_range=assay_range,
            # Specificity=specificity,
            # Target_Protein=target_protein,
            GeneId=geneid,
            SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            Review=str(review),
            Image_qty=image_qty,
            Citations=citations,
            Synonyms=synonyms,
            # Conjugate=conjugate,
            Species_Reactivity=species_reactivity,
            # Note=str(note),
        )
        session.add(new_detail)

        # if sub_citations:
        #     objects_sub_citations = []
        #     for sub in sub_citations:
        #         sub_pid = sub[0]
        #         sub_tit = sub[1]
        #         sub_pul = sub[2]

        #         new_citations = Citations(
        #             Catalog_Number=catalog_number,
        #             PMID=sub_pid,
        #             Article_title=sub_tit,
        #             Pubmed_url=sub_pul,
        #         )
        #         objects_sub_citations.append(new_citations)
        #     session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = sub[0]
                des = sub[1]

                new_images = Images(
                    Catalog_Number=catalog_number, Image_url=img, Image_description=des
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                sub_s = sub[0]
                sub_p = sub[1]

                new_price = Price(
                    Catalog_Number=catalog_number,
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
            r.lpush("bioaimscientific_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(0.5, 1.0))
