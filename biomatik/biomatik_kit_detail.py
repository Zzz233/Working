"""
Suitable Sample    sample type
Label conjugated 
Calibration Range    assay range
"""
from re import L
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
import pandas as pd

Base = declarative_base()


class Detail(Base):
    __tablename__ = "biomatik_kit_detail"

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
    __tablename__ = "biomatik_kit_citations"

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
    __tablename__ = "biomatik_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "biomatik_kit_price"

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


class Biomatik(object):
    headers = {}

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, timeout=60)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@class="container_page"]')[0]
        return content

    def brand(self):
        return "biomatik"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        catnum = str(
            html.xpath('.//dd[@class="productView-info-value"]/a/text()')[0]
        ).strip()
        return catnum

    def product_name(self, html):
        try:
            name = html.xpath('.//h1[@class="productView-title"]/text()')[0].strip()
            return name
        except Exception:
            name = None

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            mora = html.xpath(
                './/h2[contains(text(), "Format")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/h2[contains(text(), "Assay Type")]/following-sibling::div[1]/text()'
            )[0].strip()
            return assay_type
        except Exception:
            return None

    def detection_method(self, html):
        try:
            detection_method = html.xpath(
                './/h2[contains(text(), "Detection Method")]/following-sibling::div[1]/text()'
            )[0].strip()
            return detection_method
        except Exception:
            return None

    def sample_type(self, html):
        try:
            sample_type = html.xpath(
                './/h2[contains(text(), "Sample Type")]/following-sibling::div[1]/text()'
            )[0].strip()
            return sample_type
        except Exception:
            return None

    def assay_length(self, html):
        try:
            assay_length = html.xpath(
                './/h2[contains(text(), "Assay Time")]/following-sibling::div[1]/text()'
            )[0].strip()
            return assay_length
        except Exception:
            return None

    def sensitivity(self, html):
        try:
            mora = html.xpath(
                './/h2[contains(text(), "Specificity")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_range(self, html):
        try:
            assay_range = html.xpath(
                './/h2[contains(text(), "Detection Range")]/following-sibling::div[1]/text()'
            )[0].strip()
            return assay_range
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
            mora = html.xpath(
                './/h2[contains(text(), "Target Name")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def geneid(self, html):
        try:
            mora = html.xpath(
                './/h2[contains(text(), "NCBI Gene ID")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora = html.xpath(
                './/h2[contains(text(), "UniProt ID")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            datasheet_url = (
                "https://www.biomatik.com"
                + html.xpath('.//i[@class="fa fa-file"]/../@href')[0].strip()
            )
            return datasheet_url
        except Exception:
            return None

    def review(self, html):
        return 0

    def image_qty(self, html):
        return 0

    def citations(self, html):
        return 0

    def synonyms(self, html):
        try:
            mora = html.xpath(
                './/h2[contains(text(), "Target Synonyms")]/following-sibling::div[1]/text()'
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
            mora = html.xpath(
                './/h2[contains(text(), "Species Reactivity")]/following-sibling::div[1]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def note(self, html):
        return None

    # ======================================================================== #
    # Citations表
    def sub_citations(self, citations, html):
        results = []
        if citations == 0:
            return results
        else:
            citations_text = (
                html.xpath(
                    './/script[contains(text(), "THERMO_FISHER.pdp.references")]/text()'
                )[0]
                .split("THERMO_FISHER.pdp.references = ")[-1]
                .split("];")[0]
                + "]"
            )
            json_data = json.loads(citations_text)
            for item in json_data:
                title = item["title"]
                url = item["titleLink"]
                if url == "":
                    url = None
                species = item["species"]
                if species == "":
                    species = None
                if "www.ncbi.nlm.nih.gov/pubmed/" in url:
                    pmid = url.split("gov/pubmed/")[-1]
                else:
                    pmid = None
                results.append([pmid, species, title, url])
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            a_tags = html.xpath(
                './/div[@class="image_inside"]/a[@data-lightbox="image-1" and not (@href="https://www.abbexa.com/image/cache/catalog/Abbexa_ELISA_box-500x500.png")]'
            )
            for a in a_tags:
                try:
                    img_url = a.xpath("./@href")[0].strip()
                    img_des = a.xpath("./@data-title")[0].strip()
                    results.append([img_url, img_des])
                except Exception:
                    pass
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            size = (
                html.xpath('.//b[contains(text(), "Size")]/../text()')[0]
                .replace(": ", "")
                .strip()
            )
            price_tag = html.xpath('.//div[@class="productView-price"]')[0]
            original_price = price_tag.xpath(".//div/span/text()")[0]
            try:
                discount_price = price_tag.xpath('.//span[@style="color:red;"]/text()')[
                    0
                ].strip()
            except Exception:
                discount_price = None
            results.append([original_price, discount_price, size])
            return results
        except Exception:
            return results


if __name__ == "__main__":
    # ! url去掉?limit=100
    # for i in range(1):
    while r.exists("biomatik_detail"):
        # single_data = r.rpop("sigmaaldrich_detail")
        # extract = single_data.split(",")[0]
        # catano = single_data.split(",")[1]
        extract = r.rpop("biomatik_detail")
        # extract = "https://www.biomatik.com/elisa-kits/human-wingless-type-mmtv-integration-site-family-member-3-wnt3-elisa-kit-cat-ekl61560/"
        print(extract)
        try:
            lxml = Biomatik().format(extract)
        except Exception as e:
            print(e)
            # r.lpush("abbexa_detail", extract)
            # time.sleep(30)
            # print("sleeping...")
            continue
        if lxml is not None:
            brand = Biomatik().brand()
            kit_type = Biomatik().kit_type()
            catalog_number = Biomatik().catalog_number(lxml)
            product_name = Biomatik().product_name(lxml)
            detail_url = Biomatik().detail_url(extract)
            tests = Biomatik().tests(lxml)
            assay_type = Biomatik().assay_type(lxml)
            detection_method = Biomatik().detection_method(lxml)
            sample_type = Biomatik().sample_type(lxml)
            assay_length = Biomatik().assay_length(lxml)
            sensitivity = Biomatik().sensitivity(lxml)
            assay_range = Biomatik().assay_range(lxml)
            specificity = Biomatik().specificity(lxml)
            target_protein = Biomatik().target_protein(lxml)
            geneid = Biomatik().geneid(lxml)
            swissprot = Biomatik().swissprot(lxml)
            datasheet_url = Biomatik().datasheet_url(lxml)
            review = Biomatik().review(lxml)
            image_qty = Biomatik().image_qty(lxml)
            citations = Biomatik().citations(lxml)
            synonyms = Biomatik().synonyms(lxml)
            # conjugate = Biomatik().conjugate(lxml)
            species_reactivity = Biomatik().species_reactivity(lxml)
            # note = Biomatik().note(lxml)

            # sub_citations = Biomatik().sub_citations(citations, lxml)
            # sub_images = Biomatik().sub_images(image_qty, lxml)
            sub_price = Biomatik().sub_price(lxml)
            # print(specificity)
        else:
            r.lpush("biomatik_detail", extract)
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
            Specificity=specificity,
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
            # Note=str(note),
        )
        session.add(new_detail)

        # if sub_citations:
        #     objects_sub_citations = []
        #     for sub in sub_citations:
        #         sub_pid = sub[0]
        #         sub_species = sub[1]
        #         sub_tit = sub[2]
        #         sub_pul = sub[3]

        #         new_citations = Citations(
        #             Catalog_Number=catalog_number,
        #             PMID=sub_pid,
        #             Article_title=sub_tit,
        #             Pubmed_url=sub_pul,
        #             Species=sub_species,
        #         )
        #         objects_sub_citations.append(new_citations)
        #     session.bulk_save_objects(objects_sub_citations)

        # if sub_images:
        #     objects_sub_images = []
        #     for sub in sub_images:
        #         img = sub[0]
        #         des = sub[1]

        #         new_images = Images(
        #             Catalog_Number=catalog_number, Image_url=img, Image_description=des
        #         )
        #         objects_sub_images.append(new_images)
        #     session.bulk_save_objects(objects_sub_images)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                sub_price_o = sub[0]
                sub_price_d = sub[1]
                sub_size = sub[2]
                new_price = Price(
                    Catalog_Number=catalog_number,
                    Price=sub_price_o,
                    Size=sub_size,
                    Note=sub_price_d,
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("biomatik_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(0.5, 1.0))
