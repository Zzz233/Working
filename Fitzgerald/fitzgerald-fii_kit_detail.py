"""
Suitable Sample    sample type
Label conjugated 
Calibration Range    assay range
"""
# ! Assay Information 放在了 Detai.Note
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
    __tablename__ = "fitzgerald_kit_detail"

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
    __tablename__ = "fitzgerald_kit_citations"

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
    __tablename__ = "fitzgerald_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "fitzgerald_kit_price"

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


class Fitzgerald(object):
    headers = {
        "Host": "www.fitzgerald-fii.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        # "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.fitzgerald-fii.com/research-kits.html?applications=81&dir=desc&order=totalimages",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@id="main"]')[0]
        return content

    def brand(self):
        return "fitzgerald-fii"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        catnum = (
            html.xpath('.//span[@class="catalog-no"]/text()')[0].lstrip("(").rstrip(")")
        )
        return catnum

    def product_name(self, html):
        try:
            name_list = html.xpath('.//span[@itemprop="name"]//text()')
            name = "".join(i for i in name_list).strip()
        except Exception:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self):  # 48T/96T 使用次数 规格
        return None

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Assay kit format")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_type

    def detection_method(self, html):
        try:
            detection_method = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Instrument")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return detection_method

    def sample_type(self, html):
        # Suitable Sample:
        try:
            divs = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Sample type/volume")]/../following-sibling::div/div'
            )
            results = []
            for i, div in enumerate(divs):
                type = div.xpath(
                    './/div[@class="extra-large-span-4 large-span-4 medium-span-4 small-span-8 padding-zero"]/p/text()'
                )[0].strip()
                volume = (
                    divs[i]
                    .xpath(
                        './/div[@class="extra-large-span-6 large-span-6 medium-span-6 small-span-4 padding-zero"]/p/text()'
                    )[0]
                    .strip()
                )
                sample_type_single = type + "," + volume
                results.append(sample_type_single)
            sample_type = "|".join(n for n in results)
        except Exception:
            return None
        return sample_type

    def assay_length(self, html):
        try:
            assay_length = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Time-to-result")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_length

    def sensitivity(self, html):
        try:
            sensitivity = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Analytical sensitivity")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return sensitivity

    def assay_range(self, html):
        try:
            assay_range = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Assay range")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_range

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
            target_protein = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Protein name")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
        except Exception:
            return None
        return target_protein

    def geneid(self, html):
        try:
            geneid = (
                html.xpath(
                    './/p[@class="bold styles-for-key"][contains(text(), "Gene ID")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/a/text()'
                )[0]
                .split(")")[-1]
                .strip()
            )
        except Exception:
            return None
        return geneid

    def swissprot(self, html):
        try:
            swissprot = (
                html.xpath(
                    './/p[@class="bold styles-for-key"][contains(text(), "UniProt ID")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/a/text()'
                )[0]
                .split(")")[-1]
                .strip()
            )
        except Exception:
            return None
        return swissprot

    def datasheet_url(self, html):
        try:
            datasheet_url = html.xpath(
                './/a[@class="btn btn-alt green uppercase"]/@href'
            )[0].strip()
            return datasheet_url
        except Exception:
            return None

    def review(self, html):
        return None

    def image_qty(self, html):
        try:
            image_qty = len(html.xpath('.//div[@id="product-print-images"]//img'))
            return image_qty
        except Exception:
            return 0

    def citations(self, html):
        try:
            citations_text = (
                html.xpath(
                    './/script[contains(text(), "THERMO_FISHER.pdp.references")]/text()'
                )[0]
                .split("THERMO_FISHER.pdp.references = ")[-1]
                .split("];")[0]
                + "]"
            )
            json_data = json.loads(citations_text)
            citations = len(json_data)
            return citations
        except Exception:
            return 0

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/th[@class="label"][contains(text(), "Synonyms")]/following-sibling::td[@class="data"]/text()'
            )[0].strip()
            return synonyms
        except Exception:
            return None

    def conjugate(self, html):
        try:
            conjugate = html.xpath(
                './/p[@class="bold styles-for-key"][contains(text(), "Label or dye")]/../following-sibling::div[@class]/p[@class="styles-for-values"]/text()'
            )[0].strip()
            return conjugate
        except Exception:
            return None

    def species_reactivity(self, html):
        try:
            species_reactivity = html.xpath(
                './/th[@class="label"][contains(text(), "Species")]/following-sibling::td[@class="data"]/text()'
            )[0].strip()
            return species_reactivity
        except Exception:
            return None

    def note(self, html):
        try:
            note = html.xpath(
                './/th[@class="label"][contains(text(), "Assay Information")]/following-sibling::td[@class="data"]/text()'
            )[0].strip()
            return note
        except Exception:
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
            imgs = html.xpath('.//div[@id="product-print-images"]//img')
            for img in imgs:
                try:
                    img_url = img.xpath("./@src")[0].strip()
                    img_des = img.xpath("./@alt")[0].strip().replace("\n", "")
                    results.append([img_url, img_des])
                except Exception:
                    pass
        return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            size = html.xpath('.//span[@class="pull-right"]/text()')[0].strip()
            price = html.xpath('.//span[@class="price"]/text()')[0].strip()
            results.append([size, price])
            return results
        except Exception:
            return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("fitzgerald_detail"):
        # single_data = r.rpop("sigmaaldrich_detail")
        # extract = single_data.split(",")[0]
        # catano = single_data.split(",")[1]
        extract = r.rpop("fitzgerald_detail")
        # extract = "https://www.fitzgerald-fii.com/influenza-a-nucleoprotein-elisa-kit-55r-2250.html"
        print(extract)
        try:
            lxml = Fitzgerald().format(extract)
        except Exception as e:
            print(e)
            r.lpush("fitzgerald_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = Fitzgerald().brand()
            kit_type = Fitzgerald().kit_type()
            catalog_number = Fitzgerald().catalog_number(lxml)
            product_name = Fitzgerald().product_name(lxml)
            detail_url = Fitzgerald().detail_url(extract)
            #     tests = Fitzgerald().tests(lxml)
            #     assay_type = Fitzgerald().assay_type(lxml)
            #     detection_method = Fitzgerald().detection_method(lxml)
            #     sample_type = Fitzgerald().sample_type(lxml)
            #     assay_length = Fitzgerald().assay_length(lxml)
            #     sensitivity = Fitzgerald().sensitivity(lxml)
            #     assay_range = Fitzgerald().assay_range(lxml)
            #     specificity = Fitzgerald().specificity(lxml)
            #     target_protein = Fitzgerald().target_protein(lxml)
            #     geneid = Fitzgerald().geneid(lxml)
            #     swissprot = Fitzgerald().swissprot(lxml)
            datasheet_url = Fitzgerald().datasheet_url(lxml)
            #     review = Fitzgerald().review(lxml)
            image_qty = Fitzgerald().image_qty(lxml)
            #     citations = Fitzgerald().citations(lxml)
            synonyms = Fitzgerald().synonyms(lxml)
            #     conjugate = Fitzgerald().conjugate(lxml)
            species_reactivity = Fitzgerald().species_reactivity(lxml)
            note = Fitzgerald().note(lxml)

            #     sub_citations = Fitzgerald().sub_citations(citations, lxml)
            sub_images = Fitzgerald().sub_images(image_qty, lxml)
            sub_price = Fitzgerald().sub_price(lxml)
            # print(sub_price)
        else:
            r.lpush("fitzgerald_detail", extract)
            print("html is none")
            continue
        new_detail = Detail(
            Brand=brand,
            Kit_Type=kit_type,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Detail_url=detail_url,
            # Tests=tests,
            # Assay_type=assay_type,
            # Detection_Method=detection_method,
            # Sample_type=sample_type,
            # Assay_length=assay_length,
            # Sensitivity=sensitivity,
            # Assay_range=assay_range,
            # Specificity=specificity,
            # Target_Protein=target_protein,
            # GeneId=geneid,
            # SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            # Review=str(review),
            Image_qty=image_qty,
            # Citations=citations,
            Synonyms=synonyms,
            # Conjugate=conjugate,
            Species_Reactivity=species_reactivity,
            Note=note,
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
                    Catalog_Number=catalog_number, Size=sub_s, Price=sub_p
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("fitzgerald_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
