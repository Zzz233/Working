"""
Suitable Sample    sample type
Label conjugated 
Calibration Range    assay range
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

Base = declarative_base()


class Detail(Base):
    __tablename__ = "assaybiotech_kit_detail"

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
    __tablename__ = "assaybiotech_kit_citations"

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
    __tablename__ = "assaybiotech_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "assaybiotech_kit_price"

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


class Assaybiotech(object):
    headers = {
        "Host": "www.assaybiotechnology.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.assaybiotechnology.com/AhR-Colorimetric-Cell-Based-ELISA-Kit",
        "Connection": "keep-alive",
        # ; _ga=GA1.2.948301748.1609211738; _gid=GA1.2.1526946374.1609211738
        "Cookie": "language=en-gb; OCSESSID=f7b2a3a0f63b171c1d297ee0a3; language=zh-cn; currency=CNY",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath("//body")[0]
        return content

    def brand(self):
        return "assaybiotechnology"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        mora = html.xpath('.//div[@class="goods-sku"]/text()')[0].replace("货号 : ", "")
        return mora

    def product_name(self, html):
        mora = html.xpath('.//h1[@class="title"]/text()')[0].strip()
        return mora

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            mora = html.xpath(
                './/div[@class="item-left"][contains(text(), "Format")]/following-sibling::div[@class="item-right"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_type(self, html):
        try:
            mora = html.xpath(
                './/div[@class="item-left"][contains(text(), "Assay Type")]/following-sibling::div[@class="item-right"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def detection_method(self, html):
        try:
            mora = html.xpath(
                './/div[@class="item-left"][contains(text(), "Detection Method")]/following-sibling::div[@class="item-right"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def sample_type(self, html):
        if html.xpath('.//b[contains(text(), "Sample Type: ")]'):
            sample_type = html.xpath(
                './/b[contains(text(), "Sample Type: ")]/following-sibling::text()[1]'
            )
            return sample_type
        elif html.xpath('.//th[contains(text(), "Application Notes")]'):
            sample_type = html.xpath(
                './/th[contains(text(), "Application Notes")]/following-sibling::td/text()'
            )[0].strip()
            return sample_type
        else:
            return None

    def assay_length(self, html):
        try:
            assay_length = html.xpath(
                './/b[contains(text(), "Assay Time: ")]/following-sibling::text()'
            )[0].strip()
            return assay_length
        except Exception:
            return None

    def sensitivity(self, html):
        try:
            sensitivity = html.xpath(
                './/th[contains(text(), "Sensitivity")]/following-sibling::td[1]/text()'
            )[0].strip()
            return sensitivity
        except Exception:
            return None

    def assay_range(self, html):
        try:
            mora = html.xpath(
                './/div[@class="item-left"][contains(text(), "Dynamic Range")]/following-sibling::div[@class="item-right"]/text()'
            )[0].strip()
            return mora
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
                './/div[@class="item-left"][contains(text(), "TargetName")]/following-sibling::div[@class="item-right"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def geneid(self, html):
        try:
            mora = (
                html.xpath(
                    './/div[@class="item-right"][contains(text(), "Gene ID: ")]/text()'
                )[0]
                .split("Gene ID: ")[-1]
                .split(",")[0]
            )
            return mora
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora = (
                html.xpath(
                    './/div[@class="item-right"][contains(text(), "UniProt ID: ")]/text()'
                )[0]
                .split("UniProt ID: ")[-1]
                .split(",")[0]
            )
            return mora
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            mora = (
                "https://www.assaybiotechnology.com/"
                + html.xpath(
                    './/a[contains(text(), "产品说明书")][@class="datasheet"]/@href'
                )[0].strip()
            )
            return mora
        except Exception:
            return None

    def review(self, html):
        return 0

    def image_qty(self, html):
        try:
            mora = len(
                html.xpath(
                    './/div[@class="item-left"][contains(text(), "Application Images")]/following-sibling::div[@class="item-right"]/table/tbody/tr'
                )
            )
            return mora
        except Exception:
            return None

    def citations(self, html):
        return 0

    def synonyms(self, html):
        try:
            mora = html.xpath(
                './/div[@class="item-left"][contains(text(), "Synonyms")]/following-sibling::div[@class="item-right"]/text()'
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
            mora = (
                html.xpath(
                    './/div[@class="item"][contains(text(), "Reactivity")]/following-sibling::div[@class="item-txt"]/text()'
                )[0]
                .strip()
                .replace(" ", "")
            )
            return mora
        except Exception:
            return None

    def note(self, html):
        try:
            product_id = html.xpath('.//input[@name="product_id"]/@value')[0].strip()
            div = html.xpath('.//div[@class="options"]')[0]
            option_id = div.xpath(".//div[@id]/@id")[0].strip().replace("option-", "")
            inputs = div.xpath(".//label")
            size_value_list = []
            for input in inputs:
                size = input.xpath("./text()[1]")[0].strip()
                option_value = (
                    input.xpath("./@for")[0].strip().replace("option-value-", "")
                )
                size_value_list.append([size, option_value])
            note = {
                "product_id": product_id,
                "option_id": option_id,
                "array": size_value_list,
            }
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
            try:
                citations_text = html.xpath('.//div[@id="validation-images"]/p')
                for item in citations_text:
                    title = item.xpath("./text()")[0].strip()
                    results.append([title])
            except Exception:
                return results
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            imgs = html.xpath(
                './/div[@class="item-left"][contains(text(), "Application Images")]/following-sibling::div[@class="item-right"]/table/tbody/tr'
            )
            for item in imgs:
                try:
                    img_url = (
                        "https://www.assaybiotechnology.com/"
                        + item.xpath("./td/img/@src")[0].strip()
                    )
                    img_des = item.xpath("./td/span/text()")[0].strip()
                    results.append([img_url, img_des])
                except Exception:
                    pass
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            lis = html.xpath('.//li[@class="size-item" and not(@data-size="3+ Kits")]')
        except Exception:
            return results
        for item in lis:
            sub_siz = item.xpath("./@data-size")[0].strip()
            sub_pri = item.xpath("./@data-price")[0].strip()
            results.append([sub_siz, sub_pri])
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("assaybiotech_kit_detail"):
        extract = r.rpop("assaybiotech_kit_detail")
        # extract = "https://www.assaybiotechnology.com/14-3-3-betazeta-Colorimetric-Cell-Based-ELISA-Kit"
        print(extract)
        try:
            lxml = Assaybiotech().format(extract)
        except Exception as e:
            print(e)
            r.lpush("assaybiotech_kit_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = Assaybiotech().brand()
            kit_type = Assaybiotech().kit_type()
            catalog_number = Assaybiotech().catalog_number(lxml)
            product_name = Assaybiotech().product_name(lxml)
            detail_url = Assaybiotech().detail_url(extract)
            tests = Assaybiotech().tests(lxml)
            assay_type = Assaybiotech().assay_type(lxml)
            detection_method = Assaybiotech().detection_method(lxml)
            # sample_type = Assaybiotech().sample_type(lxml)
            # assay_length = Assaybiotech().assay_length(lxml)
            # sensitivity = Assaybiotech().sensitivity(lxml)
            assay_range = Assaybiotech().assay_range(lxml)
            # specificity = Assaybiotech().specificity(lxml)
            target_protein = Assaybiotech().target_protein(lxml)
            geneid = Assaybiotech().geneid(lxml)
            swissprot = Assaybiotech().swissprot(lxml)
            datasheet_url = Assaybiotech().datasheet_url(lxml)
            review = Assaybiotech().review(lxml)
            image_qty = Assaybiotech().image_qty(lxml)
            citations = Assaybiotech().citations(lxml)
            synonyms = Assaybiotech().synonyms(lxml)
            #     conjugate = Assaybiotech().conjugate(lxml)
            species_reactivity = Assaybiotech().species_reactivity(lxml)
            #     note = Assaybiotech().note(lxml)

            # sub_citations = Assaybiotech().sub_citations(citations, lxml)
            sub_images = Assaybiotech().sub_images(image_qty, lxml)
            sub_price = Assaybiotech().sub_price(lxml)
            print(sub_price)

        else:
            r.lpush("assaybiotech_kit_detail", extract)
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
            # Sample_type=sample_type,
            # Assay_length=assay_length,
            # Sensitivity=sensitivity,
            Assay_range=assay_range,
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
            # Note=str(note),
        )
        session.add(new_detail)

        # if sub_citations:
        #     objects_sub_citations = []
        #     for sub in sub_citations:
        #         sub_tit = sub[0]

        #         new_citations = Citations(
        #             Catalog_Number=catalog_number,
        #             Article_title=sub_tit,
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
            r.lpush("assaybiotech_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
