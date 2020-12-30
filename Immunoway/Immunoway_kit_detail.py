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
    __tablename__ = "immunoway_kit_detail"

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
    __tablename__ = "immunoway_kit_citations"

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
    __tablename__ = "immunoway_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "immunoway_kit_price"

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


class Immunoway(object):
    headers = {
        # "Host": "www.assaybiotechnology.com",
        # "Connection": "keep-alive",
        # "Cache-Control": "max-age=0",
        # "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        # "Sec-Fetch-Site": "none",
        # "Sec-Fetch-Mode": "navigate",
        # "Sec-Fetch-User": "?1",
        # "Sec-Fetch-Dest": "document",
        # "Accept-Encoding": "gzip, deflate, br",
        # "Accept-Language": "zh-CN,zh;q=0.9",
        # "Cookie": "language=en-gb; _ga=GA1.2.1623526287.1609210122; _gid=GA1.2.1522359955.1609210122; language=zh-cn; currency=CNY; OCSESSID=a07d1684ca5e07bbe4ecc91b00; _gat_gtag_UA_62686304_1=1",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=45)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath("//body")[0]
        return content

    def brand(self):
        return "immunoway"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        mora = html.xpath('.//span[contains(text(), "Catalog No.：")]/../text()')[
            0
        ].strip()
        return mora

    def product_name(self, html):
        mora = html.xpath('.//div[@class="attrPrdt" or @class="attrPrdt2"]/p/text()')[
            0
        ].strip()
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
                './/li[@class="title"][contains(text(), "Specificity：")]/following-sibling::li[@class="text"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def detection_method(self, html):
        try:
            mora = html.xpath(
                './/li[@class="title"][contains(text(), "Detection Method：")]/following-sibling::li[@class="text"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def sample_type(self, html):
        try:
            mora = html.xpath(
                './/li[@class="title"][contains(text(), "Specificity：")]/following-sibling::li[@class="text"]/text()'
            )[0].strip()
            return mora
        except Exception:
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
                './/li[@class="title"][contains(text(), "Range Of Detection：")]/following-sibling::li[@class="text"]/text()'
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
                './/li[@class="title"][contains(text(), "Protein Name：")]/following-sibling::li[@class="text"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def geneid(self, html):
        try:
            mora = html.xpath(
                './/li[@class="title"][contains(text(), "Human Gene Id：")]/following-sibling::li[@class="text"]/a/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora = html.xpath(
                './/li[@class="title"][contains(text(), "Human Swiss Prot No：")]/following-sibling::li[@class="text"]/a/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            mora = (
                "http://www.immunoway.com"
                + html.xpath('.//a[contains(text(), "Data Sheet")][@href]/@href')[
                    0
                ].strip()
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
                    './/a[@name="images"]/following-sibling::ul[@class="imgAttr"]'
                )
            )
            return mora
        except Exception:
            return 0

    def citations(self, html):
        try:
            mora = (
                html.xpath('.//a[@style][contains(text(), "References")]/text()')[0]
                .replace("References ( ", "")
                .replace(" )", "")
            )
            return mora
        except Exception:
            return 0

    def synonyms(self, html):
        try:
            mora = html.xpath(
                './/li[@class="title"][contains(text(), "Other Name：")]/following-sibling::li[@class="text"]/text()'
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
            mora = mora = html.xpath(
                './/span[contains(text(), "Reactivity：")]/../text()'
            )[0].strip()
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
                citations_text = html.xpath(
                    './/div[@id="tabs-2"]/p[@style="backgroup:#f2f2f2;"]'
                )
                for item in citations_text:
                    title = item.xpath("./a[@onclick]/text()")[0].strip()
                    try:
                        link = (
                            item.xpath("./a[@onclick]/@onclick")[0]
                            .replace("window.open('", "")
                            .replace("')", "")
                            .strip()
                        )
                    except Exception:
                        link = None
                    if "www.ncbi.nlm.nih.gov/pubmed/" in link:
                        pmid = link.split(".gov/pubmed/")[-1].strip()
                    else:
                        pmid = None
                    results.append([title, link, pmid])
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
                './/a[@name="images"]/following-sibling::ul[@class="imgAttr"]'
            )
            for item in imgs:
                try:
                    img_url = (
                        "http://www.immunoway.com"
                        + item.xpath('./li[@class="img"]/img/@src')[0].strip()
                    )
                    try:
                        img_des = item.xpath('./li[@class="attr"]/text()')[0].strip()
                    except Exception:
                        img_des = None
                    results.append([img_url, img_des])
                except Exception:
                    pass
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            div = html.xpath('.//div[@class="divAttr"]')[0]
        except Exception:
            return results
        sub_siz = div.xpath(
            './ul[@class="ul02"]/li[@class="title"][contains(text(), "Size")]/following-sibling::li[@class="text"]/text()'
        )[0].strip()
        sub_pri = (
            "$"
            + div.xpath(
                './ul[@class="ul02"]/li[@class="title"][contains(text(), "Price$：")]/following-sibling::li[@class="text"]/text()'
            )[0].strip()
        )
        results.append([sub_siz, sub_pri])
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("immunoway_kit_detail"):
        extract = r.rpop("immunoway_kit_detail")
        # extract = "http://www.immunoway.com/Home/22/KE1372"
        print(extract)
        try:
            lxml = Immunoway().format(extract)
        except Exception as e:
            print(e)
            r.lpush("immunoway_kit_detail", extract)
            print("sleeping...")
            time.sleep(30)
            continue
        if lxml is not None:
            brand = Immunoway().brand()
            kit_type = Immunoway().kit_type()
            catalog_number = Immunoway().catalog_number(lxml)
            product_name = Immunoway().product_name(lxml)
            detail_url = Immunoway().detail_url(extract)
            #     tests = Immunoway().tests(lxml)
            # assay_type = Immunoway().assay_type(lxml)
            detection_method = Immunoway().detection_method(lxml)
            sample_type = Immunoway().sample_type(lxml)
            #     assay_length = Immunoway().assay_length(lxml)
            #     sensitivity = Immunoway().sensitivity(lxml)
            assay_range = Immunoway().assay_range(lxml)
            #     specificity = Immunoway().specificity(lxml)
            target_protein = Immunoway().target_protein(lxml)
            geneid = Immunoway().geneid(lxml)
            swissprot = Immunoway().swissprot(lxml)
            datasheet_url = Immunoway().datasheet_url(lxml)
            review = Immunoway().review(lxml)
            image_qty = Immunoway().image_qty(lxml)
            citations = Immunoway().citations(lxml)
            synonyms = Immunoway().synonyms(lxml)
            #     conjugate = Immunoway().conjugate(lxml)
            species_reactivity = Immunoway().species_reactivity(lxml)
            #     note = Immunoway().note(lxml)

            sub_citations = Immunoway().sub_citations(citations, lxml)
            sub_images = Immunoway().sub_images(image_qty, lxml)
            sub_price = Immunoway().sub_price(lxml)
            print(sub_price)

        else:
            r.lpush("XXXX", extract)
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
            Detection_Method=detection_method,
            Sample_type=sample_type,
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

        if sub_citations:
            # title, link, pmid
            objects_sub_citations = []
            for sub in sub_citations:
                sub_tit = sub[0]
                sub_link = sub[1]
                sub_pmid = sub[2]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    Article_title=sub_tit,
                    Pubmed_url=sub_link,
                    PMID=sub_pmid,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

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
            r.lpush("immunoway_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
