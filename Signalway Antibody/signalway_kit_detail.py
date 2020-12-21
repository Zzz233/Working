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
import pandas as pd

Base = declarative_base()


class Detail(Base):
    __tablename__ = "signalway_kit_detail"

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
    __tablename__ = "signalway_kit_citations"

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
    __tablename__ = "signalway_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "signalway_kit_price"

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


class Signalway(object):
    headers = {
        # "Host": "www.abbexa.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        # "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        # "Accept-Encoding": "gzip, deflate, br",
        # "Connection": "keep-alive",
        # "Referer": "https://www.abbexa.com/products/elisa-kits",
        # "Cookie": "PHPSESSID=90554d0188c8e2a7fa47551bdc3065c6; language=en; currency=USD; _gcl_au=1.1.678391277.1608168886; _ga=GA1.2.61988102.1608168887; _gid=GA1.2.713055285.1608168887; _gat_gtag_UA_41647028_1=1",
        # "Upgrade-Insecure-Requests": "1",
        # "Pragma": "no-cache",
        # "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@class="detail"]')[0]
        return content

    def brand(self):
        return "signalway"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        catnum = (
            html.xpath('.//div[@class="detail-top "]/h2/text()')[0]
            .split(" #")[-1]
            .strip()
        )
        return catnum

    def product_name(self, html):
        try:
            name = html.xpath(
                './/span[contains(text(), "Product Name")]/following-sibling::i[1]/text()'
            )[0].strip()
            return name
        except Exception:
            name = None

    def detail_url(self, url):
        return url

    def tests(self):  # 48T/96T 使用次数 规格
        return None

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/td[contains(text(),"ELISA Type")]/following-sibling::td/a/text()'
            )[0].strip()
            return assay_type
        except Exception:
            return None

    def detection_method(self, html):
        try:
            detection_method_text = html.xpath(
                './/td[contains(text(),"ELISA Detection")]/following-sibling::td/text()'
            )
            detection_method = "".join(i for i in detection_method_text).strip()
            return detection_method
        except Exception:
            return None

    def sample_type(self, html):
        try:
            sample_type_list = html.xpath(
                './/div[@class="content"][@style="display:none;"]//p/text()'
            )
            for item in sample_type_list:
                if "Sample Type: " in item:
                    sample_type = item.split("Sample Type: ")[-1]
                    if len(sample_type) > 0:
                        return sample_type
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
            sensitivity_list = html.xpath(
                './/div[@class="content"][@style="display:none;"]//p/text()'
            )
            for item in sensitivity_list:
                if "Sensitivity: " in item:
                    sensitivity = item.split("Sensitivity: ")[-1]
                    if len(sensitivity) > 0:
                        return sensitivity
                    else:
                        return None
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
            target_protein = html.xpath(
                './/span[contains(text(), "Target Name")]/following-sibling::i[1]/text()'
            )[0].strip()
            return target_protein
        except Exception:
            return None

    def geneid(self, html):
        try:
            geneid = html.xpath(
                './/td[contains(text(),"GeneID")]/following-sibling::td/a/text()'
            )[0].strip()
            return geneid
        except Exception:
            return None

    def swissprot(self, html):
        try:
            swissprot = (
                html.xpath(
                    './/span[contains(text(), "Accession No.")]/following-sibling::i[1]/text()'
                )[0]
                .strip()
                .split("Swiss-Prot: ")[-1]
            )
            return swissprot
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            datasheet_url = (
                "https://www.sabbiotech.com.cn/"
                + html.xpath('.//a[@class="btn-pdf"]/@href')[0].strip()
            )
            return datasheet_url
        except Exception:
            return None

    def review(self, html):
        return 0

    def image_qty(self, html):
        return 0

    def citations(self, html):
        try:
            citations = len(
                html.xpath('.//span[@class="list_item1" or @class="list_item"]')
            )
            return citations
        except Exception:
            return None

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/span[@class="altive"][contains(text(), "Alternative Names")]/following-sibling::i[1]/text()'
            )[0].strip()
            return synonyms
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
            species_reactivity = html.xpath(
                './/span[contains(text(), "Species Reactivity")]/following-sibling::i[1]/text()'
            )[0].strip()
            return species_reactivity
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
            citations_text = html.xpath(
                './/span[@class="list_item1" or @class="list_item"]'
            )
            for item in citations_text:
                text = item.xpath(".//text()")
                title = "".join(i for i in text).split("  PMID: ")[0].strip()
                pmid = item.xpath(".//strong/a/text()")[0].split("PMID:")[1].strip()
                link = item.xpath(".//strong/a/@href")[0].strip()
                results.append([pmid, title, link])
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
            trs = html.xpath('.//table[@class="table table-borderless"]/tbody/tr')
        except Exception:
            return results
        for item in trs:
            sub_num = item.xpath(".//td//text()")[0].strip()
            sub_siz = item.xpath(".//td//text()")[1].strip()
            sub_pri = item.xpath(".//td//text()")[2].strip()
            results.append([sub_num, sub_siz, sub_pri])
        return results


if __name__ == "__main__":
    # ! url去掉?limit=100
    # for i in range(1):
    while r.exists("signal_kit_detail"):
        extract = r.rpop("signal_kit_detail")
        # extract = "https://www.sabbiotech.com.cn/g-169398-Porcine-Insulin-ELISA-Kit-EK18103.html"
        print(extract)
        try:
            lxml = Signalway().format(extract)
        except Exception as e:
            print(e)
            r.lpush("signal_kit_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = Signalway().brand()
            kit_type = Signalway().kit_type()
            catalog_number = Signalway().catalog_number(lxml)
            product_name = Signalway().product_name(lxml)
            detail_url = Signalway().detail_url(extract)
            # tests = Signalway().tests(lxml)
            # assay_type = Signalway().assay_type(lxml)
            # detection_method = Signalway().detection_method(lxml)
            sample_type = Signalway().sample_type(lxml)
            assay_length = Signalway().assay_length(lxml)
            sensitivity = Signalway().sensitivity(lxml)
            assay_range = Signalway().assay_range(lxml)
            # specificity = Signalway().specificity(lxml)
            target_protein = Signalway().target_protein(lxml)
            # geneid = Signalway().geneid(lxml)
            swissprot = Signalway().swissprot(lxml)
            datasheet_url = Signalway().datasheet_url(lxml)
            review = Signalway().review(lxml)
            image_qty = Signalway().image_qty(lxml)
            citations = Signalway().citations(lxml)
            synonyms = Signalway().synonyms(lxml)
            # conjugate = Signalway().conjugate(lxml)
            species_reactivity = Signalway().species_reactivity(lxml)
            # note = Signalway().note(lxml)

            sub_citations = Signalway().sub_citations(citations, lxml)
            # sub_images = Signalway().sub_images(image_qty, lxml)
            sub_price = Signalway().sub_price(lxml)
            # print(swissprot)

        else:
            r.lpush("signal_kit_detail", extract)
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
            Sample_type=sample_type,
            # Assay_length=assay_length,
            Sensitivity=sensitivity,
            Assay_range=assay_range,
            # Specificity=specificity,
            Target_Protein=target_protein,
            # GeneId=geneid,
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
            objects_sub_citations = []
            for sub in sub_citations:
                sub_pid = sub[0]
                sub_tit = sub[1]
                sub_pul = sub[2]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pid,
                    Article_title=sub_tit,
                    Pubmed_url=sub_pul,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

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
            r.lpush("signal_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
