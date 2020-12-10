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

Base = declarative_base()


class Detail(Base):
    __tablename__ = "origene_kit_detail"

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
    Citations = Column(String(20), nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Conjugate = Column(String(200), nullable=True, comment="")
    Species_Reactivity = Column(String(200), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "origene_kit_citations"

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
    __tablename__ = "origene_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "origene_kit_price"

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


class OriGene(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            html_lxml = etree.HTML(resp.text)
            try:
                content = html_lxml.xpath('//section[@id="product"]')[
                    0
                ]  # ! <xpath lxml>  None
            except Exception:
                content = None
        return content

    def brand(self):
        return "origene"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        catnum = (
            html.xpath('.//h2[@class="sku"]/text()')[0].replace("CAT＃: ", "").strip()
        )
        return catnum

    def product_name(self, html):
        try:
            name_list = html.xpath('.//h1[@class="name"]/text()')
            name = "".join(i for i in name_list).strip()
        except Exception:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        # Format
        try:
            tests = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Format")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return tests

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Assay Type")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_type

    def detection_method(self, html):
        # Signal
        try:
            detection_method = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Signal")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return detection_method

    def sample_type(self, html):
        # Suitable Sample:
        try:
            sample_type = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Sample Type")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return sample_type

    def assay_length(self, html):
        try:
            assay_length = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Assay Length")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_length

    def sensitivity(self, html):
        try:
            sensitivity = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Sensitivity")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return sensitivity

    def assay_range(self, html):
        # Curve Range
        try:
            assay_range = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Curve Range")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return assay_range

    def specificity(self, html):
        try:
            specificity = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Specificity")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return specificity

    def target_protein(self, html):
        # Gene Symbol
        try:
            target_protein = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Gene Symbol")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return target_protein

    def geneid(self, html):
        try:
            geneid = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Gene ID")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return geneid

    def swissprot(self, html):
        return None

    def datasheet_url(self, html):
        try:
            datasheet_url = html.xpath('.//a[@class="datasheet-link icon-link"]/@href')[
                0
            ].strip()
        except Exception:
            return None
        return datasheet_url

    def review(self, html):
        try:
            review = int(
                html.xpath(
                    './/h2[@class="greyHead2"][contains(text(), "Reviews for")]/span/text()'
                )[0]
                .strip()
                .split("(")[-1]
                .split(")")[0]
            )
        except Exception:
            review = 0
        return review

    def image_qty(self, html):
        try:
            image_qty = len(html.xpath('.//div[@class="text-center mb-2"]/img'))
        except Exception:
            image_qty = 0
        return image_qty

    def citations(self, html):
        try:
            citations = len(
                html.xpath(
                    './/table[@class="nav-table table table-striped table-bordered table-hover"]/tbody/tr'
                )
            )
        except Exception:
            citations = 0
        return citations

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/b[contains(text(), "Gene Alias:")]/../following-sibling::li[@class="black11_a_underline sub_content"]/text()'
            )[0].strip()
        except Exception:
            synonyms = None
        return synonyms

    def conjugate(self, html):
        try:
            conjugate = html.xpath(
                './/b[contains(text(), "Label:")]/../following-sibling::li[@class="black11_a_underline sub_content"]/text()'
            )[0].strip()
        except Exception:
            conjugate = None
        return conjugate

    def species_reactivity(self, html):
        try:
            species_reactivity = html.xpath(
                './/tr[@class="attribute"]/td[contains(text(), "Reactivity")]/following-sibling::td/text()'
            )[0].strip()
        except Exception:
            return None
        return species_reactivity

    def note(self, html):
        return None

    # ======================================================================== #
    # Citations表
    def sub_citations(self, citations, html):
        results = []
        if citations == 0:
            return results
        else:
            trs = html.xpath(
                './/table[@class="nav-table table table-striped table-bordered table-hover"]/tbody/tr'
            )
            for tr in trs:
                try:
                    pmid = (
                        tr.xpath('.//span[contains(text(), "PubMed ID ")]/text()')[0]
                        .split("PubMed ID ")[-1]
                        .strip()
                    )
                except Exception:
                    pmid = None
                title_text = tr.xpath(".//span//text()")
                title = "".join(i for i in title_text)
                try:
                    pm_url = tr.xpath(".//a/@href")[0].strip()
                except Exception:
                    pm_url = None
                results.append([pmid, title, pm_url])
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            imgs = html.xpath(
                './/div[@class="text-center mb-2"]/img[@class="img-fluid"]'
            )
            for img in imgs:
                try:
                    img_des = img.xpath("./../following-sibling::div/text()")[0].strip()
                except Exception:
                    img_des = None
                try:
                    img_url = img.xpath("./@src")[0].strip()
                except Exception:
                    img_url = 0
                results.append([img_url, img_des])
        return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            div = html.xpath(
                './/div[@class="checkout-container checkout-data-container mb-3"]'
            )[0]
            # print(div)
        except Exception:
            return results
        sub_size = div.xpath(".//span[@data-product-option-value]/text()")[0].strip()
        sub_price = div.xpath('.//div[@class="price h2"]/p/text()')[0].strip()
        results.append([sub_size, sub_price])
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("origene_kit_detail"):
        # single_data = r.rpop("novus_detail")
        # extract = single_data.split(",")[0]
        # catano = single_data.split(",")[1]
        extract = r.rpop("origene_kit_detail")
        # extract = "https://www.origene.com.cn/catalog/assay-kits/elisa-kits/ea100066/human-vegf-elisa-kit-96-well"
        print(extract)
        try:
            lxml = OriGene().format(extract)
            # print(lxml)
        except Exception as e:
            print(e)
            r.lpush("origene_kit_detail", extract)
            continue
        if lxml is not None:
            brand = OriGene().brand()
            kit_type = OriGene().kit_type()
            catalog_number = OriGene().catalog_number(lxml)
            product_name = OriGene().product_name(lxml)
            detail_url = OriGene().detail_url(extract)
            tests = OriGene().tests(lxml)
            assay_type = OriGene().assay_type(lxml)
            detection_method = OriGene().detection_method(lxml)
            sample_type = OriGene().sample_type(lxml)
            assay_length = OriGene().assay_length(lxml)
            sensitivity = OriGene().sensitivity(lxml)
            assay_range = OriGene().assay_range(lxml)
            specificity = OriGene().specificity(lxml)
            target_protein = OriGene().target_protein(lxml)
            geneid = OriGene().geneid(lxml)
            # swissprot = OriGene().swissprot(lxml)
            datasheet_url = OriGene().datasheet_url(lxml)
            # review = OriGene().review(lxml)
            image_qty = OriGene().image_qty(lxml)
            citations = OriGene().citations(lxml)
            # synonyms = OriGene().synonyms(lxml)
            # conjugate = OriGene().conjugate(lxml)
            species_reactivity = OriGene().species_reactivity(lxml)
            # note = OriGene().note(lxml)
            sub_citations = OriGene().sub_citations(citations, lxml)
            sub_images = OriGene().sub_images(image_qty, lxml)
            sub_price = OriGene().sub_price(lxml)
            # print(sub_price)

        else:
            r.lpush("origene_kit_detail", extract)
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
            # SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            # Review=str(review),
            Image_qty=image_qty,
            Citations=citations,
            # Synonyms=synonyms,
            # Conjugate=conjugate,
            Species_Reactivity=species_reactivity,
            # Note=note,
        )
        session.add(new_detail)

        if sub_citations:
            objects_sub_citations = []
            for sub in sub_citations:
                sub_pid = sub[0]
                sub_tit = sub[1]
                sub_pul = sub[2]
                # sub_species = sub[3]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pid,
                    Article_title=sub_tit,
                    Pubmed_url=sub_pul,
                    # Species=sub_species,
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
                suc_p = sub[1]

                new_price = Price(
                    Catalog_Number=catalog_number, Size=sub_s, Price=suc_p
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("origene_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 2.0))
