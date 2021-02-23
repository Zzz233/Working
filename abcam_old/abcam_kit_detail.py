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
    __tablename__ = "abcam_elisa_kit_detail"

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
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "abcam_elisa_kit_citations"

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
    __tablename__ = "abcam_elisa_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "abcam_elisa_kit_price"

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


class Abcam(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                content = html_lxml.xpath('//div[@id="frame"]')[
                    0
                ]  # ! <xpath lxml>  None
            except Exception:
                content = None
        return content

    def brand(self):
        return "abcam_old"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        try:
            catnum = html.xpath(
                './/div[@id="datasheet-header-container"]/@data-product-code'
            )[0].strip()
        except Exception:
            catnum = None
        return catnum

    def product_name(self, html):
        try:
            name = html.xpath('.//h1[@class="title"]/text()')[0].strip()
        except Exception:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        # ajax
        try:
            tests = html.xpath(
                './/th[contains(text(), "组件")]/following-sibling::th/text()'
            )[0].strip()
        except Exception:
            tests = None
        return tests

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/h3[@class="name"][contains(text(), "检测类型")]/following-sibling::div[@class="value"]/text()'
            )[0].strip()
        except Exception:
            assay_type = None
        return assay_type

    def detection_method(self, html):
        try:
            detection_method = html.xpath(
                './/h3[@class="name"][contains(text(), "检测方法")]/following-sibling::div[@class="value"]/text()'
            )[0].strip()
        except Exception:
            detection_method = None
        return detection_method

    def sample_type(self, html):
        try:
            sample_type = html.xpath(
                './/h3[@class="name"][contains(text(), "样品类型")]/following-sibling::div[@class="value"]/text()'
            )[0].strip()
        except Exception:
            sample_type = None
        return sample_type

    def assay_length(self, html):
        try:
            assay_length = (
                html.xpath(
                    './/h3[@class="name"][contains(text(), "检测时间")]/following-sibling::div[@class="value"]/text()'
                )[0]
                .replace("\r\n", "")
                .replace(" ", "")
            )
        except Exception:
            assay_length = None
        return assay_length

    def sensitivity(self, html):
        try:
            sensitivity = html.xpath(
                './/h3[@class="name"][contains(text(), "灵敏度")]/following-sibling::div[@class="value"]/text()'
            )[0].strip()
        except Exception:
            sensitivity = None
        return sensitivity

    def assay_range(self, html):
        try:
            assay_range = (
                html.xpath(
                    './/h3[@class="name"][contains(text(), "范围")]/following-sibling::div[@class="value"]/text()'
                )[0]
                .replace("\r\n", "")
                .replace(" ", "")
            )
        except Exception:
            assay_range = None
        return assay_range

    def specificity(self):
        return None

    def target_protein(self):
        return None

    def geneid(self, html):
        try:
            geneid = (
                html.xpath(
                    './/a[@class="dsIcon extLink"][contains(text(), "Entrez Gene:")]/text()'
                )[0]
                .replace("\r\n", "")
                .replace("Entrez Gene:", "")
                .strip()
            )
        except Exception:
            geneid = None
        return geneid

    def swissprot(self, html):
        try:
            swissprot = (
                html.xpath(
                    './/a[@class="dsIcon extLink"][contains(text(), "SwissProt:")]/text()'
                )[0]
                .replace("\r\n", "")
                .replace("SwissProt:", "")
                .strip()
            )
        except Exception:
            swissprot = None
        return swissprot

    def datasheet_url(self, html):
        try:
            datasheet_url = (
                "https://www.abcam.cn"
                + html.xpath(
                    './/ul[@class="pdf-links"]/li/a/span[contains(text(), "Datasheet")]/../@href'
                )[0].strip()
            )
        except Exception:
            datasheet_url = None
        return datasheet_url

    def review(self, html):
        try:
            review = len(html.xpath('.//div[@class="review"]'))
        except Exception:
            review = 0
        return review

    def image_qty(self, html):
        try:
            image_qty = len(html.xpath('.//ul[@class="images"]/li[@id]'))
        except Exception:
            image_qty = 0
        return image_qty

    def citations(self, html):
        try:
            citations = int(
                html.xpath('.//h2[@class="h3"][contains(text(),"文献")]/span/text()')[0]
                .replace("(", "")
                .replace(")", "")
                .strip()
            )
        except Exception:
            citations = 0
        return citations

    def synonyms(self, html):
        try:
            text_list = html.xpath(
                './/h3[@class="name"][contains(text(), "别名")]/following-sibling::div[@class="value"]//li/text()'
            )
            synonyms = "|".join(i for i in text_list)
        except Exception:
            synonyms = None
        return synonyms

    def note(self, html):
        return None

    # ======================================================================== #
    # Citations表
    def sub_citations(self, html):
        return None

    # ======================================================================== #
    # Images表
    def sub_images(self, html):
        results = []
        try:
            lis = html.xpath('.//ul[@class="images"]/li[@id]')
        except Exception:
            return results
        for li in lis:
            try:
                img_url = li.xpath('./div[@class="column image gallery"]/a/@href')[
                    0
                ].strip()
            except Exception:
                continue
            try:
                dec_list = li.xpath('.//div[@class="column description"]//text()')
                img_dec = "".join(i for i in dec_list)
            except Exception:
                img_dec = None
            results.append([img_url, img_dec])
        return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        pass


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("abcam_detail"):
        extract = r.rpop("abcam_detail")
        # extract = "https://www.abcam.cn/human-tnf-alpha-elisa-kit-ab181421.html"
        print(extract)
        try:
            lxml = Abcam().format(extract)
            # print(lxml)
        except Exception:
            r.lpush("abcam_detail", extract)
            continue
        if lxml is not None:
            brand = Abcam().brand()
            kit_type = Abcam().kit_type()
            catalog_number = Abcam().catalog_number(lxml)
            product_name = Abcam().product_name(lxml)
            detail_url = Abcam().detail_url(extract)
            tests = Abcam().tests(lxml)
            assay_type = Abcam().assay_type(lxml)
            detection_method = Abcam().detection_method(lxml)
            sample_type = Abcam().sample_type(lxml)
            assay_length = Abcam().assay_length(lxml)
            sensitivity = Abcam().sensitivity(lxml)
            assay_range = Abcam().assay_range(lxml)
            # specificity = Abcam().specificity(lxml)
            # target_protein = Abcam().target_protein(lxml)
            geneid = Abcam().geneid(lxml)
            swissprot = Abcam().swissprot(lxml)
            datasheet_url = Abcam().datasheet_url(lxml)
            review = Abcam().review(lxml)
            image_qty = Abcam().image_qty(lxml)
            citations = Abcam().citations(lxml)
            synonyms = Abcam().synonyms(lxml)
            # note = Abcam().note(lxml)
            # sub_citations = Abcam().sub_citations(lxml)
            sub_images = Abcam().sub_images(lxml)
            # print(sub_images)
        else:
            r.lpush("abcam_detail", extract)
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
            # target_protein=target_protein,
            GeneId=geneid,
            SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            Review=str(review),
            Image_qty=image_qty,
            Citations=str(citations),
            Synonyms=synonyms,
            # Note=note,
        )
        session.add(new_detail)

        # if sub_citations:
        #     objects_sub_citations = []
        #     for sub in sub_citations:
        #         sub_pid = sub[0]
        #         sub_spe = sub[1]
        #         sub_tit = sub[2]
        #         sub_sam = sub[3]
        #         sub_pul = sub[4]

        #         new_citations = Citations(
        #             Catalog_Number=catalog_number,
        #             PMID=sub_pid,
        #             Species=sub_spe,
        #             Article_title=sub_tit,
        #             Sample_type=sub_sam,
        #             Pubmed_url=sub_pul,
        #         )
        #         objects_sub_citations.append(new_citations)
        #     session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = sub[0]
                dec = sub[1]

                new_images = Images(
                    Catalog_Number=catalog_number, Image_url=img, Image_description=dec
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("abcam_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(0.5, 1.0))
