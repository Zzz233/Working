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
    __tablename__ = "novus_kit_detail"

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
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "novus_kit_citations"

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
    __tablename__ = "novus_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "novus_kit_price"

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


class Novus(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                content = html_lxml.xpath('//div[@id="caspertesting"]')[
                    0
                ]  # ! <xpath lxml>  None
            except Exception:
                content = None
        return content

    def brand(self):
        return "novusbio"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, catnum):
        # from redis
        return catnum

    def product_name(self, html):
        try:
            name_list = html.xpath(".//h1//text()")
            name = "".join(i for i in name_list).strip()
        except Exception:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        return None

    def assay_type(self, html):
        return None

    def detection_method(self, html):
        return None

    def sample_type(self, html):
        return None

    def assay_length(self, html):
        return None

    def sensitivity(self, html):
        return None

    def assay_range(self, html):
        return None

    def specificity(self, html):
        try:
            specificity_list = html.xpath(
                './/tr[@class="lined"]/td/strong[contains(text(), "Specificity")]/../following-sibling::td[1]/div//text()'
            )
            specificity = "".join(i for i in specificity_list).strip()
        except Exception:
            specificity = None
        return specificity

    def target_protein(self):
        return None

    def geneid(self, html):
        return None

    def swissprot(self, html):
        return None

    def datasheet_url(self, html):
        try:
            datasheet_url = html.xpath('.//a[@class="datasheet-download"]/@href')[
                0
            ].strip()
        except Exception:
            datasheet_url = None
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
            image_qty = len(
                html.xpath('.//div[@class="cycle-slideshow slideshows"]//img')
            )
        except Exception:
            image_qty = 0
        return image_qty

    def citations(self, html):
        try:
            citations = len(
                html.xpath(
                    './/table[@id="publications_list_table"]/tbody/tr[@class="lined revfil firstten" or @class="lined revfil hidden"]'
                )
            )
        except Exception:
            citations = 0
        return citations

    def synonyms(self, html):
        try:
            synonyms_list = html.xpath(
                './/h2[@class="greyHead2"][contains(text(), "Alternate Names for ")]/following-sibling::div[1][@class="ds_info"]/div[@class="information-list"]/ul/li/text()'
            )
            synonyms = ",".join(i for i in synonyms_list)
        except Exception:
            synonyms = None
        return synonyms

    def conjugate(self, html):
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
            lis = html.xpath(
                './/div[@id="pdpManualCitations"]/ul/li[@class="OneLinkNoTx"]'
            )
            for li in lis:
                pm_url = li.xpath("./a/@href")[0].strip()
                if "www.ncbi.nlm.nih.gov/pubmed/" in pm_url:
                    pmid = pm_url.split("gov/pubmed/")[1].strip()
                else:
                    pmid = None
                title_list = li.xpath("./a//text()")
                title = "".join(i for i in title_list)
                results.append([pmid, title, pm_url])
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            divs = html.xpath(
                './/div[@data-app-id][@class][@data-figure-type="image"][@data-product-id]/div[@class="imgWrapper"]'
            )
            for div in divs:
                img_url = div.xpath("./img[@data-lazy]/@data-lazy")[0].strip()
                img_dec_list = div.xpath("./../p//text()")
                img_dec = "".join(i for i in img_dec_list)
                results.append([img_url, img_dec])
        return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            trs = html.xpath(".//tr[@data-skuid]")
        except Exception:
            return results
        for tr in trs:
            sub_cata = tr.xpath("./@data-skuid")[0].strip()
            sub_size_list = tr.xpath('./td[@class="product-size"]//text()')
            sub_size = "".join(i for i in sub_size_list).strip()
            results.append([sub_cata, sub_size])
        return results


if __name__ == "__main__":
    for i in range(1):
        # while r.exists("cellsignal_detail"):
        # extract = r.rpop("cellsignal_detail")
        extract = (
            "https://www.novusbio.com/products/human-il-6-quantikine-elisa-kit_d6050"
        )
        catano = "M6000B"
        print(extract)
        try:
            lxml = Novus().format(extract)
            print(lxml)
        except Exception:
            # r.lpush("cellsignal_detail", extract)
            continue
        if lxml is not None:
            brand = Novus().brand()
            kit_type = Novus().kit_type()
            catalog_number = Novus().catalog_number(catano)
            product_name = Novus().product_name(lxml)
            detail_url = Novus().detail_url(extract)
            # tests = Novus().tests(lxml)
            # assay_type = Novus().assay_type(lxml)
            # detection_method = Novus().detection_method(lxml)
            # sample_type = Novus().sample_type(lxml)
            # assay_length = Novus().assay_length(lxml)
            # sensitivity = Novus().sensitivity(lxml)
            # assay_range = Novus().assay_range(lxml)
            specificity = Novus().specificity(lxml)
            # target_protein = Novus().target_protein(lxml)
            # geneid = Novus().geneid(lxml)
            # swissprot = Novus().swissprot(lxml)
            datasheet_url = Novus().datasheet_url(lxml)
            review = Novus().review(lxml)
            image_qty = Novus().image_qty(lxml)
            citations = Novus().citations(lxml)
            synonyms = Novus().synonyms(lxml)
            print(synonyms)
            # note = Novus().note(lxml)
            # sub_citations = Novus().sub_citations(citations, lxml)
            # sub_images = Novus().sub_images(image_qty, lxml)
            # sub_price = Novus().sub_price(lxml)

        # else:
        #     r.lpush("cellsignal_detail", extract)
        #     continue
        # new_detail = Detail(
        #     Brand=brand,
        #     Kit_Type=kit_type,
        #     Catalog_Number=catalog_number,
        #     Product_Name=product_name,
        #     Detail_url=detail_url,
        #     Tests=tests,
        #     # Assay_type=assay_type,
        #     # Detection_Method=detection_method,
        #     # Sample_type=sample_type,
        #     # Assay_length=assay_length,
        #     # Sensitivity=sensitivity,
        #     # Assay_range=assay_range,
        #     Specificity=specificity,
        #     # target_protein=target_protein,
        #     GeneId=geneid,
        #     SwissProt=swissprot,
        #     DataSheet_URL=datasheet_url,
        #     # Review=str(review),
        #     Image_qty=image_qty,
        #     Citations=citations,
        #     # Synonyms=synonyms,
        #     # Conjugate=conjugate,
        #     # Note=note,
        # )
        # session.add(new_detail)

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

        # if sub_images:
        #     objects_sub_images = []
        #     for sub in sub_images:
        #         img = sub[0]
        #         dec = sub[1]

        #         new_images = Images(
        #             Catalog_Number=catalog_number, Image_url=img, Image_description=dec
        #         )
        #         objects_sub_images.append(new_images)
        #     session.bulk_save_objects(objects_sub_images)

        # if sub_price:
        #     objects_sub_price = []
        #     for sub in sub_price:
        #         sub_c = sub[0]
        #         suc_s = sub[1]

        #         new_price = Price(
        #             Catalog_Number=catalog_number, sub_Catalog_Number=sub_c, Size=suc_s
        #         )
        #         objects_sub_price.append(new_price)
        #     session.bulk_save_objects(objects_sub_price)

        # try:
        #     session.commit()
        #     session.close()
        #     print("done")
        # except Exception as e:
        #     r.lpush("cellsignal_detail", extract)
        #     session.rollback()
        #     print(e)
        # time.sleep(random.uniform(0.5, 1.0))
