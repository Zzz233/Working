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
    __tablename__ = "biovision_kit_detail"

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
    __tablename__ = "biovision_kit_citations"

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
    __tablename__ = "biovision_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "biovision_kit_price"

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


class Biovision(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.biovision.com/acat1-human-elisa-kit.html",
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": "2250",
        "Origin": "https://www.biovision.com",
        "Connection": "keep-alive",
        "Cookie": "__cfduid=d3ca9450c45686fb2ebdac4c2460efeb21609723386; cf_clearance=f50603d7f3f124f1e4866965ee7bc25777fd8fdf-1609723386-0-150; frontend=gn2b6i1he2bgc9okgikva3rk11; frontend_cid=ku619S7ro4cwOWSt; external_no_cache=1; _ga=GA1.2.806556624.1609723384; _gid=GA1.2.128930719.1609723384; _gat=1; es_newssubscriber=1",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "TE": "Trailers",
    }
    data = {
        "r": "0e4c56b9744531e0698edac1bec5df6626bc6cb6-1609723382-0-AbZn/fDC6+L2QitlbPG6Ya2vmEZyIYtMVXl2ihNkOTxs+o73cdog+cW/h0FlSEtmIKpzgSJ4oJJVHa53EZBHvRVjxQCnC0TJTP/lGBXdjmdyNcypRfC/4nsOhbuNReBFu472rrYE68j9b9LEjkeyXuyotyMKWoUzD5+aPhWNh+A5K3yDHR3j0qO57hBJ+fcUrqJjD/G/08FAGpBrunQsW+YVY8RGAhn11kWkIEEsJgSnBCf6UkacusBT6UbhqQS8/tzD9ryY8Tr600Itw7CAahDB6zJ2K1phJrptrgX78w2txqlxAsZE669736c9cB7NQ12yfUvuEor68/+ZQLh7LvMgTOwYmaf8sUT61uWCrJr5VIvMMFfZhvMV0gdSYdF+FnBlREMeBmQjzqpykrFkiKgXA3qQoDHJEvi77Hateb/JlJhk/WyqwaM2Z7VYAqeJx4QvCwI6rkSv2MbjxH1XJs6HbJgc3oZfuXGx/L5lDAKdfctwH6qVsZjqvrvVwYIuzQPUVkDiziSIjMRJiUMlaCKKqru/4g053BFsslT6DPr9Nwq3cBLqrJLjwcEO3ZI6dyNSMVH4lAZSoBH++f1iUtTVu7cPIMLlWwaPfCvpnn20dFtUNuwxnTrUGAQtIU4D23L7R5j54Uec10jCBDbMkmej9oQrZ4WRmgPac+NqYipk5Q1YtKqFqBj0e+F6uApSqtzMpBej2ib0Gu2YDIXnOSwj+g/oOR7wws8PRT9/tMv5+VN1LdFLYi61B0EdiQC7inSNj7sY/+epqzRenWxyRNKN9uQN5TH1nCb1yxljvne2iwjjHAnn+tBu1WetX0Ly2nbLYMwFNAQee0gJ7Y1lEy84BrH+ckmooGZx0y1QAK68uRSm/o0qKoKluUOLwpXWbywIFzluVwVbcp2RHNkaWpAfOK3qHYdhREwNYAD/Jyz1u6NnDqfjgouGCmTyn/u0FyhKw59KET95O6DNz2b8SDIrIX0jQhlRhHoA+rdpiWEogmCBSe1vbYnrFEbmgx7EBKgBQHut7D4pwEX4j1htgp9tYQaeHDldYpiiw7GxvZOHj25JkIAZHFDhXUMrmEfet4QPmBYhQj9Kx56b+Rnh4ElX8AitvaKIrnBHu/PV5IN12aRnrT2ZlbvdzoGN+V3N8aAz3nYegHzbr2EeOm6yC7/wE4SG3Ofmx8ds7KfhX/1vHYI2AQX+SEuogsvrWR+lUp19+ulQPd+wbK5Fs696aCXCfuJSC+1oi7lsmGehfTjAUp9ltIc4RG+H4f2v3QAbjv9X68H8azUvWB/mRlCSfOOHHj64WzFM24uGzpV0GWX5kbrWKQ5jf5HmjYU12KkhjkGZzlJg0o39HKppFhBAjSiF3ejjlUaddtNdjhP6NIquyDGtsEHf7XyK9LbBlMhEu0yHm1sDLBd49ix/Br7YmvT35cJCKDvugLrFFg3Wv0vpw33lL7axhEetpUk49d83+uPEX34cf4u7wH+gloRiioewuUTmLk6pNmSieaTO3LUhHdYzb29oz2HcauTA604XsmnWc/OHTMIt9PAmnq9tBCPoPEwCdqBrGWDrNMgcZ/pUAXcm5Uv3B8o5PycXufuzac6w4pC/E2XqTmgy7amjqYod5SR6oe196dfd/s+a0uFMjV+pDSXf1ymmClDKtxIzx04iJaCQ9jDZO84jwd00puEiZ+56mdYPSJfcRGKRTO/raqamZniLPfBJ+VUrQcWqUxGbDLD9TY2H2RNsoDDpEWFM0rE0xqaSjH3aNaXLuUHrLeeifD/YMQMJOjc0rmn/qg4e4yMAK9y28cDazr74l+RsWgf6zm2ogeCD3rCtt6iDeGCaQRdZxaaUiY5FB119MpEZgn0psNOwhgizP2u8qXpXWZywc6J/j04xbXC3MchLpYrXMRikfFpJiTWShnR0ZA==",
        "jschl_vc": "c325a2c9822b57bbfd2ddbce508fbb98",
        "pass": "1609723386.105-7CloikVh0W",
        "jschl_answer": "INpvYeFwKELk-7-60c126e21e7fe809",
        "cf_ch_verify": "plat",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.post(url=url, data=self.data, headers=self.headers, timeout=45)
            # resp.encoding = "utf-8"
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath("//html")[0]
        return content

    def brand(self):
        return "biovision"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        mora = html.xpath(
            './/span[@class="label"][contains(text(), "Catalog #:")]/following-sibling::span[@class="labelB"]/text()'
        )[0].strip()
        return mora

    def product_name(self, html):
        mora = html.xpath('.//h1[@itemprop="name"]/text()')[0].strip()
        return mora

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            mora = html.xpath(
                './/th[@class="label"][contains(text(), "Size") and not(contains(text(), "Cat # +Size"))]/following-sibling::td[@class="data"]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_type(self, html):
        try:
            mora = html.xpath(
                './/span[@class="info-name"][contains(text(), "Assay type")]/following-sibling::p/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def detection_method(self, html):
        try:
            mora = html.xpath(
                './/th[@class="label"][contains(text(), "Detection Method")]/following-sibling::td[@class]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def sample_type(self, html):
        try:
            mora = html.xpath(
                './/th[@class="label"][contains(text(), "Applications")]/following-sibling::td[@class]/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def assay_length(self, html):
        try:
            mora = html.xpath(
                './/span[@class="info-name"][contains(text(), "Assay duration ")]/following-sibling::p/text()'
            )[0].strip()
            return mora
        except Exception:
            return None

    def sensitivity(self, html):
        try:
            mora = html.xpath(
                './/th[@class="label"][contains(text(), "Features & Benefits")]/following-sibling::td[@class="data"]/text()'
            )
            for item in mora:
                if "• Sensitivity:" in item:
                    return item.split("Sensitivity:")[-1].strip()
        except Exception:
            return None

    def assay_range(self, html):
        try:
            mora = html.xpath(
                './/th[@class="label"][contains(text(), "Features & Benefits")]/following-sibling::td[@class="data"]/text()'
            )
            for item in mora:
                if "• Detection Range:" in item:
                    return item.split("Detection Range:")[-1].strip()
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
            mora = (
                html.xpath(
                    './/div[@class="pro-cont"]//*[contains(text(), "Gene ID:")]/text()'
                )[0]
                .split("Gene ID:")[-1]
                .strip()
            )
            return mora
        except Exception:
            return None

    def swissprot(self, html):
        try:
            mora = (
                html.xpath(
                    './/div[@class="pro-cont"]//*[contains(text(), "Swiss Port:")]/text()'
                )[0]
                .split("Swiss Port:")[-1]
                .strip()
            )
            return mora
        except Exception:
            return None

    def datasheet_url(self, html):
        try:
            mora = (
                "https://www.biovision.com"
                + html.xpath('.//strong[contains(text(), "Datasheet")]/../../@href')[
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
                html.xpath('.//a[contains(text(), "Zoom")][@id="zoom-btn"]/@href')
            )
            return mora
        except Exception:
            return 0

    def citations(self, html):
        try:
            mora = len(
                html.xpath(
                    './/div[@id="extra_tabs_att3_citations_contents"]//div[@class="faq-odd" or @class="faq-even"]'
                )
            )
            return mora
        except Exception:
            return 0

    def synonyms(self, html):
        try:
            mora = html.xpath(
                './/span[@class="info-name"][contains(text(), "Alternative names")]/following-sibling::p/text()'
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
                './/th[@class="label"][contains(text(), "Species Reactivity")]/following-sibling::td[@class]/text()'
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
                    './/div[@id="extra_tabs_att3_citations_contents"]//div[@class="faq-odd" or @class="faq-even"]'
                )
                for item in citations_text:
                    title_text = item.xpath('./div[@class="citation"]//text()')
                    title = "".join(i for i in title_text)
                    if len(title) == 0:
                        continue
                    try:
                        link = item.xpath('./div[@class="citation"]/a/@href')[0].strip()
                    except Exception:
                        link = None
                    results.append([title, link])
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
            imgs = html.xpath('.//a[contains(text(), "Zoom")][@id="zoom-btn"]')[0]
            img_url = imgs.xpath("./@href")[0].strip()
            results.append([img_url])
            return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            sub_pri = html.xpath(
                './/div[@class="product-type-data"]//span[@class="price"]/text()'
            )[0].strip()
            results.append([sub_pri])
            return results
        except Exception:
            return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("biovision_kit_detail"):
        extract = r.rpop("biovision_kit_detail")
        # extract = "https://www.biovision.com/adiponectin-mouse-elisa-assay-kit.html"
        print(extract)
        try:
            lxml = Biovision().format(extract)
        except Exception as e:
            print(e)
            r.lpush("biovision_kit_detail", extract)
            print("sleeping...")
            time.sleep(30)
            continue
        if lxml is not None:
            brand = Biovision().brand()
            kit_type = Biovision().kit_type()
            catalog_number = Biovision().catalog_number(lxml)
            product_name = Biovision().product_name(lxml)
            detail_url = Biovision().detail_url(extract)
            tests = Biovision().tests(lxml)
            # assay_type = Biovision().assay_type(lxml)
            detection_method = Biovision().detection_method(lxml)
            sample_type = Biovision().sample_type(lxml)
            # assay_length = Biovision().assay_length(lxml)
            sensitivity = Biovision().sensitivity(lxml)
            assay_range = Biovision().assay_range(lxml)
            # specificity = Biovision().specificity(lxml)
            # target_protein = Biovision().target_protein(lxml)
            # geneid = Biovision().geneid(lxml)
            # swissprot = Biovision().swissprot(lxml)
            datasheet_url = Biovision().datasheet_url(lxml)
            review = Biovision().review(lxml)
            image_qty = Biovision().image_qty(lxml)
            citations = Biovision().citations(lxml)
            # synonyms = Biovision().synonyms(lxml)
            # conjugate = Biovision().conjugate(lxml)
            species_reactivity = Biovision().species_reactivity(lxml)
            # note = Biovision().note(lxml)

            sub_citations = Biovision().sub_citations(citations, lxml)
            sub_images = Biovision().sub_images(image_qty, lxml)
            sub_price = Biovision().sub_price(lxml)

        else:
            r.lpush("biovision_kit_detail", extract)
            print("html is none")
            continue
        new_detail = Detail(
            Brand=brand,
            Kit_Type=kit_type,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Detail_url=detail_url,
            Tests=tests,
            # Assay_type=assay_type,
            Detection_Method=detection_method,
            Sample_type=sample_type,
            # Assay_length=assay_length,
            Sensitivity=sensitivity,
            Assay_range=assay_range,
            # Specificity=specificity,
            # Target_Protein=target_protein,
            # GeneId=geneid,
            # SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            Review=str(review),
            Image_qty=image_qty,
            Citations=citations,
            # Synonyms=synonyms,
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

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    Article_title=sub_tit,
                    Pubmed_url=sub_link,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = sub[0]
                # des = sub[1]

                new_images = Images(Catalog_Number=catalog_number, Image_url=img)
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                # sub_s = sub[0]
                sub_p = sub[0]

                new_price = Price(
                    Catalog_Number=catalog_number,
                    Size=tests,
                    Price=sub_p,
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("biovision_kit_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
