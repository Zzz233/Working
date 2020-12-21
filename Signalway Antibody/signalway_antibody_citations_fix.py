import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time
import random

Base = declarative_base()


class Detail(Base):
    __tablename__ = "sab_antibody_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Antibody_Type = Column(String(40), nullable=True, comment="")
    Sellable = Column(String(40), nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Application = Column(String(500), nullable=True, comment="")
    Conjugated = Column(String(200), nullable=True, comment="")
    Clone_Number = Column(String(40), nullable=True, comment="")
    Recombinant_Antibody = Column(String(10), nullable=True, comment="")
    Modified = Column(String(100), nullable=True, comment="")
    Host_Species = Column(String(20), nullable=True, comment="")
    Reactivity_Species = Column(String(20), nullable=True, comment="")
    Antibody_detail_URL = Column(String(500), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Price_Status = Column(String(20), nullable=True, comment="")
    Citations_Status = Column(String(20), nullable=True, comment="")
    GeneId = Column(String(500), nullable=True, comment="")
    KO_Validation = Column(String(10), nullable=True, comment="")
    Species_Reactivity = Column(String(1000), nullable=True, comment="")
    SwissProt = Column(String(500), nullable=True, comment="")
    Immunogen = Column(String(1000), nullable=True, comment="")
    Predicted_MW = Column(String(200), nullable=True, comment="")
    Observed_MW = Column(String(200), nullable=True, comment="")
    Isotype = Column(String(200), nullable=True, comment="")
    Purify = Column(String(200), nullable=True, comment="")
    Citations = Column(String(20), nullable=True, comment="")
    Citations_url = Column(String(500), nullable=True, comment="")
    DataSheet_URL = Column(String(500), nullable=True, comment="")
    Review = Column(String(20), nullable=True, comment="")
    Price_url = Column(String(500), nullable=True, comment="")
    Image_qty = Column(Integer, nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Application(Base):
    __tablename__ = "sab_antibody_application"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Application = Column(String(1000), nullable=True, comment="")
    Dilution = Column(String(2000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "sab_antibody_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    PMID = Column(String(40), nullable=True, comment="")
    Application = Column(String(300), nullable=True, comment="")
    Species = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(1000), nullable=True, comment="")
    Pubmed_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "sab_antibody_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Images(Base):
    __tablename__ = "sab_antibody_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=2)
r = redis.Redis(connection_pool=pool)

"""
TODO 修饰 modify
url含Conjugated不要
"""


class Signalway(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers)
            x = etree.HTML(resp.text)
            y = x.xpath('//div[@class="detail"]')[0]
        return y

    def brand(self):
        return "Signalway Antibody"

    def catalog_number(self, html):
        catalog_number = html.xpath(".//h2/text()")[0].split("#")[1]
        return catalog_number

    def product_name(self, html):
        product_name = html.xpath(".//h2/text()")[0].split("#")[0]
        return product_name

    def antibody_type(self, html):
        try:
            antibody_type = html.xpath(
                './/span[contains(text(), "Clonality")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return antibody_type

    def sellable(self, html):
        try:
            yes = (
                html.xpath('.//span[contains(text(), "Yes")]/text()')[0].strip().lower()
            )
        except Exception:
            return "no"
        if yes == "yes":
            return yes

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/span[@class="altive"]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return synonyms

    def application(self, html):
        try:
            application = html.xpath(
                './/span[contains(text(), "Applications")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return application

    def conjugated(self):
        return None

    def clone_number(self, html):
        try:
            clone_number = html.xpath(
                './/span[contains(text(), "Clone No.")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return clone_number

    def recombinant_antibody(self):
        return None

    def modified(self, html):
        try:
            modified = html.xpath(
                './/span[contains(text(), "Modification")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return modified

    def host_species(self, html):
        try:
            host_species = html.xpath(
                './/span[contains(text(), "Host Species")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return host_species

    def reactivity_species(self):
        return None

    def antibody_detail_url(self, url):
        return url

    def antibody_status(self):
        return None

    def price_status(self):
        return None

    def citations_status(self):
        return None

    def geneid(self, html):
        try:
            accessionNo = html.xpath(
                './/span[contains(text(), "Accession No.")]/following-sibling::i//text()'
            )
            noText = " ".join(i for i in accessionNo)
            if "Gene ID:" in noText:
                geneid = noText.split("Gene ID:")[1].strip().split(" ")[0]
            elif "Gene id:" in noText:
                geneid = noText.split("Gene id:")[1].strip().split(" ")[0]
            else:
                return None
        except Exception:
            return None
        return geneid

    def ko_validation(self):
        return None

    def species_reactivity(self, html):
        try:
            species_reactivity = (
                html.xpath(
                    './/span[contains(text(), "Species Reactivity")]/following-sibling::i/text()'
                )[0]
                .strip()
                .replace(" ", ", ")
            )
        except Exception:
            return None
        return species_reactivity

    def immunogen(self, html):
        try:
            immunogen = html.xpath(
                './/span[contains(text(), "Immunogen Description")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return immunogen

    def swissprot(self, html):
        try:
            accessionNo = html.xpath(
                './/span[contains(text(), "Accession No.")]/following-sibling::i//text()'
            )
            noText = " ".join(i for i in accessionNo)
            if "Swiss-Prot#:" in noText:
                swissprot = noText.split("Swiss-Prot#: ")[1].split(" ")[0]
            elif "Swiss-Prot:" in noText:
                swissprot = noText.split("Swiss-Prot: ")[1].split(" ")[0]
            else:
                return None
        except Exception:
            return None
        return swissprot

    def predicted_mw(self, html):
        try:
            predicted_mw = html.xpath(
                './/span[contains(text(), "Calculated MW")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return predicted_mw

    def observed_mw(self, html):
        # SDS obs  Cal pre
        try:
            observed_mw = html.xpath(
                './/span[contains(text(), "SDS-PAGE MW")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return observed_mw

    def isotype(self, html):
        try:
            isotype = html.xpath(
                './/span[contains(text(), "Isotype")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return isotype

    def purify(self, html):
        try:
            purify = html.xpath(
                './/span[contains(text(), "Purification")]/following-sibling::i/text()'
            )[0].strip()
        except Exception:
            return None
        return purify

    def citations(self, html):
        try:
            citations = len(html.xpath('.//span[@class="list_item1"]'))
        except Exception:
            return 0
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        try:
            dataSheet_url = (
                "https://www.sabbiotech.com.cn/"
                + html.xpath('.//a[@class="btn-pdf"]/@href')[0].strip()
            )
        except Exception:
            return None
        return dataSheet_url

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        try:
            image_qty = len(
                html.xpath(
                    './/span[contains(text(), "Images")]/../following-sibling::div[@class="content"]/ul/li'
                )
            )
        except Exception:
            return 0
        return image_qty

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        # 使用时判断返回类型 list str
        results = []
        note = 0
        try:
            p = html.xpath(
                './/span[contains(text(), "Application Details")]/../following-sibling::div[@class="content"]/ul/li//text()'
            )
        except Exception:
            return results
        for text in p:
            if "Not yet tested in other applications." in text:
                break
            elif "\t" in text:
                sub_app = text.split("\t")[0].replace("/n", "").strip()
                sub_dil = text.split("\t")[1].strip()
                # print(sub_app, sub_dil)
                results.append([sub_app, sub_dil])
            elif ": " in text:
                sub_app = text.split(": ")[0].replace("/n", "").strip()
                sub_dil = text.split(": ")[1].strip()
                # print(sub_app, sub_dil)
                results.append([sub_app, sub_dil])
            elif " (" in text:
                for single_line in text.split(","):
                    sub_app = single_line.split(" (")[0].replace("/n", "").strip()
                    sub_dil = single_line.split(" (")[1].replace(")", "").strip()
                    # print(sub_app, sub_dil)
                    results.append([sub_app, sub_dil])
                return results
            else:
                note = " | ".join(i for i in p)
                return note
        return results

    # ======================================================================== #
    # price表
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

    # ======================================================================== #
    # Citations表
    def sub_citations(self, html):
        results = []
        try:
            spans = html.xpath('.//span[@class="list_item1"]')
            for item in spans:
                text = item.xpath(".//text()")
                title = "".join(i for i in text).split("  PMID: ")[0].strip()
                pmid = item.xpath(".//strong/a/text()")[0].split("PMID:")[1].strip()
                link = item.xpath(".//strong/a/@href")[0].strip()
                results.append([pmid, title, link])
        except Exception:
            return results
        return results

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        results = []
        try:
            lis = html.xpath(
                './/span[contains(text(), "Images")]/../following-sibling::div[@class="content"]/ul/li'
            )
        except Exception:
            return results
        for item in lis:
            img_url = (
                "https://www.sabbiotech.com.cn/"
                + item.xpath(".//span/img/@src")[0].strip()
            )
            try:
                img_des = item.xpath(".//i/text()")[0].strip()
            except Exception:
                img_des = None
            results.append([img_url, img_des])
        return results


if __name__ == "__main__":
    while r.exists("Signalway"):
        url = r.lpop("Signalway")
        if "-Conjugated-" in url:
            print("pass")
            continue
        print(url)
        try:
            lxml = Signalway().format(url)
        except Exception as e:
            print(e)
            r.rpush("Signalway", url)
            continue
        brand = Signalway().brand()
        catalog_number = Signalway().catalog_number(lxml)
        product_name = Signalway().product_name(lxml)
        antibody_type = Signalway().antibody_type(lxml)
        sellable = Signalway().sellable(lxml)
        synonyms = Signalway().synonyms(lxml)
        application = Signalway().application(lxml)
        clone_number = Signalway().clone_number(lxml)
        modified = Signalway().modified(lxml)
        host_species = Signalway().host_species(lxml)
        antibody_detail_url = Signalway().antibody_detail_url(url)
        geneid = Signalway().geneid(lxml)
        species_reactivity = Signalway().species_reactivity(lxml)
        immunogen = Signalway().immunogen(lxml)
        swissprot = Signalway().swissprot(lxml)
        predicted_mw = Signalway().predicted_mw(lxml)
        observed_mw = Signalway().observed_mw(lxml)
        isotype = Signalway().isotype(lxml)
        purify = Signalway().purify(lxml)
        citations = Signalway().citations(lxml)
        dataSheet_url = Signalway().dataSheet_url(lxml)
        image_qty = Signalway().image_qty(lxml)

        sub_application = Signalway().sub_application(lxml)
        sub_price = Signalway().sub_price(lxml)
        sub_citations = Signalway().sub_citations(lxml)
        sub_images = Signalway().sub_images(lxml)

        new_detial = Detail(
            Brand=brand,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Antibody_Type=antibody_type,
            Sellable=sellable,
            Synonyms=synonyms,
            Application=application,
            # Conjugated=conjugated,
            Clone_Number=clone_number,
            # Recombinant_Antibody=recombinant_antibody,
            Modified=modified,
            Host_Species=host_species,
            # Reactivity_Species=reactivity_species,
            Antibody_detail_URL=antibody_detail_url,
            GeneId=geneid,
            # KO_Validation=ko_validation,
            Species_Reactivity=species_reactivity,
            SwissProt=swissprot,
            Immunogen=immunogen,
            Predicted_MW=predicted_mw,
            Observed_MW=observed_mw,
            Isotype=isotype,
            Purify=purify,
            Citations=str(citations),
            DataSheet_URL=dataSheet_url,
            # Review=review,
            Image_qty=image_qty,
        )
        session.add(new_detial)

        if isinstance(sub_application, str):
            new_application = Application(
                Catalog_Number=catalog_number, Note=sub_application
            )
            session.add(new_application)
        elif isinstance(sub_application, list):
            objects_sub_application = []
            for sub in sub_application:
                app = sub[0]
                dil = sub[1]
                new_application = Application(
                    Catalog_Number=catalog_number, Application=app, Dilution=dil
                )
                objects_sub_application.append(new_application)
            session.bulk_save_objects(objects_sub_application)
        else:
            pass

        if isinstance(sub_price, list):
            objects_sub_price = []
            for sub in sub_price:
                cat = sub[0]
                siz = sub[1]
                pri = sub[2]
                new_price = Price(
                    Catalog_Number=catalog_number,
                    sub_Catalog_Number=cat,
                    Size=siz,
                    Price=pri,
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)
        else:
            pass

        if isinstance(sub_citations, list):
            objects_sub_citations = []
            for sub in sub_citations:
                pid = sub[0]
                tit = sub[1]
                lin = sub[2]
                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=pid,
                    Article_title=tit,
                    Pubmed_url=lin,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)
        else:
            pass

        if isinstance(sub_images, list):
            objects_sub_images = []
            for sub in sub_images:
                img_u = sub[0]
                img_d = sub[1]
                new_images = Images(
                    Catalog_Number=catalog_number,
                    Image_url=img_u,
                    Image_description=img_d,
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)
        else:
            pass

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.rpush("Signalway", url)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
