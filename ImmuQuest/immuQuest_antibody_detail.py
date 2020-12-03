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
    __tablename__ = "immuquest_antibody_detail"

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
    __tablename__ = "immuquest_antibody_application"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Application = Column(String(1000), nullable=True, comment="")
    Dilution = Column(String(2000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "immuquest_antibody_citations"

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
    __tablename__ = "immuquest_antibody_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Images(Base):
    __tablename__ = "immuquest_antibody_images"

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


class ImmuQuest:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                content = html_lxml.xpath('//div[@id="product-box"]')[0]
            except Exception as e:
                content = None
        return content

    def brand(self):
        return "ImmuQuest"

    def catalog_number(self, html):
        try:
            catalog_number = html.xpath(
                './/b[contains(text(), "Catagloue Number:")]/../text()'
            )[0].strip()
        except Exception as e:
            catalog_number = None
        return catalog_number

    def product_name(self, html):
        try:
            product_name = html.xpath(".//h1/text()")[0].strip()
        except Exception as e:
            product_name = None
        return product_name

    def antibody_type(self, name):
        if "Monoclonal" in name:
            antibody_type = "Monoclonal"
        elif "Polyclonal" in name:
            antibody_type = "Polyclonal"
        elif "[" in name:
            antibody_type = "Monoclonal"
        else:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        try:
            if html.xpath('.//button[contains(text(), "Add to Cart")]'):
                sellable = "yes"
            else:
                sellable = "no"
        except Exception as e:
            sellable = None
        return sellable

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/td[@class="heading"][contains(text(), "Also Known As:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            synonyms = None
        return synonyms

    def application(self, html):
        try:
            application = html.xpath(
                './/td[@class="heading"][contains(text(), "Applications:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            application = None
        return application

    def conjugated(self):
        return None

    def clone_number(self, name):
        if "[" in name:
            clone_number = name.split("[")[1].split("]")[0].replace(" Polyclonal", "")
        else:
            clone_number = None
        return clone_number

    def recombinant_antibody(self):
        return None

    def modified(self):
        return None

    def host_species(self, html):
        try:
            host_species = html.xpath(
                './/td[@class="heading"][contains(text(), "Host:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            host_species = None
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

    def geneId(self, html):
        try:
            geneId = html.xpath(
                './/td[@class="heading"][contains(text(), "Entrez Gene ID(s):")]/following-sibling::td//text()'
            )[0].strip()
        except Exception as e:
            geneId = None
        return geneId

    def ko_validation(self):
        return None

    def species_reactivity(self, html):
        try:
            species_reactivity = html.xpath(
                './/td[@class="heading"][contains(text(), "Species Reactivity:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            species_reactivity = None
        return species_reactivity

    def immunogen(self, html):
        try:
            immunogen = html.xpath(
                './/td[@class="heading"][contains(text(), "Immunogen:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            immunogen = None
        return immunogen

    def swissprot(self, html):
        try:
            swissprot = html.xpath(
                './/td[@class="heading"][contains(text(), "SwissProt ID(s):")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            swissprot = None
        return swissprot

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self, html):
        try:
            isotype = html.xpath(
                './/td[@class="heading"][contains(text(), "Isotype:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            isotype = None
        return isotype

    def purify(self, html):
        try:
            purify = html.xpath(
                './/td[@class="heading"][contains(text(), "Purification:")]/following-sibling::td/text()'
            )[0].strip()
        except Exception as e:
            purify = None
        return purify

    def citations(self, html):
        try:
            citations = len(html.xpath('.//td[@id="pubmed_IDs"]/text()')[0].split(","))
        except Exception as e:
            citations = None
        return citations

    def citations_url(self):
        return None

    def dataSheet_url(self, html):
        return None

    def review(self):
        return None

    def price_url(self):
        return None

    def image_qty(self, html):
        try:
            image_qty = len(
                html.xpath('.//ul[@id="product-thumbnails"][@class="slides"]/li')
            )
        except Exception as e:
            image_qty = None
        return image_qty

    def image_url(self):
        return None

    def Note(self):
        return None

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        results = []
        try:
            applications = (
                html.xpath(
                    './/td[@class="heading"][contains(text(), "Applications:")]/following-sibling::td/text()'
                )[0]
                .strip()
                .split(",")
            )
            for item in applications:
                results.append([item.strip()])
        except Exception as e:
            return results
        return results

    # ======================================================================== #
    # price表
    def sub_price(self, html):
        results = []
        try:
            sub_siz = html.xpath(
                './/td[@class="heading"][contains(text(), "Quantity:")]/following-sibling::td/text()'
            )[0].strip()
            sub_pri = html.xpath(
                './/span[@id="price-field"]/span[@class="money"]/text()'
            )[0].strip()
            results.append([sub_siz, sub_pri])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # citations表
    def sub_citations(self, html):
        results = []
        try:
            sub_cite = html.xpath('.//p[@class="citation"]')
            for item in sub_cite:
                sub_title = item.xpath("./text()")[0].strip()
                sub_url = item.xpath("./a/@href")[0].strip()
                if (
                    "www.ncbi.nlm.nih.gov/pubmed/" in sub_url
                    and "www.ncbi.nlm.nih.gov/pubmed/?" not in sub_url
                ):
                    sub_pmid = sub_url.split("www.ncbi.nlm.nih.gov/pubmed/")[1].replace(
                        "?dopt=Abstract", ""
                    )
                elif "pubmed.ncbi.nlm.nih.gov/" in sub_url:
                    sub_pmid = sub_url.split("pubmed.ncbi.nlm.nih.gov/")[1]
                else:
                    sub_pmid = None
                if sub_title == "Test":
                    pass
                else:
                    results.append([sub_pmid, sub_title, sub_url])
        except Exception as e:
            results = []
        return results

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        results = []
        try:
            sub_imgs = html.xpath('.//ul[@id="product-thumbnails"][@class="slides"]/li')
            for item in sub_imgs:
                sub_url = item.xpath(".//img/@src")[0].strip()
                sub_des = item.xpath(".//img/@alt")[0].strip()
                results.append([sub_url, sub_des])
        except Exception as e:
            results = []
        return results


if __name__ == "__main__":
    while True:
        extract = r.rpop("immuquest_detial")
        print(extract)
        # link = 'https://immuquest.com/collections/immunology-antibodies/products/f4-80-antibody-iqf4-80-polyclonal'
        try:
            lxml = ImmuQuest().format(extract)
        except Exception as e:
            r.lpush("immuquest_detial", extract)
            print(e)
            continue
        if lxml is not None:
            brand = ImmuQuest().brand()
            catalog_number = ImmuQuest().catalog_number(lxml)
            product_name = ImmuQuest().product_name(lxml)
            antibody_type = ImmuQuest().antibody_type(product_name)
            sellable = ImmuQuest().sellable(lxml)
            synonyms = ImmuQuest().synonyms(lxml)
            application = ImmuQuest().application(lxml)
            clone_number = ImmuQuest().clone_number(product_name)
            host_species = ImmuQuest().host_species(lxml)
            antibody_detail_url = ImmuQuest().antibody_detail_url(extract)
            geneId = ImmuQuest().geneId(lxml)
            species_reactivity = ImmuQuest().species_reactivity(lxml)
            immunogen = ImmuQuest().immunogen(lxml)
            swissprot = ImmuQuest().swissprot(lxml)
            isotype = ImmuQuest().isotype(lxml)
            purify = ImmuQuest().purify(lxml)
            citations = ImmuQuest().citations(lxml)
            image_qty = ImmuQuest().image_qty(lxml)
            sub_price = ImmuQuest().sub_price(lxml)
            sub_citations = ImmuQuest().sub_citations(lxml)
            sub_images = ImmuQuest().sub_images(lxml)
            sub_application = ImmuQuest().sub_application(lxml)

            print(sub_application)
        else:
            print("html is None")
            r.lpush("immuquest_detial", extract)
            continue
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
            # Modified=modified,
            Host_Species=host_species,
            # Reactivity_Species=reactivity_species,
            Antibody_detail_URL=antibody_detail_url,
            GeneId=geneId,
            # KO_Validation=ko_validation,
            Species_Reactivity=species_reactivity,
            SwissProt=swissprot,
            Immunogen=immunogen,
            # Predicted_MW=predicted_mw,
            # Observed_MW=observed_mw,
            Isotype=isotype,
            Purify=purify,
            Citations=str(citations),
            # DataSheet_URL=dataSheet_url,
            # Review=review,
            Image_qty=image_qty,
        )
        session.add(new_detial)

        if sub_application:
            objects_sub_application = []
            for sub in sub_application:
                sub_application = sub[0]
                new_application = Application(
                    Catalog_Number=catalog_number,
                    Application=sub_application,
                )
                objects_sub_application.append(new_application)
            session.bulk_save_objects(objects_sub_application)

        if sub_price:
            objects_sub_price = []
            for sub in sub_price:
                # sub_price, sub_size
                sub_price = sub[1]
                sub_size = sub[0]
                new_price = Price(
                    Catalog_Number=catalog_number, Price=sub_price, Size=sub_size
                )
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        if sub_citations:
            objects_sub_citations = []
            for sub in sub_citations:
                # sub_price, sub_size
                sub_pmid = sub[0]
                sub_title = sub[1]
                sub_url = sub[2]
                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pmid,
                    Article_title=sub_title,
                    Pubmed_url=sub_url,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                # sub_price, sub_size
                sub_img_url = sub[0]
                sub_img_des = sub[1]
                new_img = Images(
                    Catalog_Number=catalog_number,
                    Image_url=sub_img_url,
                    Image_description=sub_img_des,
                )
                objects_sub_images.append(new_img)
            session.bulk_save_objects(objects_sub_images)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("immuquest", extract)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
