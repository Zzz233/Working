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

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=1)
r = redis.Redis(connection_pool=pool)
Base = declarative_base()


class Detail(Base):
    __tablename__ = "lsbio_antibody_detail"

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
    __tablename__ = "lsbio_antibody_application"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Application = Column(String(1000), nullable=True, comment="")
    Dilution = Column(String(2000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "lsbio_antibody_citations"

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
    __tablename__ = "lsbio_antibody_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Images(Base):
    __tablename__ = "lsbio_antibody_images"

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


class LSbio:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Geck"
        "o/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,ima"
        "ge/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;"
        "q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        # 'Cookie': 'LifespanSessionId=kaw1deoamlwa5ycgndt4alya; _'\
        # 'ga=GA1.2.41388869.1605162837; _gid=GA1.2.1581760578.1605162837; _'\
        # 'uetsid=0a3ab40024b111eb86834f0e73ed8ba6; _uetvid=0a3b0ef024b111ebb'\
        # 'ea1f18dee0b72bf',
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "TE": "Trailers",
    }

    def format_html(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=30)
            # print(resp.text)
            html = etree.HTML(resp.text)
            # TODO 条件
            if html.xpath('//div[@class="c-AntibodyDetail"]'):
                html_content = html.xpath('//div[@class="c-AntibodyDetail"]')
                status = 1
            else:
                status = 0
        return status, html_content[0]

    # ======================================================================== #
    # detail表
    def brand(self):
        return "LifeSpan BioSciences"

    def catalog_number(self, html):
        catano = html.xpath(
            './/div[@class="l-DetailPage__Title l-Detai'
            'lPage__Title-Mobile"]/h1/span/text()'
        )[0].split(" ")[-1]
        return catano

    def product_name(self, html):
        name = html.xpath(
            './/div[@class="l-DetailPage__Overview"]//div['
            'contains(text(),"Antibody:")]/following-sibli'
            'ng::div[@class="DataText"]/text()'
        )[0]
        return name

    def antibody_type(self, html):
        if html.xpath(
            './/div[@class="LabelText c-AntibodyS'
            'pecifications__LabelText"][contains('
            'text(), "Clonality")]'
        ):
            antibody_type = html.xpath(
                './/div[@class="LabelText c-AntibodyS'
                'pecifications__LabelText"][contains('
                'text(), "Clonality")]/following-sibl'
                'ing::div[@class="DataText"]/span/tex'
                "t()"
            )[0]
        else:
            antibody_type = None
        return antibody_type

    def sellable(self, html):
        if html.xpath('.//div[@class="c-OrderBox__OrderForm"]'):
            sellable = "yes"
        else:
            sellable = "no"
        return sellable

    def synonyms(self, html):
        if html.xpath('.//div[@class="SubLabelText"][contains(' 'text(), "Synonyms")]'):
            synonyms = (
                html.xpath(
                    './/div[@class="SubLabelText"][contains('
                    'text(), "Synonyms")]/'
                    "following-sibling::"
                    'div[@class="DataText"]/text()'
                    ""
                )[0]
                .strip()
                .replace(" | ", ",")
            )
        else:
            synonyms = None
        return synonyms

    def application(self, html):
        if html.xpath(
            './/div[@class="c-AntibodyOverview__Labe'
            'lText"][contains(text(), "Application:")]'
        ):
            application = html.xpath(
                './/div[@class="c-AntibodyOverview__Labe'
                'lText"][contains(text(), "Application:")]'
                '/following-sibling::div[@class="DataText"'
                "]/text()"
            )[0]
        else:
            application = None
        return application

    def conjugated(self, html):
        if html.xpath('.//div[contains(text(),"Conjugations")]'):
            conjugated = (
                str(
                    html.xpath(
                        './/div[contains(text(),"Conjugations")]'
                        '/following-sibling::div[@class="DataText'
                        '"]//text()'
                    )
                )
                .replace("\\n", "")
                .replace("\\r", "")
                .replace("', '", "")
                .replace("['", "")
                .replace("']", "")
                .replace("                            \\xa0", "")
                .strip()
                .split("(This antibody is")[0]
            )
        else:
            conjugated = None
        return conjugated

    def clone_number(self):
        # TODO
        return "问问问问问"

    def recombinant_antibody(self):
        # TODO
        return "问问问问问"

    def modified(self, html):
        if html.xpath(
            './/div[@class="LabelText c-AntibodySpecifications__LabelTe'
            'xt"][contains(text(), "Modifications")]'
        ):
            modified = html.xpath(
                './/div[@class="LabelText c-AntibodySpecifications__LabelTe'
                'xt"][contains(text(), "Modifications")]/following-sibling'
                '::div[@class="DataText"]/text()'
            )[0].strip()
        else:
            modified = None
        return modified

    def host_species(self, html):
        if html.xpath(
            './/div[@class="LabelText c-AntibodySpecifications__LabelTe'
            'xt"][contains(text(), "Host")]'
        ):
            host_species = html.xpath(
                './/div[@class="LabelText c-AntibodySpecifications__LabelTe'
                'xt"][contains(text(), "Host")]/following-sibling'
                '::div[@class="DataText"]/text()'
            )[0].strip()
        else:
            host_species = None
        return host_species

    def reactivity_species(self):
        # TODO
        return "问问问问问"

    def antibody_detail_url(self, url):
        return url

    def antibody_status(self):
        return None

    def price_status(self):
        return None

    def citations_status(self):
        return None

    def geneId(self):
        return None

    def ko_validation(self):
        return None

    def species_reactivity(self, html):
        if html.xpath(
            './/div[@class="c-AntibodyOverview__LabelText"][contains(te'
            'xt(), "Reactivity:")]'
        ):
            species_reactivity = html.xpath(
                './/div[@class="c-AntibodyOverview__LabelText"][contains(te'
                'xt(), "Reactivity:")]/following-sibling::div[@class="Data'
                'Text"]/text()'
            )[0].strip()
        else:
            species_reactivity = None
        return species_reactivity

    def swissprot(self, html):
        if html.xpath('.//label[contains(text(), "Uniprot:")]'):
            swissprot = html.xpath(
                './/label[contains(text(), "Uniprot:")]/' "following-sibling::a/text()"
            )[0].strip()
        else:
            swissprot = None
        return swissprot

    def immunogen(self, html):
        if html.xpath(
            './/div[@class="LabelText c-AntibodySpecifications__LabelText"]'
            '[contains(text(), "Immunogen")]'
        ):
            immunogen = html.xpath(
                './/div[@class="LabelText c-AntibodySpecifications__LabelText"]'
                '[contains(text(), "Immunogen")]/following-sibling::div['
                '@class="DataText"]/text()'
            )[0].strip()
        else:
            immunogen = None
        return immunogen

    def predicted_mw(self):
        return None

    def observed_mw(self):
        return None

    def isotype(self):
        return None

    def purify(self, html):
        if html.xpath(
            './/div[@class="LabelText c-AntibodySpecifications__LabelText"]'
            '[contains(text(), "Purification")]'
        ):
            purify = html.xpath(
                './/div[@class="LabelText c-AntibodySpecifications__LabelText"]'
                '[contains(text(), "Purification")]/following-sibling::div['
                '@class="DataText"]/text()'
            )[0].strip()
        else:
            purify = None
        return purify

    def citations(self, html):
        if html.xpath(
            './/h2[@class="c-DetailPage__MainContentSectionHeaderText"]'
            '[contains(text(), "Publications (")]'
        ):
            citations = html.xpath(
                './/h2[@class="c-DetailPage__MainContentSectionHeaderText"]'
                '[contains(text(), "Publications (")]/text()'
            )[0].strip()
            num = citations.split("(")[1].split(")")[0]
        else:
            num = 0
        return num

    def citations_url(self):
        return None

    def dataSheet_url(self):
        return None

    def review(self, html):
        review = html.xpath(
            './/h2[@class="c-DetailPage__MainContentSectionHeaderText"]'
            '[contains(text(), "Customer Reviews")]/text()'
        )[0].strip()
        num = review.split("(")[1].split(")")[0]
        return num

    def price_url(self, html):
        return None

    def image_qty(self, html):
        image_qty = html.xpath(
            './/img[@class="c-ImageCarousel__Image '
            "c-ImageCarousel__Image-Type_Primary c-ImageCa"
            'rousel__Image-Type_Primary-Stretch_Height"]'
        )
        num = len(image_qty)
        return num

    def image_url(self):
        return None

    def Note(self):
        return "0"

    # ======================================================================== #
    # application表
    def sub_application(self, html):
        result = []
        if html.xpath('.//ul[@class="l-AntibodySpecifications__AssayList"]'):
            ul = html.xpath('.//ul[@class="l-AntibodySpecifications__AssayList"]')[0]
        else:
            return result
        result = []
        for item in ul.xpath("./li/text()"):
            if "(" in item:
                sub_application = item.split("(")[0].strip()
                sub_dilution = item.split("(")[1].split(")")[0].strip()
                result.append([sub_application, sub_dilution])
            else:
                sub_application = item.strip()
                sub_dilution = None
                if "Applications tested for the base" not in sub_application:
                    result.append([sub_application, sub_dilution])
        return result

    # ======================================================================== #
    # citations表 species??
    def sub_citations(self, html):
        result = []
        if html.xpath('.//div[@class="c-Publication"]'):
            sub_citations = html.xpath('.//div[@class="c-Publication"]')
        else:
            return result
        for item in sub_citations:
            article_title = item.xpath(
                './/div[@class="l-Publi' 'cation__Text"]/text()'
            )[0].strip()
            pmid = item.xpath(".//a[contains(text()" ', "PubMed:")]/text()')[0].split(
                ": "
            )[1]
            pubmed_url = item.xpath(".//a[contains(text()," ' "PubMed:")]/@href')[0]
            result.append([article_title, pmid, pubmed_url])
        return result

    # ======================================================================== #
    # images表
    def sub_images(self, html):
        result = []
        if html.xpath('.//div[@class="l-ImageCarousel__Image"]'):
            sub_images = html.xpath('.//div[@class="l-ImageCarousel__Image"]')
        else:
            return result
        for item in sub_images:
            if "Height" in item:
                image_url = item.xpath(
                    './/img[@class="c-ImageCarousel__Image '
                    "c-ImageCarousel__Image-Type_Primary c-I"
                    "mageCarousel__Image-Type_Primary-Stret"
                    'ch_Height"]/@src'
                )[0].strip()
                image_decription = item.xpath(
                    './/img[@class="c-ImageCarousel__Image '
                    "c-ImageCarousel__Image-Type_Primary c-I"
                    "mageCarousel__Image-Type_Primary-Stret"
                    'ch_Height"]/@alt'
                )[0].strip()
                result.append([image_url, image_decription])
            elif "Width" in item:
                image_url = item.xpath(
                    './/img[@class="c-ImageCarousel__Image '
                    "c-ImageCarousel__Image-Type_Primary c-I"
                    "mageCarousel__Image-Type_Primary-Stret"
                    'ch_Width"]/@src'
                )[0].strip()
                image_decription = item.xpath(
                    './/img[@class="c-ImageCarousel__Image '
                    "c-ImageCarousel__Image-Type_Primary c-I"
                    "mageCarousel__Image-Type_Primary-Stret"
                    'ch_Width"]/@alt'
                )[0].strip()
                result.append([image_url, image_decription])

        return result

    # ======================================================================== #
    # price表
    def sub_price(self, html):
        result = []
        if html.xpath('.//div[@class="l-OrderBox__ProductsTableRow"]'):
            orders = html.xpath('.//div[@class="l-OrderBox__ProductsTableRow"]')
        else:
            return result
        for i, item in enumerate(orders):
            sub_catalog_number = item.xpath(
                './/div[@class="c-OrderBox__PricingTable' 'CatalogCol"]/text()'
            )[0].strip()
            sub_price = item.xpath(".//input/@value")[0]
            sub_size = html.xpath(
                './/select[@class="c-OrderBox__SizeSelect"]/option/text()'
            )[i]
            result.append([sub_catalog_number, sub_size, sub_price])
        return result


def main():
    # link = 'https://www.lsbio.com/antibodies/esrrg-antibody-err-gamma-antibody-aa190-239-hrp-ihc-wb-western-ls-c426752/439458'
    while r.exists("lsbio_detail"):
        extract = r.rpop("lsbio_detail")
        print(extract)
        try:
            r_status, r_html = LSbio().format_html(extract)
        except Exception as e:
            r.lpush("lsbio_detail", extract)
            continue
        if r_status == 0:
            time.sleep(5)
            continue
        brand = LSbio().brand()
        catalog_number = LSbio().catalog_number(r_html)
        product_name = LSbio().product_name(r_html)
        antibody_type = LSbio().antibody_type(r_html)
        sellable = LSbio().sellable(r_html)
        synonyms = LSbio().synonyms(r_html)
        application = LSbio().application(r_html)
        conjugated = LSbio().conjugated(r_html)
        modified = LSbio().modified(r_html)
        host_species = LSbio().host_species(r_html)
        antibody_detail_url = LSbio().antibody_detail_url(extract)
        species_reactivity = LSbio().species_reactivity(r_html)
        swissprot = LSbio().swissprot(r_html)
        immunogen = LSbio().immunogen(r_html)
        purify = LSbio().purify(r_html)
        citations = LSbio().citations(r_html)
        review = LSbio().review(r_html)
        image_qty = LSbio().image_qty(r_html)
        sub_application = LSbio().sub_application(r_html)
        sub_citations = LSbio().sub_citations(r_html)
        sub_images = LSbio().sub_images(r_html)
        sub_price = LSbio().sub_price(r_html)

        new_detail = Detail(
            Brand=brand,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Antibody_Type=antibody_type,
            Sellable=sellable,
            Synonyms=synonyms,
            Application=application,
            Conjugated=conjugated,
            Modified=modified,
            Host_Species=host_species,
            Antibody_detail_URL=antibody_detail_url,
            Species_Reactivity=species_reactivity,
            SwissProt=swissprot,
            Immunogen=immunogen,
            Purify=purify,
            Citations=citations,
            Review=review,
            Image_qty=image_qty,
        )

        objects_sub_application = []
        for sub in sub_application:
            sub_application = sub[0]
            sub_dilution = sub[1]
            new_application = Application(
                Catalog_Number=catalog_number,
                Application=sub_application,
                Dilution=sub_dilution,
            )
            objects_sub_application.append(new_application)

        objects_sub_citations = []
        for sub in sub_citations:
            sub_article_title = sub[0]
            sub_pmid = sub[1]
            sub_pubmed_url = sub[2]

            new_citations = Citations(
                Catalog_Number=catalog_number,
                PMID=sub_pmid,
                Application=application,
                Species="Species",
                Article_title=sub_article_title,
                Pubmed_url=sub_pubmed_url,
            )
            objects_sub_citations.append(new_citations)

        objects_sub_images = []
        for sub in sub_images:
            sub_image_url = sub[0]
            sub_description = sub[1]

            new_images = Images(
                Catalog_Number=catalog_number,
                Image_url=sub_image_url,
                Image_description=sub_description,
            )
            objects_sub_images.append(new_images)

        objects_sub_price = []
        for sub in sub_price:
            sub_catalog_number = sub[0]
            sub_size = sub[1]
            sub_price = sub[2]

            new_price = Price(
                Catalog_Number=catalog_number,
                sub_Catalog_Number=sub_catalog_number,
                Size=sub_size,
                Price=sub_price,
            )
            objects_sub_price.append(new_price)

        try:
            session.add(new_detail)
            session.bulk_save_objects(objects_sub_application)
            session.bulk_save_objects(objects_sub_citations)
            session.bulk_save_objects(objects_sub_images)
            session.bulk_save_objects(objects_sub_price)
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            session.rollback()
            print(e)
        time.sleep(random.uniform(1, 2.5))


if __name__ == "__main__":
    main()
