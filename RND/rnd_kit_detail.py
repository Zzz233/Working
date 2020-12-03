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
    __tablename__ = 'r&d_elisa_kit_detail'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(100),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Detail_url = Column(String(1000),
                        nullable=True, comment='')
    Tests = Column(String(200),
                   nullable=True, comment='')
    Assay_type = Column(String(200),
                        nullable=True, comment='')
    Sample_type = Column(String(1000),
                         nullable=True, comment='')
    Assay_length = Column(String(200),
                          nullable=True, comment='')
    Sensitivity = Column(String(200),
                         nullable=True, comment='')
    Assay_range = Column(String(200),
                         nullable=True, comment='')
    Specificity = Column(String(200),
                         nullable=True, comment='')
    GeneId = Column(String(500),
                    nullable=True, comment='')
    SwissProt = Column(String(500),
                       nullable=True, comment='')
    DataSheet_URL = Column(String(500),
                           nullable=True, comment='')
    Review = Column(String(50),
                    nullable=True, comment='')
    Image_qty = Column(Integer,
                       nullable=True, comment='')
    Citations = Column(String(20),
                       nullable=True, comment='')
    Synonyms = Column(String(3000),
                      nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Citations(Base):
    __tablename__ = 'r&d_elisa_kit_citations'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    PMID = Column(String(40),
                  nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    Sample_type = Column(String(100),
                         nullable=True, comment='')
    Pubmed_url = Column(String(1000),
                        nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Images(Base):
    __tablename__ = 'r&d_elisa_kit_images'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Image_url = Column(String(500),
                       nullable=True, comment='')
    Image_description = Column(String(2000),
                               nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Price(Base):
    __tablename__ = 'r&d_elisa_kit_price'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    sub_Catalog_Number = Column(String(40),
                                nullable=True, comment='')
    Size = Column(String(50),
                  nullable=True, comment='')
    Price = Column(String(50),
                   nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


# Mysql
engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=2)
r = redis.Redis(connection_pool=pool)


class RND():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5'
                      '37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                content = html_lxml.xpath(
                    '//div[@class="main-container container"]')[0]
            except Exception as e:
                content = None
        return content

    def brand(self):
        return 'R&D Systems'

    def catalog_number(self, html):
        try:
            catnum = html.xpath('.//span[@class="category_number"]/text()')[
                0].strip()
        except Exception as e:
            catnum = None
        return catnum

    def product_name(self, html):
        try:
            name = html.xpath('.//h1[@class="ds_title"]/text()')[
                0].strip()
        except Exception as e:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        # ajax
        try:
            pass
            # tests = html.xpath('.//')
        except Exception as e:
            pass

    def assay_type(self, html):
        try:
            assay_type = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Assay Type")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            assay_type = None
        return assay_type

    def sample_type(self, html):
        try:
            sample_type = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Sample Type & Volume Required Per Well")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            sample_type = None
        return sample_type

    def assay_length(self, html):
        try:
            assay_length = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Assay Length")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            assay_length = None
        return assay_length

    def sensitivity(self, html):
        try:
            sensitivity = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Sensitivity")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            sensitivity = None
        return sensitivity

    def assay_range(self, html):
        try:
            assay_range = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Assay Range")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            assay_range = None
        return assay_range

    def specificity(self, html):
        try:
            specificity = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Specificity")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
            return specificity
        except Exception as e:
            pass
        try:
            specificity = html.xpath(
                './/div[@class="ds_table_cell summary_table_name"][contains(text(), "Species")]/following-sibling::div[@class="ds_table_cell summary_table_data"]/text()')[
                0].strip()
        except Exception as e:
            specificity = None
        return specificity

    def geneid(self, html):
        try:
            geneid = html.xpath(
                './/div[@class="ds_table_cell background_table_name"][contains(text(), "Entrez Gene IDs")]/following-sibling::div[@class="ds_table_cell background_table_data"]/text()')[
                0].strip()
        except Exception as e:
            geneid = None
        return geneid
        
    def swissprot(self):
        return None

    def datasheet_url(self, html):
        try:
            datasheet_url = html.xpath('.//a[@id="cfpdf"]/@href')[0].strip()
        except Exception as e:
            datasheet_url = None
        return datasheet_url

    def review(self, html):
        try:
            review = len(html.xpath('.//div[@class="review_row"]'))
        except Exception as e:
            review = 0
        return review

    def image_qty(self, html):
        try:
            a = len(html.xpath(
                './/img[@class=" hideoverflow quantikineLinearityImage"]'))
        except Exception as e:
            a = 0
        try:
            b = len(html.xpath(
                './/a[@class="ds-image overview-images data_ex_image_link"]'))
        except Exception as e:
            b = 0
        image_qty = a + b
        return image_qty

    def citations(self, html):
        try:
            citations = len(html.xpath('.//li[@class="top-lined"]'))
        except Exception as e:
            citations = 0
        return citations

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/div[@class="ds_table_cell background_table_name"][contains(text(), "Alternate Names")]/following-sibling::div[@class="ds_table_cell background_table_data"]/text()')[
                0].strip()
        except Exception as e:
            synonyms = None
        return synonyms

    def note(self, html):
        try:
            text = html.xpath(
                './/*[@class="et_atc_product"][@data-sku]/@data-sku')
            note = ','.join(i for i in text)
        except Exception as e:
            return None
        return note

    # ======================================================================== #
    # Citations表
    def sub_citations(self, html):
        results = []
        try:
            lis = html.xpath('.//li[@class="top-lined"]')
        except Exception as e:
            return results

        for item in lis:
            # PMID Species Article_title Sample_type Pubmed_url
            try:
                sub_species = item.xpath('./@data-species')[0].strip()
            except Exception as e:
                sub_species = None
            try:
                sub_sample_type = item.xpath('./@data-sampletype')[
                    0].strip()
            except Exception as e:
                sub_sample_type = None
            sub_title = item.xpath('.//span[@class="ctitle"]/a/text()')[
                0].strip()
            pm_url = item.xpath('.//span[@class="ctitle"]/a/@href')[
                0].strip()
            if 'www.ncbi.nlm.nih.gov/pubmed/' in pm_url:
                pmid = pm_url.split('gov/pubmed/')[1].strip()
            else:
                pmid = None
            results.append(
                [pmid, sub_species, sub_title, sub_sample_type, pm_url])
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, html):
        results = []
        linearity = html.xpath(
            './/div[@id="ds_linearity"][@class="datasheet_section"]')
        dataExamples = html.xpath('.//div[@class="row data_ex_row"]')

        try:
            sub_dec = linearity.xpath('.//h2/div/text()')[0].strip()
        except Exception as e:
            sub_dec = None
        try:
            sub_img = linearity.xpath('.//img[@src]')[0].strip()
            results.append([sub_img, sub_dec])
        except Exception as e:
            results = []

        for item in dataExamples:
            try:
                sub_dec = item.xpath('.//img[@class="img-fluid"]/@alt')[
                    0].strip()
            except Exception as e:
                sub_dec = None
            try:
                sub_img = item.xpath('.//img[@class="img-fluid"]/@src')[
                    0].strip()
                results.append([sub_img, sub_dec])
            except Exception as e:
                pass
        return results


if __name__ == '__main__':
    while r.exists('rnd_detail'):
        extract = r.rpop('rnd_detail')
        # extract = 'https://www.rndsystems.com/cn/products/protein-a-elisa-kit-for-engineered-variant_bppaev0'
        print(extract)
        try:
            lxml = RND().format(extract)
            # print(lxml)
        except Exception as e:
            r.lpush('rnd_detail', extract)
            continue
        if lxml is not None:
            brand = RND().brand()
            catalog_number = RND().catalog_number(lxml)
            product_name = RND().product_name(lxml)
            detail_url = RND().detail_url(extract)
            assay_type = RND().assay_type(lxml)
            sample_type = RND().sample_type(lxml)
            assay_length = RND().assay_length(lxml)
            sensitivity = RND().sensitivity(lxml)
            assay_range = RND().assay_range(lxml)
            specificity = RND().specificity(lxml)
            geneid = RND().geneid(lxml)
            datasheet_url = RND().datasheet_url(lxml)
            review = RND().review(lxml)
            image_qty = RND().image_qty(lxml)
            citations = RND().citations(lxml)
            synonyms = RND().synonyms(lxml)
            note = RND().note(lxml)

            sub_citations = RND().sub_citations(lxml)
            sub_images = RND().sub_images(lxml)
        else:
            r.lpush('rnd_detail', extract)
            continue
        new_detail = Detail(Brand=brand,
                            Catalog_Number=catalog_number,
                            Product_Name=product_name,
                            Detail_url=detail_url,
                            # Tests=tests,
                            Assay_type=assay_type,
                            Sample_type=sample_type,
                            Assay_length=assay_length,
                            Sensitivity=sensitivity,
                            Assay_range=assay_range,
                            Specificity=specificity,
                            GeneId=geneid,
                            # SwissProt=swissprot,
                            DataSheet_URL=datasheet_url,
                            Review=str(review),
                            Image_qty=image_qty,
                            Citations=str(citations),
                            Synonyms=synonyms,
                            Note=note)
        session.add(new_detail)

        if sub_citations:
            objects_sub_citations = []
            for sub in sub_citations:
                sub_pid = sub[0]
                sub_spe = sub[1]
                sub_tit = sub[2]
                sub_sam = sub[3]
                sub_pul = sub[4]

                new_citations = Citations(
                    Catalog_Number=catalog_number,
                    PMID=sub_pid,
                    Species=sub_spe,
                    Article_title=sub_tit,
                    Sample_type=sub_sam,
                    Pubmed_url=sub_pul
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = sub[0]
                dec = sub[1]

                new_images = Images(
                    Catalog_Number=catalog_number,
                    Image_url=img,
                    Image_description=dec
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)
        try:
            session.commit()
            session.close()
            print('done')
        except Exception as e:
            r.lpush('rnd_detail', extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(2, 2.5))
