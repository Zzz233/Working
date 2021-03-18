import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from Random_UserAgent import get_request_headers
import time
import random
import redis
from contextlib import contextmanager

Base = declarative_base()


class Application(Base):
    __tablename__ = "abcam_antibody_application"

    id = Column(Integer, primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40), nullable=True, comment='')
    Application = Column(String(1000), nullable=True, comment='')
    Dilution = Column(String(2000), nullable=True, comment='')
    Note = Column(String(500), nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment='')


class Citations(Base):
    __tablename__ = "abcam_antibody_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40), nullable=True, comment='')
    PMID = Column(String(40), nullable=True, comment='')
    Application = Column(String(300), nullable=True, comment='')
    Species = Column(String(100), nullable=True, comment='')
    Article_title = Column(String(1000), nullable=True, comment='')
    Pubmed_url = Column(String(1000), nullable=True, comment='')
    Note = Column(String(500), nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment='')


class Detail(Base):
    __tablename__ = "abcam_antibody_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40), nullable=True, comment='')
    Catalog_Number = Column(String(40), nullable=True, comment='')
    Product_Name = Column(String(200), nullable=True, comment='')
    Antibody_Type = Column(String(40), nullable=True, comment='')
    Sellable = Column(String(40), nullable=True, comment='')
    Synonyms = Column(String(3000), nullable=True, comment='')
    Application = Column(String(500), nullable=True, comment='')
    Conjugated = Column(String(200), nullable=True, comment='')
    Clone_Number = Column(String(40), nullable=True, comment='')
    Recombinant_Antibody = Column(String(10), nullable=True, comment='')
    Modified = Column(String(100), nullable=True, comment='')
    Host_Species = Column(String(20), nullable=True, comment='')
    Reactivity_Species = Column(String(20), nullable=True, comment='')
    Antibody_detail_URL = Column(String(500), nullable=True, comment='')
    Antibody_Status = Column(String(20), nullable=True, comment='')
    Price_Status = Column(String(20), nullable=True, comment='')
    Citations_Status = Column(String(20), nullable=True, comment='')
    GeneId = Column(String(500), nullable=True, comment='')
    KO_Validation = Column(String(10), nullable=True, comment='')
    Species_Reactivity = Column(String(1000), nullable=True, comment='')
    SwissProt = Column(String(500), nullable=True, comment='')
    Immunogen = Column(String(1000), nullable=True, comment='')
    Predicted_MW = Column(String(200), nullable=True, comment='')
    Observed_MW = Column(String(200), nullable=True, comment='')
    Isotype = Column(String(200), nullable=True, comment='')
    Purify = Column(String(200), nullable=True, comment='')
    Citations = Column(String(20), nullable=True, comment='')
    Citations_url = Column(String(500), nullable=True, comment='')
    DataSheet_URL = Column(String(500), nullable=True, comment='')
    Review = Column(String(20), nullable=True, comment='')
    Price_url = Column(String(500), nullable=True, comment='')
    Image_qty = Column(Integer, nullable=True, comment='')
    Image_url = Column(String(500), nullable=True, comment='')
    Note = Column(String(500), nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment='')


class Images(Base):
    __tablename__ = "abcam_antibody_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40), nullable=True, comment='')
    S_Image_url = Column(String(500), nullable=True, comment='')
    L_Image_url = Column(String(500), nullable=True, comment='')
    Image_description = Column(String(3000), nullable=True, comment='')
    Note = Column(String(500), nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment='')


class Price(Base):
    __tablename__ = "abcam_antibody_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40), nullable=True, comment='')
    sub_Catalog_Number = Column(String(40), nullable=True, comment='')
    Size = Column(String(50), nullable=True, comment='')
    Price = Column(String(50), nullable=True, comment='')
    Antibody_Status = Column(String(20), nullable=True, comment='')
    Note = Column(String(500), nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment='')


# Mysql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/2021_antibody_info?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)


class Abcam(object):
    def format(self, url, proxies):
        with requests.Session() as s:
            resp = s.get(url=url, headers=get_request_headers(), proxies=proxies,timeout=120)
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@id="frame"]')[
                0
            ]  # ! <xpath lxml>  None
        return content

    def parse(self, html, url):
        # todo Detail-Brand
        brand = 'abcam'

        # todo Detail-Catalog_Number
        try:
            cata_num = html.xpath(
                './/div[@id="datasheet-header-container"]/@data-product-code'
            )[0].strip()
        except Exception:
            cata_num = None
        print('货号： '+cata_num)

        # todo Detail-Product_Name
        try:
            name = html.xpath('.//h1[@class="title"]/text()')[0].strip()
        except Exception:
            name = None
        print('名字： '+name)

        # todo Detail-Antibody_Type
        try:
            antibody_type = html.xpath('.//h3[@class="name"][contains(text(), "克隆")]/../div[@class="value"]/text()')[
                0].strip()
            if '多克隆' in antibody_type:
                antibody_type = 'Polyclonal'
            elif '单克隆' in antibody_type:
                antibody_type = 'Monoclonal'
            else:
                antibody_type = None
        except Exception:
            antibody_type = None
        print('抗体类型： '+antibody_type)

        # todo Detail-Sellable
        sellable = html.xpath('.//div[@class="size-price-placeholder"]')
        if sellable:
            sellable = 'yes'
        else:
            sellable = 'no'
        print('是否可购： '+sellable)

        # todo Detail-Synonyms
        synonyms_list = html.xpath(
            './/h3[@class="name"][contains(text(), "别名")]/following-sibling::div[@class="value"]//li/text()'
        )
        synonyms = "|".join(i for i in synonyms_list)
        if len(synonyms) == 0:
            synonyms = None
        print('别名： '+synonyms)

        # todo Detail-Application
        application_list = html.xpath(
            './/h3[@class="name"][contains(text(), "经测试应用")]/following-sibling::div[@class="value"]/abbr/text()'
        )
        application = "|".join(l for l in application_list)
        if len(application) == 0:
            application = None
        print('应用： '+application)

        # todo Detail-Conjugated
        try:
            conjugation = html.xpath('.//section[@id="key-features"]/ul/li[contains(text(), "Conjugation:")]/text()')[
                0].replace('Conjugation:', '').strip()
        except:
            conjugation = None
        print('conjugation： '+conjugation)

        # todo Detail-Clone_Number
        try:
            clone_number = \
                html.xpath(
                    './/h3[@class="name"][contains(text(), "克隆编号")]/following-sibling::div[@class="value"]/text()')[
                    0].strip()
        except:
            clone_number = None
        print('clone_number： '+clone_number)

        # todo Detail-Recombinant_Antibody
        recombinant = html.xpath('.//a[@class="product-label product-label--recombinant"]')
        if recombinant:
            recombinant = 'yes'
        else:
            recombinant = 'no'
        print('recombinant： '+recombinant)

        # todo Detail-Modified
        try:
            modify = html.xpath(
                './/h3[@class="name"][contains(text(), "描述")]/following-sibling::div[@class="value"][contains(text(), "(")]/text()')[0].split('(')[-1].split(')')[0]
        except:
            modify = None
        print('modify： '+modify)

        # todo Detail-Host_Species
        host_list = html.xpath(
            './/h3[@class="name"][contains(text(), "宿主")]/following-sibling::div[@class="value"]//text()')
        if host_list:
            host = '|'.join(m.strip() for m in host_list)
        else:
            host = None
        print('host： '+host)

        # todo Detail-Reactivity_Species

        # todo Detail-Antibody_detail_URL
        antibody_url = url
        print('antibody_url： '+antibody_url)

        # todo Detail-Antibody_Status

        # todo Detail-Price_Status

        # todo Detail-Citations_Status

        # todo Detail-GeneId
        gene_id_list = html.xpath(
            './/h3[@class="name"][contains(text(), "数据库链接")]/following-sibling::div[@class="value"]/ul/li[contains(text(), "Human")]/a[contains(text(), "Entrez Gene:")]/text()')
        if gene_id_list:
            gene_id = '|'.join(n.split(' ')[-1] for n in gene_id_list)
        else:
            gene_id = None
        print('gene_id： '+gene_id)

        # todo Detail-KO_Validation
        ko_validation = html.xpath('.//img[@alt="使用敲除细胞株进行验证" or @title="使用敲除细胞株进行验证"]')
        if ko_validation:
            ko_validation = 'yes'
        else:
            ko_validation = 'no'
        print('ko_validation： '+ko_validation)

        # todo Detail-Species_Reactivity
        try:
            species_reactivity = \
                html.xpath(
                    './/h3[@class="name"][contains(text(), "种属反应性")]/following-sibling::div[@class="value"]/text()')[
                    0].strip()
        except:
            species_reactivity = None
        print('species_reactivity： '+species_reactivity)

        # todo Detail-SwissProt
        swissprot_list = html.xpath(
            './/h3[@class="name"][contains(text(), "数据库链接")]/following-sibling::div[@class="value"]/ul/li[contains(text(), "Human")]/a[contains(text(), "SwissProt:")]/text()')
        if gene_id_list:
            swissprot = '|'.join(n.split(' ')[-1] for n in swissprot_list)
        else:
            swissprot = None
        print('swissprot： '+swissprot)

        # todo Detail-Immunogen

        # todo Detail-Predicted_MW
        try:
            predicted_mw = html.xpath('.//b[contains(text(), "Predicted band size:")]/following-sibling::text()[1]')[
                0].strip()
        except:
            predicted_mw = None
        print(predicted_mw)

        # todo Detail-Observed_MW
        try:
            observed_mw = html.xpath('.//b[contains(text(), "Observed band size:")]/following-sibling::text()[1]')[
                0].strip()
        except:
            observed_mw = None
        print(observed_mw)

        # todo Detail-Isotype
        try:
            isotype = \
                html.xpath(
                    './/h3[@class="name"][contains(text(), "同种型")]/following-sibling::div[@class="value"]/text()')[
                    0].strip()
        except:
            isotype = None
        print(isotype)

        # todo Detail-Purify
        try:
            purify = \
                html.xpath(
                    './/h3[@class="name"][contains(text(), "纯度")]/following-sibling::div[@class="value"]/text()')[
                    0].strip()
        except:
            purify = None
        print(purify)

        # todo Detail-Citations
        try:
            citations = \
                html.xpath('.//a[@class="pws_publications pws_link "]/text()')[0].split('(')[-1].split(')')[0]
        except:
            citations = '0'
        print(citations)

        # todo Detail-Citations_url
        citations_url = 'https://www.abcam.cn/DatasheetProperties/References?productcode=' + cata_num
        print(citations_url)

        # todo Detail-DataSheet_URL
        try:
            datasheet_url = 'https://www.abcam.cn' + html.xpath('.//ul[@class="pdf-links"]/li[1]/a/@href')[0].strip()
        except:
            datasheet_url = None
        print(datasheet_url)

        # todo Detail-Review
        try:
            review = \
                html.xpath('.//p[@class="display_filter__msg"]/strong[contains(text(), "Abreviews or Q")]/text()')[
                    0].split(
                    ' ')[0].strip()
        except:
            review = '0'
        print(review)

        # todo Detail-Price_url
        price_url = 'https://www.abcam.cn/datasheetproperties/availability?abId=' + cata_num.replace('ab', '')
        print(price_url)

        # todo Detail-Image_qty
        image_qty = len(html.xpath('.//ul[@class="images"]/li/div[@class="column image gallery"]/a/@href'))
        print(image_qty)

        # todo Detail-Image_url

        # todo Detail-Note

        # todo Application表
        app_list = []
        trs = html.xpath('.//div[@id="description_applications"]/div[@class="acc__content"]/table/tbody/tr')
        for tr in trs:
            try:
                appl = tr.xpath('./td[@class="name"]/abbr/text()')[0].strip()
                dilution = tr.xpath('./td[@class="value value2--addon"]/text()')[0].strip()
                app_list.append([appl, dilution])
            except:
                pass
        print(app_list)

        # todo Images表
        img_list = []
        lis = html.xpath('.//ul[@class="images"]/li')
        for li in lis:
            try:
                l_img_url = li.xpath('.//div[@class="column image gallery"]/a/@href')[0].strip()
                s_img_url = l_img_url + '?imwidth=70'
                img_desc_list = li.xpath('.//div[@class="column description"]//text()')
                if img_desc_list:
                    img_desc = ''.join(desc for desc in img_desc_list)
                else:
                    img_desc = None
                img_list.append([s_img_url, l_img_url, img_desc])
            except:
                pass
        print(img_list)

        detail_obj = Detail(
            Brand=brand,
            Catalog_Number=cata_num,
            Product_Name=name,
            Antibody_Type=antibody_type,
            Sellable=sellable,
            Synonyms=synonyms,
            Application=application,
            Conjugated=conjugation,
            Clone_Number=clone_number,
            Recombinant_Antibody=recombinant,
            Modified=modify,
            Host_Species=host,
            Antibody_detail_URL=antibody_url,
            GeneId=gene_id,
            KO_Validation=ko_validation,
            Species_Reactivity=species_reactivity,
            SwissProt=swissprot,
            Predicted_MW=predicted_mw,
            Observed_MW=observed_mw,
            Isotype=isotype,
            Purify=purify,
            Citations=citations,
            Citations_url=citations_url,
            DataSheet_URL=datasheet_url,
            Review=review,
            Price_url=price_url,
            Image_qty=image_qty
        )
        app_objs = []
        for app_item in app_list:
            new_application = Application(
                Catalog_Number=cata_num,
                Application=app_item[0],
                Dilution=app_item[1]
            )
            app_objs.append(new_application)

        img_objs = []
        for img_item in img_list:
            new_images = Images(
                Catalog_Number=cata_num,
                S_Image_url=img_item[0],
                L_Image_url=img_item[1],
                Image_description=img_item[2]
            )
            img_objs.append(new_images)

        return detail_obj, app_objs, img_objs

    @contextmanager
    def session_maker(self, session=session):
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def insert(self, detail_obj, app_objs, img_objs):
        with self.session_maker() as db_session:
            db_session.add(detail_obj)
            db_session.bulk_save_objects(app_objs)
            db_session.bulk_save_objects(img_objs)

    def get_proxy(self):
        proxy_url = 'http://localhost:16888/random?protocol=http'
        content = requests.get(url=proxy_url).text.strip()
        proxies = {
            "http": content,
            # "https": content,
        }
        print("获取新代理", content)
        return proxies

    def run(self):
        proxies = self.get_proxy()
        while r.exists('abcam'):
            # url = r.lpop('abcam')
            url = 'https://www.abcam.cn/sox17-antibody-epr20684-ab224637.html'
            print('开始' + ' ' + url)
            try:
                html = self.format(url, proxies)
            except Exception as e:
                print(e)
                r.rpush('abcam', url)
                proxies = self.get_proxy()
                continue
            try:
                detail_obj, app_objs, img_objs = self.parse(html, url)
            except Exception as e:
                r.rpush('abcam', url)
                print(e)
                continue
            break
            # try:
            #     self.insert(detail_obj, app_objs, img_objs)
            # except Exception as e:
            #     print(e)
            #     r.rpush('abcam', url)
            #     # time.sleep(random.uniform(3.0, 3.5))
            #     continue
            # time.sleep(random.uniform(3.0, 3.5))


if __name__ == '__main__':
    test = Abcam()
    test.run()
