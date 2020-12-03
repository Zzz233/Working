import requests
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time

Base = declarative_base()


class Citations(Base):
    __tablename__ = 'elisa_kit_citations_test'

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


class Detail(Base):
    __tablename__ = 'elisa_kit_detail_test'

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
    Review = Column(String(20),
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


class Images(Base):
    __tablename__ = 'elisa_kit_images_test'

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


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

# TODO detail citation images 映射  pdf直接获取字符串split。。。

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x6'
                  '4; rv:82.0) Gecko/20100101 Firefox/82.0'
}


class ElabScience():
    brand = 'elabscience'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/53'
                      '7.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
    }

    # 解析html
    def parse_html(self, url):
        with requests.Session() as session:
            resp = session.get(url=url, headers=self.headers)
            soup = BeautifulSoup(resp.text, 'html.parser')
            # print(soup)
        return soup, resp.text

    # 品牌
    def get_brand(self):
        return self.brand

    # 左上角图片
    def get_image(self, soup):
        images = soup.find(
            'div', id='ban_pic1').find_all('img')
        img_str = ''
        for image in images:
            if '/elisa/elisa' not in image['src']:
                img_str = 'https://www.elabscience.cn' + image[
                    'src'] + ',' + img_str

        return img_str.rstrip(',')

    # 货号
    def get_catalog_num(self, soup):
        catlog_str = soup.find('div', class_='txt_rgt').find('b').get_text()
        if '产品货号:' in catlog_str:
            catlog = catlog_str.replace('产品货号:', '')
            return catlog

    # 别称
    def get_aka(self, soup, html_str):
        agname = None
        if '别称:' in html_str:
            wha = soup.find('p', class_='wha').get_text()
            agname = wha.replace('别称:', '')
            return agname

    # 产品名称
    def get_product_name(self, soup):
        name = soup.find('h1').get_text()
        return name

    # detail_url
    def get_detail_url(self, url):
        return url

    # 检测原理
    def get_assay_type(self, soup, html_str):
        assey = None
        if '检测原理\n' in soup.get_text():
            assey = soup.get_text().split('检测原理\n')[1].strip().split('\n')[0]
        return assey

    # 灵敏度&检测范围
    def get_ziduan_fix(slef, soup, html_str):
        sen_str = None
        assay_str = None
        if '灵敏度\n' in html_str:
            p_str = soup.find('div', class_='ziduan fix').get_text()
            sen_str = p_str.split('灵敏度:')[1].strip()

        if '检测范围\n' and '灵敏度\n' in html_str:
            p_str = soup.find('div', class_='ziduan fix').get_text()
            assay_str = p_str.split('检测范围:')[1].split('灵敏度:')[0].strip()
        return sen_str, assay_str

    # 特异性
    def get_specificity(self, soup, html_str):
        specificity = None
        if '特异性\n' in soup.get_text():
            specificity = soup.get_text().split('特异性\n')[1].strip().split('\n')[
                0]
        return specificity

    # 规格
    def get_tests(self, html_str):
        tests = '0'
        if 'size_1' and 'size_2' in html_str:
            tests = '48T/96T'
        elif 'size_1' in html_str:
            tests = '96T'
        elif 'size_2' in html_str:
            tests = '48T'
        return tests

    # 波长
    def get_assay_length(self, html_str):
        assay_length = None
        if '酶标仪(450nm波长滤光片)' in html_str:
            assay_length = '450nm'
        return assay_length

    # 样本类型
    def get_sample_type(self, soup, html_str):
        sample_type = None

        if '样本类型' and '样本体积' in soup.get_text():
            sample_type = soup.get_text().split(
                '样本类型\n')[1].strip().split('\n')[0]
        print(sample_type)
        return sample_type

    def get_instruction(self, soup, html_str):
        instruction = None
        if '说明书' in html_str:
            instruction = 'https://www.elabscience.cn' + \
                          soup.find('a', class_='load same_sp pdfjs_btn')[
                              'href']
        return instruction


# pdf文献完整信息。。== pubmed_url
# def data_sheet_url(self, soup, html_str):
#     if '客户发表文献' in html_str:
#         pdf_soup = soup.find('ol', class_="publications")
#         pdfs = pdf_soup.find_all('li')
#         pdf_list = []
#
#         for single in pdfs:
#             pdf_title = single.find('a', rel='nofollow').get_text()
#             print(pdf_title)
#             pdf_link = single.find('a', rel='nofollow')['href']
#
#             p_text = single.get_text().replace('\n', '')
#             if 'PMID' in single.get_text():
#                 pm_id = p_text.split('PMID:')[1].split('引用产品:')[0].strip()
#             else:
#                 pm_id = None
#             species = p_text.split('物种:')[1].strip()
#             smaple_type = p_text.split('样本类型:')[1].split('物种:')[0].strip()
#             # catno = '一个产品货号'
#
#             result_text = pdf_title, pdf_link, pm_id, species, smaple_type
#             pdf_list.append(result_text)
#
#         return pdf_list


if __name__ == '__main__':
    for line in open('urls.csv'):
        link = line.replace('\n', '')
        # link = 'https://www.elabscience.cn/p-mouse_hs_crp_high_sensitivity_c_reactive_protein_elisa_kit-48844.html'
        # 解析html
        cooked_soup, html_text = ElabScience().parse_html(link)

        r_brand = ElabScience().get_brand()  # string

        r_image = ElabScience().get_image(cooked_soup)  # string 一个或多个图片链接

        r_catno = ElabScience().get_catalog_num(cooked_soup)  # string 货号

        r_synonyms = ElabScience().get_aka(cooked_soup, html_text)  # string 别称

        r_product_name = ElabScience().get_product_name(cooked_soup)  # string

        r_detail_url = ElabScience().get_detail_url(link)

        r_assay_type = ElabScience().get_assay_type(cooked_soup,
                                                    html_text)  # string 检测原理
        r_sensitivity, r_assay_range = ElabScience().get_ziduan_fix(
            cooked_soup, html_text)  # 灵敏度 检测范围、
        r_specificity = ElabScience().get_specificity(cooked_soup,
                                                      html_text)  # 特异性
        r_tests = ElabScience().get_tests(html_text)
        r_assay_length = ElabScience().get_assay_length(html_text)
        r_sample_type = ElabScience().get_sample_type(cooked_soup,
                                                      html_text)  # 样品类型

        # r_data_sheet_url = ElabScience().data_sheet_url(cooked_soup, html_text)
        r_data_sheet_url = ElabScience().get_instruction(cooked_soup, html_text)

        r_article_url = ''
        # if r_data_sheet_url is not None:
        #     for l in r_data_sheet_url:
        #         r_pmid = l[2]
        #         r_species = l[3]
        #         r_article_title = l[0]
        #         r_article_url = l[1] + ',' + r_article_url
        # else:
        #     r_pmid = None
        #     r_species = None
        #     r_article_title = None
        #     r_article_url = None
        # images
        new_image = Images(Catalog_Number=r_catno,
                           Image_url=r_image,
                           Kit_Status='0',
                           )
        # detial
        new_detail = Detail(Brand=r_brand,
                            Catalog_Number=r_catno,
                            Product_Name=r_product_name,
                            Detail_url=link,
                            Tests=r_tests,
                            Assay_type=r_assay_type,
                            Sample_type=r_sample_type,
                            Assay_length=r_assay_length,
                            Sensitivity=r_sensitivity,
                            Assay_range=r_assay_range,
                            Specificity=r_specificity,
                            DataSheet_URL=r_article_url,
                            Image_qty='0',
                            Synonyms=r_synonyms,
                            Kit_Status='0'
                            )
        
        session.add(new_image)
        session.add(new_detail)
        session.commit()
        session.close()
        print(link, 'done')
        time.sleep(1.5)
