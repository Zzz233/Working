import requests
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x6'
                      '4; rv:82.0) Gecko/20100101 Firefox/82.0'
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
            'div', class_='img_big').find_all(
            'img', attrs={
                "alt": "sandwich-Ab-ELISA-Elabscience"
            }
        )
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
    def get_aka(self, soup):
        wha = soup.find('p', class_='wha').get_text()
        if '别称:' in wha:
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
    def get_assay_type(self, soup):
        assey = soup.find_all(
            'div', class_='dt01')[1].find(
            'div', class_='details_atcl').p.get_text().replace('\n',
                                                               '').replace('\t',
                                                                           '')

        return assey

    # 灵敏度&检测范围
    def get_ziduan_fix(slef, soup):
        p_str = soup.find('div', class_='ziduan fix').get_text()
        sen_str = p_str.split('灵敏度:')[1].strip()
        assay_str = p_str.split('检测范围:')[1].split('灵敏度:')[0].strip()
        return sen_str, assay_str

    # 特异性
    def get_specificity(self, soup):
        specificity = '0'
        specificity_str = soup.find_all('div', class_="dt01")[2]
        if '特异性' in specificity_str.find('span').get_text():
            specificity = specificity_str.find('p').get_text()
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
        assay_length = '0'
        if '酶标仪(450nm波长滤光片)' in html_str:
            assay_length = '450nm'
        return assay_length

    def get_sample_type(self, soup, html_str):
        sample_type = '0'
        if '样本类型' in html_str:
            sample_type = soup.find_all(
                'div', class_='dt01')[1].get_text().split('样本类型')[1].strip()
        return sample_type

    # pdf文献完整信息。。== pubmed_url
    def data_sheet_url(self, soup, html_str):
        if '客户发表文献' in html_str:
            pdf_soup = soup.find('ol', class_="publications")
            pdfs = pdf_soup.find_all('li')
            pdf_list = []
            for single in pdfs:
                pdf_title = single.find('a', rel='nofollow').get_text()
                pdf_link = single.find('a', rel='nofollow')['href']

                p_text = single.get_text().replace('\n', '')
                pm_id = p_text.split('PMID:')[1].split('引用产品:')[0].strip()
                species = p_text.split('物种:')[1].strip()
                smaple_type = p_text.split('样本类型:')[1].split('物种:')[0].strip()
                # catno = '一个产品货号'

                result_text = pdf_title, pdf_link, pm_id, species, smaple_type
                pdf_list.append(result_text)

            return pdf_list


if __name__ == '__main__':
    link = 'https://www.elabscience.cn/p-quickey_human_crp_c_reactive_protein_elisa_kit-201257.html'
    link2 = 'https://www.elabscience.cn/p-human_pla2g10_phospholipase_a2_group_x_elisa_kit-46859.html'

    # 解析html
    cooked_soup, html_text = ElabScience().parse_html(link)

    r_brand = ElabScience().get_brand()  # string
    r_image = ElabScience().get_image(cooked_soup)  # string 一个或多个图片链接
    r_catno = ElabScience().get_catalog_num(cooked_soup)  # string 货号
    r_synonyms = ElabScience().get_aka(cooked_soup)  # string 别称
    r_product_name = ElabScience().get_product_name(cooked_soup)  # string
    r_detail_url = ElabScience().get_detail_url(link)
    r_assay_type = ElabScience().get_assay_type(cooked_soup)  # string 检测原理
    r_sensitivity, r_assay_range = ElabScience().get_ziduan_fix(
        cooked_soup)  # 灵敏度 检测范围、
    r_specificity = ElabScience().get_specificity(cooked_soup)  # 特异性
    r_tests = ElabScience().get_tests(html_text)
    r_assay_length = ElabScience().get_assay_length(html_text)
    r_sample_type = ElabScience().get_sample_type(cooked_soup, html_text)
    r_data_sheet_url = ElabScience().data_sheet_url(cooked_soup, html_text)

    r_article_url = ''
    if r_data_sheet_url is not None:
        for l in r_data_sheet_url:
            r_pmid = l[2]
            r_species = l[3]
            r_article_title = l[0]
            r_article_url = l[1] + ',' + r_article_url
    else:
        r_pmid = None
        r_species = None
        r_article_title = None
        r_article_url = None
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
    session.add(new_detail)
    session.commit()
    session.close()
    print('done')

    # citations.txt
    # if len(r_data_sheet_url) is not None:
    #     for l in r_data_sheet_url:
    #         r_pmid = l[2]
    #         r_species = l[3]
    #         r_article_title = l[0]
    #         r_article_url = l[2]
    #         with open('citations.txt', 'a') as f:  # 追加
    #             f.write(r_catno, r_pmid + '\n')
