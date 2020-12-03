import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


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


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Abbkine():
    brand = 'Abbkine'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': '__tins__19011748=%7B%22sid%22%3A%201604621429913%2C%20%22vd%22%3A%202%2C%20%22expires%22%3A%201604624803616%7D; __51cke__=; __51laig__=2; wp_xh_session_8ce7451aa12a2213a89e614aaf6dd2a4=04a8666237bdacc5c0c2f0f920bbc1ad%7C%7C1604794307%7C%7C1604790707%7C%7C5702c6a82f06ded42059146dcb9186c4; tk_ai=woo%3AQqtHtSGI%2BPAVUWjms5O3XPZ5',
        'Host': 'www.abbkine.cn',
        'Pragma': 'no-cache',
        'Referer': 'http://www.abbkine.cn/products?productcat=75',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
    }

    def parse_html(self, url):
        resp = requests.get(url=url, headers=self.headers)
        html = etree.HTML(resp.text)
        return html

    # list
    # def get_url(self, html):
    #     for item in html.xpath('//div[@class="productlistleft fl"]'):
    #         # print(item)
    #         product_url = item.xpath('./h3/a/@href')[0]
    #
    #         print(product_url)
    #         # print(l + "," + product_url)
    #         # with open('urls.txt', 'a') as file:
    #         #     file.write(l + '\n')
    #     return 'done'

    # Brand
    def get_brand(self):
        return self.brand

    # catno
    def get_catalog_number(self, html):
        html.xpaht()

    def get_product_name(self, html):
        name = str(html.xpath('//h4/text()')[0]).replace(
            '\n', '').replace('\r', '').replace(' ', '')
        return name

    def get_detail_url(self, link):
        return link

    # 规格 48T 96T
    def get_tests(self, html):
        value = ''
        for item in html.xpath('//select[@name="attribute_pa_size"]/option'):
            value = value + '/' + item.xpath('./@value')[0].strip()
        return value.lstrip('/')

    # 试验类型
    def get_assay_type(self, html):
        assay_type = html.xpath(
            '//td[contains(text(), "试验类型")]/following-sibling::td[1]/text()')[0]
        return assay_type

    def get_sample_type(self, html):
        sample_type = html.xpath(
            '//td[contains(text(), "样品类型")]/following-sibling::td[1]/text()')[0]
        return sample_type

    def get_sensitivity(self, html):
        sensitivity = None
        if html.xpath(
                '//td[contains(text(), "灵敏度")]/following-sibling::td[1]/text()'):
            sensitivity = html.xpath(
                '//td[contains(text(), "灵敏度")]/following-sibling::td[1]/text()')[
                0]
        return sensitivity

    def get_specificity(self, html):
        specificity = None
        if html.xpath(
                '//td[contains(text(), "特点&优势")]/following-sibling::td[1]/text()'):
            specificity = html.xpath(
                '//td[contains(text(), "特点&优势")]/following-sibling::td[1]/text()')[
                0]
        return specificity

    def get_geneid(self, html):
        geneid = None
        if html.xpath(
                '//td[contains(text(), "基因ID")]/following-sibling::td[1]/a/text()'):
            geneid = html.xpath(
                '//td[contains(text(), "基因ID")]/following-sibling::td[1]/a/text()')[
                0]
        return geneid

    # uniprot
    def get_swissprot(self, html):
        swissprot = None
        if html.xpath(
                '//td[contains(text(), "蛋白质ID")]/following-sibling::td[1]/a/text()'):
            swissprot = html.xpath(
                '//td[contains(text(), "蛋白质ID")]/following-sibling::td[1]/a/text()')[
                0]
        return swissprot

    def get_datasheet_url(self, html):
        datasheet_url = html.xpath('//a[@class="prodown"]/@href')[0]
        return datasheet_url

    def get_review(self, html):
        review = \
            html.xpath('//span[contains(text(),"用户评论")]/text()')[0].split('(')[
                1].split(
                ')')[0]
        return review

    def get_image_qty(self):
        return 0

    def get_citations(self, html):
        citations = \
            html.xpath('//li/span[contains(text(),"文献引用")]/text()')[0].split(
                '(')[
                1].split(
                ')')[0]
        return citations

    def get_synonyms(self, html):
        synonyms = None
        if html.xpath(
                '//td[contains(text(), "别名")]/following-sibling::td[1]/text()'):
            synonyms = html.xpath(
                '//td[contains(text(), "别名")]/following-sibling::td[1]/text()')[
                0]
        return synonyms

    def get_kit_status(self):
        return 0

    def note(self):
        return None


if __name__ == '__main__':
    for line in open('url_debug.txt', encoding='utf-8'):
        link = line.strip('\n')
        r_html = Abbkine().parse_html(link)
        # r_url = Abbkine().get_url(r_html)
        r_name = Abbkine().get_product_name(r_html)
        r_tests = Abbkine().get_tests(r_html)
        r_assay_type = Abbkine().get_assay_type(r_html)
        r_sample_type = Abbkine().get_sample_type(r_html)
        r_sensitivity = Abbkine().get_sensitivity(r_html)
        r_specificity = Abbkine().get_specificity(r_html)
        r_geneid = Abbkine().get_geneid(r_html)
        r_swissprot = Abbkine().get_swissprot(r_html)
        r_datasheet_url = Abbkine().get_datasheet_url(r_html)
        r_review = Abbkine().get_review(r_html)
        r_citations = Abbkine().get_citations(r_html)
        r_synonyms = Abbkine().get_synonyms(r_html)
        # new_detail = Detail(Brand='Abbkine',
        #                     Catalog_Number=link.split('/')[-1],
        #                     Product_Name=r_name,
        #                     Detail_url=link,
        #                     Tests=r_tests,
        #                     Assay_type=r_assay_type,
        #                     Sample_type=r_sample_type,
        #                     Assay_length=None,
        #                     Sensitivity=r_sensitivity,
        #                     Assay_range=None,
        #                     Specificity=r_specificity,
        #                     GeneId=r_geneid,
        #                     SwissProt=r_swissprot,
        #                     DataSheet_URL=r_datasheet_url,
        #                     Image_qty='0',
        #                     Citations=r_citations,
        #                     Synonyms=r_synonyms,
        #                     Kit_Status='0',
        #                     Note=None
        #                     )
        # session.add(new_detail)
        session.commit()
        session.close()
        print('done')
