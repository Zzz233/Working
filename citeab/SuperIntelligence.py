import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_, and_
from sqlalchemy.sql import func
import redis
import random
import time

Base = declarative_base()


class Data(Base):
    __tablename__ = 'citeab_citations2'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(400),
                            nullable=True, comment='')
    Company = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    pmid = Column(String(100),
                  nullable=True, comment='')
    citeab_href = Column(String(500),
                         nullable=True, comment='')
    application = Column(String(300),
                         nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Pdf_url = Column(String(1000),
                     nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class List(Base):
    __tablename__ = 'citeab_citations_list_urls'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Company = Column(String(100),
                     nullable=True, comment='')
    list_url = Column(String(1000),
                      nullable=True, comment='')
    url_status = Column(String(20),
                        nullable=True, comment='')
    Crawl_Date = Column(DateTime,
                        nullable=True, comment='')

    def to_dict(self):
        return {
            'id': self.id,
            'Company': self.Company,
            'list_url': self.list_url,
            'url_status': self.url_status,
            'Crawl_Date': self.Crawl_Date,
        }


engine = create_engine(
    'mysql+pymysql://root:app1234@192.168.124.10:3306/citeab?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Citeab():

    # 多贝云
    def get_proxy(self):
        ip_url = 'http://http.tiqu.alicdns.com/getip3?num=1&type=1&pro' \
                 '=&city=0&yys=0&port=11&time=1&ts=0&ys=0&cs=0&lb=1&sb' \
                 '=0&pb=4&mr=2&regions=&gm=4'
        ip, port = requests.get(url=ip_url).text.strip().split(':')
        proxies = {
            "http": "http://" + ip + ':' + port,
            'https': "https://" + ip + ':' + port
        }
        return ip, port, proxies

    # selenium 获取cookie
    def get_cookie(self, url, ip, port):
        profile = webdriver.FirefoxProfile()
        port = int(port)
        settings = {
            'network.proxy.type': 1,  # 0: 不使用代理；1: 手动配置代理
            'network.proxy.http': ip,
            'network.proxy.http_port': port,
            'network.proxy.ssl': ip,  # https的网站,
            'network.proxy.ssl_port': port,
        }
        for key, value in settings.items():
            profile.set_preference(key, value)
        profile.update_preferences()
        options = Options()
        firefox = webdriver.Firefox(firefox_profile=profile, options=options)
        firefox.get(url)
        input("通过验证码跳转后:")

        cookies = firefox.get_cookies()
        cookie = [item["name"] + "=" + item["value"] for item in cookies]
        cookie_str = '; '.join(item for item in cookie)
        firefox.quit()
        print(cookie_str)
        return cookie_str

    def format_html(self, url, proxies, cookie):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q'
                      '=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en'
                               '-US;q=0.3,en;q=0.2',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '',
            'Host': 'www.citeab.com',
            'Pragma': 'no-cache',
            'Referer': 'https://www.citeab.com/antibodies/2041744-11-5773-'
                       'foxp3-monoclonal-antibody-fjk-16s-fitc-e/publicati'
                       'ons?page=11',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.'
                          '0) Gecko/20100101 Firefox/82.0',
        }
        with requests.Session() as s:
            headers['Cookie'] = cookie
            resp = s.get(url=url, headers=headers, proxies=proxies, timeout=30)
            print(resp.text)
            html = etree.HTML(resp.text)
            print(headers)
            # TODO 条件
            if html.xpath('//div[@class="citation"]'):
                status = 1
            else:
                status = 0
        return status, html

    def spider(self, html):
        data_list = []

        # TODO catno, company, citationQty
        catno = html.xpath(
            '//h1[contains(text(),"Citations for")]/strong/text()')[0]
        company = html.xpath(
            '//h1[contains(text(),"Citations for")]/strong/text()')[1]
        # citationQty = int(html.xpath(
        #     '//h4[contains(text(),"reference this Antibody")]/text()')[
        #                       0].split(' publication')[0].replace(',', ''))

        # TODO article_title, pmid, citeab_href, appliction, species, pdf_url
        citations = html.xpath('//div[@class="citation"]')
        for citation in citations:
            article_title = citation.xpath('.//p[@class="title"]/a/text()')
            pmid = citation.xpath('.//p[@class="title"]/a/@href')[0].split('-')[
                1]
            citeab_href = citation.xpath('.//p[@class="title"]/a/@href')[0]
            if citation.xpath('.//abbr[@class="app-abbr"]/text()'):
                application = citation.xpath(
                    './/abbr[@class="app-abbr"]/text()')
            else:
                application = None
            if citation.xpath('.//span/text()'):
                species = citation.xpath('.//span/text()')
            else:
                species = None
            if citation.xpath('.//div[@class="cell shrink"]/a/@href'):
                pdf_url = citation.xpath('.//div[@class="cell shrink"]/a/@href')
            else:
                pdf_url = None
            result = [catno, company, article_title, pmid,
                      citeab_href, application, species, pdf_url]
            data_list.append(result)

        return data_list


def main():
    pool = redis.ConnectionPool(host='localhost', port=6379,
                                decode_responses=True,
                                db=0)
    r = redis.Redis(connection_pool=pool)
    task_list = session.query(List.id, List.list_url).filter(
        and_(List.url_status == '0',
             or_(List.Company == 'BD Biosciences', List.Company == 'BioLegend'))
    ).all()
    print(len(task_list))

    while task_list:
        task_item = task_list.pop()
        cookie_link = task_item.list_url
        # crawler_link = 'https://www.citeab.com/antibodies/826371-sc-418
        # -rho-a-antibody-26c4/publications?page=40'
        r_ip, r_port, r_proxies = Citeab().get_proxy()
        print(r_proxies)
        r_cookie = Citeab().get_cookie(cookie_link, r_ip, r_port)

        # for i in task_list:
        #     print('开始循环')
        #     task_url = i.list_url
        #     task_id = i.id
        #
        #     try:
        #         # 访问详细页
        #         r_status, r_html = Citeab().format_html(task_url, r_proxies,
        #                                                 r_cookie)
        #     except Exception as e:
        #         print('重新获取', e)
        #         continue
        #     if r_status == 1:
        #         r_result = Citeab().spider(r_html)
        #         objects = []
        #         for n in r_result:
        #             new_data = Data(Catalog_Number=n[0],
        #                             Company=n[1],
        #                             Article_title=n[2],
        #                             pmid=n[3],
        #                             citeab_href=n[4],
        #                             application=n[5],
        #                             Species=n[6],
        #                             Pdf_url=n[7])
        #             objects.append(new_data)
        #         try:
        #             session.bulk_save_objects(objects)
        #             session.commit()
        #             session.close()
        #             print('done')
        #         except Exception as e:
        #             session.rollback()
        #             print(e)
        #     else:
        #         print('Error')
        #     break
        # time.sleep(random.uniform(2.5, 5.5))
        break


if __name__ == '__main__':
    main()
