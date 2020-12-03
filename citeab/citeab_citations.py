import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
import random
import time

Base = declarative_base()


class Data(Base):
    __tablename__ = 'citeab_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    company = Column(String(45),
                     nullable=True, comment='')
    catno = Column(String(45),
                   nullable=True, comment='')
    citationQty = Column(Integer,
                         nullable=True, comment='')
    title = Column(String(300),
                   nullable=True, comment='')
    title_link = Column(String(300),
                        nullable=True, comment='')
    pdf_link = Column(String(300),
                      nullable=True, comment='')
    journal = Column(String(300),
                     nullable=True, comment='')
    public_date = Column(String(50),
                         nullable=True, comment='')
    application = Column(String(200),
                         nullable=True, comment='')
    reactivity = Column(String(200),
                        nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Citeab():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': '__utma=257053465.1196203676.1604990728.1604990728.1604990728.1; __utmc=257053465; __utmz=257053465.1604990728.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=257053465.2.10.1604990728; _citeab-live-session=%2FF83cGDqoGInIucQh3kQq6U5Dj1pLMdJfpKne0pz3yLyaOTRd%2Ff0OrKAAHrobjEnl5zuHMNU7RTogfPKrkoDLNbdRdV8W0pRn4V%2FaiTpUmaMTCDZRm5GzD3bxMRd4oLuKQYpGhivxZdYtYAEq4e%2F%2BpHLP7CS2LP5xKxNAlRzhRnE19vV7fYoFBWnPuFXSXWrQl1rDIsvORLOHH1mO7%2Bb5Br0CKeFBzL92MqKYBpjFSHcKvaWTdDf%2Fh3LKWE6dd%2BKCC6UaCyLuLf6tEwvigaDRnPIPSYLhafo92LKmwRj%2Fd58fDC76tx%2BWXZyg5vGt1buQcMCNHBprMnFwTeTUXM5n5u7kZVEk%2BuNKt03ehEbvpUa8BLFT4xryG13wwZfNkMDgCbA%2BGNGAhYkAQMU7YBkiibHxS11Cq0KaRu0Kq7Vh0ABSGHKe9G7qGwMgaUuOx2%2BXYDNRRrozb15zM0AE8zPoiuRmOevJItKSw%2Bx6dCIHSogtTRsOQZwkuv6ie25pxEs%2BcKj%2F4OZXMNp%2FcrnQsepCHsbMyZAE3UqnfZEp8Vi5KGftGIt2Kl1BMPwtQeyzF9S1itq8G3Vni4SkCimtDf6KA2iSMFXt0koCqBogjPavdpDiYO7WuABPyGmF5jFLCd%2BYafu4mPSac%2B1czAWv5wfCbF%2BeqSH%2BOQz4H%2F87QQ2CGnbFzC4BRTsk%2F17sum5jIceYE6CMhfOSiTGHAAhF6JSJaKCWhaZ4vgL9vrASx1SLZvYXD3POFT5KCVyHeftV7s%2Fb6esNsyb8lHrsk5WR9CyuvpDTzs34OdPlUYs6nLrqJYWm35%2FgeWPNTvzmJNsgYgkgFx7VF2IvaYsGkT%2Fw0UmzU1PFioifi%2BpF68HOSlrH26%2BsTyD4oMEWe7Ci7vEIOmWIM7QTHwwvpERsy%2FOjk8wAhynZJbLNRTzzctZihTPGSSvAQXVjHr0gS1B825fDh0QSwpviij%2BEdcNjrqMcr2umdJzJdVBXrC%2B5P8J5SrbBPQ3%2BV1YLIhPMzsnOaXrWw3R--TszGe88moHYahu%2By--c6HMUTtiwJCPAbHKS%2BGwBw%3D%3D',
        'Host': 'www.citeab.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.citeab.com/accounts/sign_in',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
    }

    # 解析html
    def parse_html(self, url):
        with requests.Session() as s:
            # proxies = {'http': '49.88.245.164:45257',
            #            'https': '49.88.245.164:45257'}
            resp = s.get(url=url, headers=self.headers)
            html = etree.HTML(resp.text)
            status = 0
            if html.xpath(
                    '//h5[contains(text(),"re a human")]'):
                status = 1
        return html, status

    def get_top(self, html):
        # company, catno, citationQty
        catno = html.xpath(
            '//h1[contains(text(),"Citations for")]/strong/text()')[0]
        company = html.xpath(
            '//h1[contains(text(),"Citations for")]/strong/text()')[1]
        citationQty = int(html.xpath(
            '//h4[contains(text(),"reference this Antibody")]/text()')[
                              0].split(' publication')[0].replace(',', ''))
        return company, catno, citationQty

    def get_bottom(self, html, company, catno, citationQty):
        data_list = []
        citations = html.xpath('//div[@class="citation"]')
        for citation in citations:
            title = citation.xpath('.//p[@class="title"]/a/text()')[0]
            title_url = 'https://www.citeab.com' + citation.xpath(
                './/p[@class="title"]/a/@href')[0]
            pdf_url = '0'
            if citation.xpath('.//div[@class="cell shrink"]/a/@href'):
                pdf_url = citation.xpath(
                    './/div[@class="cell shrink"]/a/@href')[0]
            journal = citation.xpath(
                './/p[@class="authors"]/b/text()')[0]
            public_date = citation.xpath(
                './/p[@class="authors"]/text()')[1].strip().split('\n')[0]

            if citation.xpath(
                    './/div[@class="cell medium-3"]/b[contains(text(),"Applications:")]'):
                application = str(citation.xpath(
                    './/div[@class="cell medium-3"]/b[contains(text(),"Applications:")]/../following-sibling::div[1]//text()')).replace(
                    '\\n', '').replace(
                    '\'', '').replace(
                    '[', '').replace(
                    ']', '').replace(
                    ',', '').strip()
            else:
                application = '0'

            if citation.xpath(
                    './/div[@class="cell medium-3"]/b[contains(text(),"Reactivity:")]'):
                reactivity = str(citation.xpath(
                    './/div[@class="cell medium-3"]/b[contains(text(),"Reactivity:")]/../following-sibling::div[1]//text()')).replace(
                    '\\n', '').replace(
                    '\'', '').replace(
                    '[', '').replace(
                    ']', '').replace(
                    ',', '').strip()
            else:
                reactivity = '0'
            result = [company, catno, citationQty, title, title_url, pdf_url,
                      journal, public_date, application, reactivity]
            data_list.append(result)

        return data_list


# def get_new_ip():
#     ip_resp = requests.get(
#         'http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all')
#     global resp_ip, resp_port, resp_msg
#     resp_ip = ip_resp.json()['data'][0]['ip']
#     resp_port = ip_resp.json()['data'][0]['port']
#     resp_msg = ip_resp.json()['msg']
#     proxies = {
#         'http': resp_ip + ":" + resp_port,
#         'https': resp_ip + ":" + resp_port
#     }
#     print(proxies)
#     return proxies, resp_msg


if __name__ == '__main__':
    # r_proxies, r_resp_msg = get_new_ip()
    for line in open('citeab_ceshi.csv'):
        link = line.split(',')[2]
        print(link)
        r_html, r_status_code = Citeab().parse_html(link)
        if r_status_code == 1:
            print('遇到机器人')
            # r_proxies, r_resp_msg = get_new_ip()
            # print(r_resp_msg)
            break
        r_company, r_catno, r_citationQty = Citeab().get_top(r_html)
        r_result = Citeab().get_bottom(r_html, r_company, r_catno,
                                       r_citationQty)
        print(r_result)
        # for n in r_result:
        #     new_data = Data(company=n[0], catno=n[1],
        #                     citationQty=n[2], title=n[3],
        #                     title_link=n[4], pdf_link=n[5],
        #                     journal=n[6], public_date=n[7],
        #                     application=n[8], reactivity=n[9])
        #     try:
        #         session.add(new_data)
        #         session.commit()
        #         session.close()
        #
        #     except Exception as e:
        #         session.rollback()
        #         print(e)

        print(link)

        time.sleep(random.randint(30, 40))
