'''TODO
s = Session()
objects = [
    User(name="u1"),
    User(name="u2"),
    User(name="u3")
]
s.bulk_save_objects(objects)
s.commit()'''
import requests
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
import time
import random

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Cookie': '__utmz=257053465.1603690376.1.1.utmcsr=cn.bing.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utma=257053465.1093246679.1603690376.1603766770.1603780901.5; __utmc=257053465; _citeab-live-session=g27WJdfc0mF25XC94qa96dcD2M7Hv6SihCiD6YL4nx%2FnQdVrTWR7QDhNqfvYW9Ats%2FOZl5DpmJSm3m9%2FPfS2dAeyKu4bVhPep4q4dQwn6LHPnJi8J5GUCtMMVcfc0aAisRXQQ3L6jOno6CaQMn3SBZ7FaZJeDbE8w5Oo00v2blGpQ7argD6WJD8C7VzWMQqS0vX1gnADlia9kSkoOBuspDRo7S6ow5cY29fHfvla93a2GYob2mrwPmGM0r%2BTgaPJQLxldlfa9Cxxwd7BiFrwUQrxGgfKzXaySaXP5dwvyGPIOVvvPDyqlFHpLyDpR2cYdRnZRqjNkjnrcWLqpZw32rC2VkqQ4gWqF%2BMLToUHDg6U1Yxt90xbTNpyMAhZecWP87AVOjIzfkB5mMzv6NqwfkO0EQV04S7oRDApvHb8nHxXWcQyazTSxDlJo9PrmuVqAEwJP0TYihNs2F%2BMiywsyFnMJODE%2Fk7D4vv%2BL6ZTt9kQcQwu0niTJ0yI%2FYhsDRmFldfmWqGBPBPGZxlTQuiH9kliHrZCFLrKH5kKc6zya8IgymDd%2ByMZdaYH%2B2qm37y%2FUWKAbq7Hg%2Ft7IZD6pMzhUhO4skR2RAZ%2Bi%2B2O8ju52A%2BnVWN2TdwxhD3uBC6WH4w0sDX1lKAM8TfWVZabXurBNW9CCNe%2BV2ABy86dFOZ52t9ANBfALbor7s1VwOzyM5V2yvwOvvW1bbfLB1WrKU8QIVLJxBf37H3Omnc1x5Ap89jaGgDkxlREtExazxaI0P7K3SdnIDkGwL%2BzUwDOrYER5%2FGy4KAnitWJ%2BMQ4FEUAzaaNOOTohrawMQhi3hqj%2Fn0w3dMUN7OPZiyZ28vvwmrSbtijS6PlNRHYd3%2BvEmH%2F2qieiOo1TQ3mdDgshaIOFsYd8A4AZ46P4E%2BGEpl%2B3RGB22suTzqdnUe8e79HbmguDzz8avrHuRh2f7ae%2FQEQfdHhr4bsT4mCe%2FjbfkPmAmg9lm9Wo2ulIIn72xsT2vuW2kCxpuREfT9u%2FX4Vcb7oRiak--%2Bz5sy9z0JIpalF%2FB--yqdc8l96RPz%2BlAryGPByfw%3D%3D',
    'Host': 'www.citeab.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.citeab.com/antibodies',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
}

working_list = [
    'https://www.citeab.com/antibodies/1473591-mabd64-anti-achaete-scute-homolog-1-antibody-clone/publications?page=1',
    'https://www.citeab.com/antibodies/1072326-orb10143-mash1-achaete-scute-homolog-1-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/1665845-bs-1155r-ascl1-polyclonal-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/1488995-hpa029217-anti-ascl1/publications?page=1',
    'https://www.citeab.com/antibodies/82835-61271-mash1-antibody-mab/publications?page=1',
    'https://www.citeab.com/antibodies/1472806-gp155-anti-arvcf-guinea-pig-polyclonal-serum/publications?page=1',
    'https://www.citeab.com/antibodies/787998-sc-23874-arvcf-antibody-4b1/publications?page=1',
    'https://www.citeab.com/antibodies/252364-h00000421-m01-arvcf-monoclonal-antibody-m01-clone-5d2/publications?page=1',
    'https://www.citeab.com/antibodies/657581-a303-310a-arvcf-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/407246-h00000421-m01-arvcf-antibody-5d2/publications?page=1',
    'https://www.citeab.com/antibodies/2413996-556604-purified-mouse-anti-mash1/publications?page=8',
    'https://www.citeab.com/antibodies/662955-as05-059-anti-psbr-10-kda-protein-of-psii/publications?page=1',
    'https://www.citeab.com/antibodies/712233-ab71295-anti-art1-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/979067-15930-1-ap-art3-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/139249-hpa011268-anti-art3/publications?page=1',
    'https://www.citeab.com/antibodies/412014-h00000419-d01p-art3-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/2299029-hpa011268-anti-art3/publications?page=1',
    'https://www.citeab.com/antibodies/252350-h00000419-d01p-art3-purified-maxpab-rabbit-polyclonal/publications?page=1',
    'https://www.citeab.com/antibodies/408418-h00000419-m05-art3-antibody-3a2/publications?page=1',
    'https://www.citeab.com/antibodies/252351-h00000419-m05-art3-monoclonal-antibody-m05-clone-3a2/publications?page=1',
    'https://www.citeab.com/antibodies/2413996-556604-purified-mouse-anti-mash1/publications?page=9',
    'https://www.citeab.com/antibodies/2292373-wh0000419m5-monoclonal-anti-art3/publications?page=1',
    'https://www.citeab.com/antibodies/458581-nbp1-20886-arylsulfatase-d-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/2413996-556604-purified-mouse-anti-mash1/publications?page=10',
    'https://www.citeab.com/antibodies/2839759-abin1169071-anti-sts-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/1530693-hpa002904-anti-sts/publications?page=1',
    'https://www.citeab.com/antibodies/779667-ab62219-anti-steroid-sulfatase-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/490775-nbp1-90095-steroid-sulfatase-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/979965-17870-1-ap-sts-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/28244-eb07457-goat-anti-arylsulfatase-a-antibody/publications?page=1', ]

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
    pdf_link = Column(String(100),
                      nullable=True, comment='')
    journal = Column(String(100),
                     nullable=True, comment='')
    public_date = Column(String(50),
                         nullable=True, comment='')
    application = Column(String(200),
                         nullable=True, comment='')
    reactivity = Column(String(50),
                        nullable=True, comment='')

    def to_dict(self):
        return {
            'id': self.id,
            'company': self.company,
            'catno': self.catno,
            'citationQty': self.citationQty,
            'title': self.title,
            'title_link': self.title_link,
            'pdf_link': self.pdf_link,
            'journal': self.journal,
            'public_date': self.public_date,
            'application': self.application,
            'reactivity': self.reactivity,
        }


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


def get_data(url, br_headers):
    link = url
    res = requests.get(url=link, headers=br_headers, timeout=60)
    soup = BeautifulSoup(res.text, 'html.parser')

    return soup


def cook_soup_top(soup):
    strong_text = soup.find_all('strong')
    catno = strong_text[0].get_text()
    company = strong_text[1].get_text()
    citationQty = int(soup.find('h4').get_text().split(' ')[0])

    return company, catno, citationQty


def cook_soup_bottom(soup, company, catno, citationQty):
    data_list = []
    for item in soup.find_all('div', class_='citation'):
        title_text = item.find('p', class_='title')
        title = title_text.get_text().replace('\n', '')
        title_link = title_text.find('a')['href']
        # TODO try catch ...
        try:
            pdf_link = item.find('a', class_='tiny button new-button view-pdf')[
                'href']
        except Exception as e:
            pdf_link = '0'
        authors_text = item.find('p', class_='authors').get_text()
        journal = authors_text.split('\n')[2]
        public_date = authors_text.split('\n')[3].replace('on ', '')

        bottom = item.find('div', class_='grid-x grid-margin-x meta')
        try:
            application = bottom.find('abbr').get_text()
        except Exception as e:
            application = '0'

        try:
            reactivity = bottom.find('span').get_text()
        except Exception as e:
            reactivity = '0'

        data_list.append(
            {
                'company': company, 'catno': catno, 'citationQty': citationQty,
                'title': title, 'title_link': title_link,
                'pdf_link': pdf_link, 'journal': journal,
                'public_date': public_date, 'application': application,
                'reactivity': reactivity
            }
        )
    print(data_list)
    print(data_list[0]['company'])
    return data_list


if __name__ == '__main__':
    for link in working_list:
        url = link
        main_soup = get_data(url, headers)
        company_top, catno_top, citationQty_top = cook_soup_top(main_soup)
        info_bottom = cook_soup_bottom(main_soup, company_top,
                                       catno_top, citationQty_top)
        objects = []
        for i in info_bottom:
            objects.append(Data(company=i['company'],
                                catno=i['catno'],
                                citationQty=i['citationQty'],
                                title=i['title'],
                                title_link=i['title_link'],
                                pdf_link=i['pdf_link'],
                                journal=i['journal'],
                                public_date=i['public_date'],
                                application=i['application'],
                                reactivity=i['reactivity'], )
                           )

        # try:
        #     session.bulk_save_objects(objects)
        #     session.commit()
        #     session.close()
        #     print('done')
        # except Exception as e:
        #     session.rollback()
        #     print(e)

        time.sleep(random.randint(0, 3))
