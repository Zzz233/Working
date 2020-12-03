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
    'Cookie': '__utmz=257053465.1603690376.1.1.utmcsr=cn.bing.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utma=257053465.1093246679.1603690376.1603766770.1603780901.5; __utmc=257053465; __utmt=1; __utmb=257053465.4.10.1603780901; _citeab-live-session=C7AjmVyG%2BWi%2BpDFUinkBSNRJvA7lEmIDmHYw0P6f0ifRpCRtEWNBRXti0Bs2mDgiium5c3mIPjn9ZKQA8EDDfrS9ue87EpIAkoQSJpoxZlGepHAQ1jeCPQPUZ5hR%2B%2BcXOQqDZlHo7iF6iZTJxJaZA%2F%2B7%2FA743ByKq8DcJQVWaRsD4aKTo1AcC8ZKQ5ToSXwmxqDhm0LkVg2cyUZ9bqz1GoJ9a%2BOjB%2FVVLrIff2DnftXRz3WmtyPviwjJ5Zv8VxPgfY%2FyffY93rOFF1FkPAyp8%2Bx81hbOmssSvPUhY9eoInk3MrUoqbtYRM7LdHyTZjHwyI1r2IPmriMXs%2FuXK0EOeBea5GvWWJvX568JTOlZZBUtbtBc78YhEJXk1cWmY59DA00kmQ%2FZYl5FsS1pojqVmhzQgvX54UCHlqyH7HVvWVC8s9lJLrJXKH8toHZjCLlySw7p0FJEpUfqLA%2FmReABzACueYQbbi7c6hJ7yxH1ble9t%2FH1haBeNt9CgPkIdfMnwQS0jpS%2BYPB9JwkgQags9xwHjZ8CtXHGKT8SyZsz4N4ZtlwDMxHab7b9%2F9%2FFVDHA1br5PnKWFbn8mPzlMyV%2BUOPvdd9AvhjYV6c0yT1ITBtJ%2FVDHSlYBkZMVG%2Bx5iehQ%2BbUO9dqdb7keWYbK52vDR2dJC6LwTHDG9VYkv26RWrTE6CY55YTck5i%2BLRR%2B%2Fc3onqGsBeUPVCE%2FShzIsLIz8XnlyuM6cLAl0qIzv8CzoYA8wPonnHl6rMnRT1HFlELUXHz0c5MIrWLhoQDERuhbiRlVt5rMeSKq0nP6dNJ7SgD5oOkUYfqi%2FXQY%2FSqJSrf8LM%2BdSRN2MrhBpwSkY2kD7wE5wgzs9vhtMrEmqR94LI0fwmDDtgoYg1UO%2BIfNWjgI7mh8Q6IEfm%2FlL6UaOys89wvBejcjCSEwObcehFCqJn5HfE90n6T0sgB7BKq1Ss0zB1ue5WVZLwYzuQrEN32bNA6V%2BldujquLjIreOj0oHqeDI0b9gmgQZlitTc5bXsu2--gN2oMsf226ZzThLS--FM4zcWHs6SlWOqMO2tS49g%3D%3D',
    'Host': 'www.citeab.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.citeab.com/antibodies/1472806-gp155-anti-arvcf-guinea-pig-polyclonal-serum/publications?page=1%27',
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


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()


def get_data(url, br_headers):
    link = url
    res = requests.get(url=link, headers=br_headers, timeout=60)
    # print(res.text)
    html = res.text

    return html


def cook_soup(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    # print(soup)
    strong_text = soup.find_all('strong')
    catno = strong_text[0].get_text()
    company = strong_text[1].get_text()
    citationQty = int(soup.find('h4').get_text().split(' ')[0])
    title_text = soup.find('p', class_='title')
    title = title_text.get_text().replace('\n', '')
    title_link = title_text.find('a')['href']

    try:
        pdf_link = soup.find('a', class_='tiny button new-button view-pdf')[
            'href']
    except Exception as e:
        pdf_link = '0'

    authors_text = soup.find('p', class_='authors').get_text()
    journal = authors_text.split('\n')[2]
    public_date = authors_text.split('\n')[3].replace('on ', '')

    bottom = soup.find('div', class_='grid-x grid-margin-x meta')
    try:
        application = bottom.find('abbr').get_text()
    except Exception as e:
        application = '0'

    try:
        reactivity = bottom.find('span').get_text()
    except Exception as e:
        reactivity = '0'

    return company, catno, citationQty, title, title_link, pdf_link, \
           journal, public_date, application, reactivity


if __name__ == '__main__':
    for url in working_list:
        a = get_data(url, headers)

        b = cook_soup(a)
        new_data = Data(company=b[0], catno=b[1],
                        citationQty=b[2], title=b[3],
                        title_link=b[4], pdf_link=b[5],
                        journal=b[6], public_date=b[7],
                        application=b[8], reactivity=b[9])
        try:
            session.add(new_data)
            session.commit()
            session.close()
            print('done')
        except Exception as e:
            session.rollback()
            print(e)

        time.sleep(random.randint(0, 8))
