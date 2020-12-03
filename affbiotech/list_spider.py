import requests
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time

Base = declarative_base()


class Data(Base):
    __tablename__ = 'bp_antibody_list_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Product_name = Column(String(200),
                          nullable=True, comment='')
    Application = Column(String(1000),
                         nullable=True, comment='')
    Antibody_detail_URL = Column(String(500),
                                 nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    Antibody_Type = Column(String(100),
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

brand = 'Affinity Biosciences'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0)'
                  ' Gecko/20100101 Firefox/82.0'
}
for i in range(49, 1447):
    # 解析html
    url = f'http://www.affbiotech.cn/search.php?keywords=&category=1&brand=0&reactivity=&predicted=&site=&validation=&conjugate=0&modification=0&application=&clonality=0&source=0&genes=&gid=&action=&page={i}'
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        soup = BeautifulSoup(resp.text, 'html.parser')

    for item in soup.find_all('div', class_='col-sm-8 pro-info'):
        name = item.find('b').get_text()

        text = item.get_text()
        catno = text.split('货号:')[1].strip().split('\n')[0]
        # print(text.split('来源:')[1].strip().split('\n')[0])
        application = text.split('应用:')[1].strip().split('\n')[0]
        # print(text.split('反应:')[1].strip().split('\n')[0])

        new_data = Data(Brand='Affinity Biosciences',
                        Catalog_Number=catno,
                        Product_name=name,
                        Application=application,
                        Antibody_detail_URL=url,
                        Antibody_Status='0'
                        )
        session.add(new_data)
        session.commit()
        session.close()

    print(url, 'done')
    # time.sleep(1)
