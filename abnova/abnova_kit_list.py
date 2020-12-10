import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "abnova_kit_list"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

headers = {
    "Host": "www.abnova.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    # "Cookie": "CookiesAbnovaSelectLanguage=CN; Session=SEID=%7B76609563%2DEBCF%2D4387%2D9302%2D3A84C98EC98B%7D; TRID=%7B657239B0%2DF7BB%2D435E%2DAC9E%2D69ED4684587B%7D; ASPSESSIONIDACDBCQRQ=NDDKHJCAMLJNAPJIJCOPNFJJ; _ga=GA1.2.1698886661.1607493654; _gid=GA1.2.1666527437.1607493654; IsAllowedCookie=Y; __atuvc=11%7C50",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
brand = "abnova"
# ! 1, 42
# "http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=1,2,3,5,6,8,A,B,C&page=41"

# ! 1, 27
# "http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=D,E,F,G,H&page=26"

# ! 1, 29
# "http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=I,J,K,L,M&page=28"

# ! 1, 27
# "http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=N,O,P,Q,R,S&page=26"

# ! 1, 19
# "http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=T,U,V,W,X,Y,Z&page=18"

for i in range(1, 19):
    url = f"http://www.abnova.com/products/products_list.asp?class=AIAD00000000&classify=T,U,V,W,X,Y,Z&page={i}"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        # print(resp.text)
        lxml = etree.HTML(resp.text)
        divs = lxml.xpath('//div[@id="divproductlist-tr"]')
        # results = []
        for div in divs:
            catnum = div.xpath('./div[1]/a[@class="bluetitle13"]/text()')[0].strip()
            link = div.xpath('./div[1]/a[@class="bluetitle13"]/@href')[0].strip()
            name = div.xpath('./div[2]/a[@class="bluetitle13"]/text()')[0].strip()
            print(catnum, name, link)
            new_data = Data(
                Brand=brand, Catalog_Number=catnum, Product_Name=name, Detail_url=link
            )
            session.add(new_data)
            try:
                session.commit()
                session.close()
            except Exception as e:
                session.rollback()
                print(e)
                pass
    print(i, "done")
    time.sleep(random.uniform(0.5, 1.0))
