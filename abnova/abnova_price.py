"http://www.abnova.com/inc/ajax_price_info.asp?Catalog_ID=KA5410&strCountry=CN"
from platform import machine
import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random
import redis

Base = declarative_base()


class Price(Base):
    __tablename__ = "abnova_kit_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


# Mysql
engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)


class MyBlood(object):
    headers = {
        "Host": "www.abnova.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": "http://www.abnova.com/products/products_detail.asp?catalog_id=KA5410",
        # "Cookie": "CookiesAbnovaSelectLanguage=CN; Session=SEID=%7B76609563%2DEBCF%2D4387%2D9302%2D3A84C98EC98B%7D; TRID=%7B657239B0%2DF7BB%2D435E%2DAC9E%2D69ED4684587B%7D; ASPSESSIONIDACDBCQRQ=NDDKHJCAMLJNAPJIJCOPNFJJ; _ga=GA1.2.1698886661.1607493654; _gid=GA1.2.1666527437.1607493654; IsAllowedCookie=Y; __atuvc=13%7C50; __atuvs=5fd16d60ce6283b4000",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=60)
            seconde = resp.text.split('innerHTML=("')[1].rstrip('");')
            html_lxml = etree.HTML(seconde)
            try:
                content = html_lxml.xpath("//ul[@class='data']")[0]
            except Exception:
                content = None
        return content

    def price(self, html):
        results = []
        try:
            siz = html.xpath('./li[@class="size"]/text()')[0].strip()
            pri = html.xpath('./li[@class="price"]/text()')[0].strip()
            if pri == "..........":
                pri = None
            results.append([siz, pri])
        except Exception:
            return None
        return results


if __name__ == "__main__":
    while r.exists("abnova_price"):
        # for i in range(1):
        catnum = r.rpop("abnova_price")
        url = "http://www.abnova.com/inc/ajax_price_info.asp?Catalog_ID=KA5410&strCountry=CN"
        lxml = MyBlood().format(url)
        price = MyBlood().price(lxml)
        print(lxml, price)

        if price:
            objects_sub_price = []
            for sub in price:
                sub_s = sub[0]
                suc_p = sub[1]

                new_price = Price(Catalog_Number=catnum, Size=sub_s, Price=suc_p)
                objects_sub_price.append(new_price)
            session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
        except Exception as e:
            print(e)
            r.lpush("abnova_price", catnum)
            session.rollback()
            continue
        print(catnum, "done")
