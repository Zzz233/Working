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
    __tablename__ = "cellsignal_elisa_kit_list"

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
    "Host": "www.cellsignal.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    # "Cookie": "__cfduid=d78d3db82a5975148df316bd63115fb681607407998; _gcl_au=1.1.341848392.1607408004; Hm_lvt_6b9b87ffc386265e96689ecf61116606=1607408004; Hm_lpvt_6b9b87ffc386265e96689ecf61116606=1607408007; _ga=GA1.2.451083012.1607408009; _gid=GA1.2.1898967601.1607408009; _gat_UA-4351229-24=1",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}
brand = "cellsignal"

for i in [0, 200, 400]:
    url = f"https://www.cellsignal.cn/browse/elisa-kits?N=102262+4294956287&No={i}&Nrpp=200"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        lxml = etree.HTML(resp.text)
        products = lxml.xpath("//tbody/tr")
        # print(products)
        results = []
        for product in products:
            catano = product.xpath('.//td[@class="hyperlink"][@data-sku]/@data-sku')[
                0
            ].strip()
            name_list = product.xpath('.//td[@class="hyperlink"][@data-name]/a//text()')
            name = "".join(i for i in name_list)
            link = (
                "https://www.cellsignal.cn"
                + product.xpath('.//td[@class="hyperlink"][@data-name]/a/@href')[
                    0
                ].strip()
            )
            print(catano, name, link)
            new_data = Data(
                Brand=brand, Catalog_Number=catano, Product_Name=name, Detail_url=link
            )
            results.append(new_data)
        session.bulk_save_objects(results)
        session.commit()
        session.close()
    time.sleep(random.uniform(1.0, 2.0))