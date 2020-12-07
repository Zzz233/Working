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
    __tablename__ = "abcam_elisa_kit_list"

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
    "Host": "www.abcam.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "X-NewRelic-ID": "VQIAU1ZQGwsDU1JQBw==",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Referer": "https://www.abcam.com/products?selected.productType=Kits",
    # "Cookie": "ak_bmsc=DCC7D3409949248ACE21FD7DB9530361173CA86670680000F090CD5FFDDADF2A~plqzVvM00JrTXN3NrN0w1foPcjVP3oNbpYhZhCS7fe3yEUvGZSOoep9j6KsHM2RrTxl6I+St1UySz62yaRON9o63CYwY2MPepDPYlcR/vGBxJYWy5yL0oYM/8uiUl2bQ8bhiftbqSucorLEqZB31Bdxv4JXt3hxhJnzqF+dMPorPalgoLYFhxqEmsLzLxSzYiRbrbS+epoVxxyyrDqgpTTtxq6+XXvzd4eTZeYgRlCXeZZFohq3AFrlvnrAc72W1l2; _gcl_au=1.1.2112752315.1607307507; _sp_ses.2495=*; _sp_id.2495=9e72eec18705b3ae.1607307507.1.1607307799.1607307507.ee9e212d-fb94-4db7-a213-bd1ba38e88b1; _hjTLDTest=1; _hjid=6979f514-0f00-4425-ae38-8d0916685a5e; _hjFirstSeen=1; PP=1; check=true; mbox=session#aa6a97755bbb400daeffefc3f8dcadb4#1607309369|PC#aa6a97755bbb400daeffefc3f8dcadb4.35_0#1670552388; C2LC=US; JSESSIONID=5C45C7025AC72371307E34D8749A4CD9.Pub2; bm_sv=36DDF4CCBF8DE202D67BC57E9DD85827~VeTKkPuEvYywHV7/sr1Yfa91vu+awKksVpl9Eo3lv7fOxgzgDTEvq2pUl4Arn/4yfbsD3Nfw4NoVSwWGs0lk9SDexyx3icQr5PPyeh0rv8Xw3zA4o8Cw4ppp6dm34rGaX+W5+oSBZZ0EdvIdRndgub7BF52lz1ela2np71xdtWo=; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=1; _hjIncludedInSessionSample=1; _ga=GA1.2.844532259.1607307510; _gid=GA1.2.1618503788.1607307510; _gac_UA-367099-1=1.1607307575.EAIaIQobChMIn5rI_ua67QIVZxitBh02xwi7EAAYAiAAEgKQGPD_BwE; mboxEdgeCluster=35; _hjShownFeedbackMessage=true; _mibhv=anon-1607307512962-5163650945_7395; _fbp=fb.1.1607307513195.1365994282; _uetsid=abf78650383211ebb092a580b1f0a3c0; _uetvid=abf78300383211ebb4284babe6eb8d5b",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}

for i in range(1, 408):
    objects = []
    url = f"https://www.abcam.com/products/loadmore?selected.productType=Kits&pagenumber={i}"

    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        lxml = etree.HTML(resp.text)
        products = lxml.xpath("//div[@data-productcode]")
        for item in products:
            name = item.xpath(".//@data-productname")[0].strip()
            catano = item.xpath(".//@data-productcode")[0].strip()
            link = "https://www.abcam.com" + item.xpath(".//h3/a/@href")[0].strip()
            # print(catano, name, link)
            new_data = Data(
                Brand="abcam", Catalog_Number=catano, Product_Name=name, Detail_url=link
            )
            objects.append(new_data)
    session.bulk_save_objects(objects)
    session.commit()
    session.close()
    print(i, "done")
    time.sleep(random.uniform(1.0, 2.0))
