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
    "Host": "www.abcam_old.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "X-NewRelic-ID": "VQIAU1ZQGwsDU1JQBw==",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Referer": "https://www.abcam.cn/products?sortOptions=Relevance&selected.classification=ELISA%20and%20Matched%20Antibody%20Pair%20Kits--ELISA%20Kits%20and%20Reagents--ELISA",
    "Cookie": "_sp_id.4591=8143265cdee71a36.1607303447.4.1607323156.1607310390.acf31b3b-bfb1-4f09-b8fa-988f9e45e8c6; Hm_lvt_30fac56dd55db0f4c94ac39955a1d1f1=1607303447; Hm_lpvt_30fac56dd55db0f4c94ac39955a1d1f1=1607323095; PP=1; C2LC=CN; _gcl_au=1.1.1278091111.1607303448; Qs_lvt_186141=1607303448; Qs_pv_186141=1978628143855239200%2C876172786810121000%2C2711236939431772000%2C2412310853606670300%2C3634332151587046400; check=true; mbox=PC#519a7b07ff1c43a2a702068b20209d3b.35_0#1670567896|session#e5ee7e9af6b049b29e157b8a4906d7a3#1607323370; _ga=GA1.2.356824111.1607303450; _gid=GA1.2.587176512.1607303450; mediav=%7B%22eid%22%3A%2295155%22%2C%22ep%22%3A%22%22%2C%22vid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22ctn%22%3A%22%22%2C%22vvid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22_mvnf%22%3A1%2C%22_mvctn%22%3A0%2C%22_mvck%22%3A0%2C%22_refnf%22%3A0%7D; _mibhv=anon-1607303455074-6650784431_8620; ak_bmsc=3640B22DEF6F44243D0ADC75F77615C1D2C07717E75C0000A4C7CD5F19FE233A~ply/+ZJmfUGOsO9yxStJivrMYEw2hgHOWTfLovmj/ME4gu7+KQRB47MG+Lb8mXIwhHybhLG7QGdcWzp73DdpywiLcQaRMN4Blpv36aNkQ+8ALF8aFlDxZC1+XBaOniNBfjlbwZBn840qRAZeIF5P3WctKbJriLCZVAbaNhS0fYq4XJTadBZqGYnK3W4n/eLUJvtcfEnNjXBlNCsaCa8ZV5w3JVRF7+cJALOEXUOiMNfcsFcr38XiVWZ/GEeZBJZJJK; _sp_ses.4591=*; JSESSIONID=F03095EE8FBEC0F3116460A8A019B0DC.Pub2; bm_sv=B03D599ABCCD59C56D014FC028C5B38E~kjinP/qKSUCGVavxoomPA1VbsBGFaJHRD9v3IF+F1eRme1zLBtkOVkiWtcSJPxktQio1pauP0VuMdRU9LS2vFydun584feNpmGueQLaZTXRrFS20tFKbxZGv0edMjHoiUhppn5e3lHVUq2bpmQCoq/PDXgNUA3UWrLyUasJ15yM=; mboxEdgeCluster=35; _uetsid=647fa3d0383111eb96821173e067000b; _uetvid=64804db0383111ebba642974ece6f04c; _dc_gtm_UA-367099-9=1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}

for i in range(1, 59):
    # objects = []
    url = f"https://www.abcam.cn/products/loadmore?sortOptions=Relevance&selected.classification=ELISA and Matched Antibody Pair Kits--ELISA Kits and Reagents--SimpleStep ELISA™&pagenumber={i}"

    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        lxml = etree.HTML(resp.text)
        products = lxml.xpath("//div[@data-productcode]")
        for item in products:
            name = item.xpath(".//@data-productname")[0].strip()
            catano = item.xpath(".//@data-productcode")[0].strip()
            link = "https://www.abcam.cn" + item.xpath(".//h3/a/@href")[0].strip()
            # print(catano, name, link)
            new_data = Data(
                Brand="abcam_old", Catalog_Number=catano, Product_Name=name, Detail_url=link
            )
            session.add(new_data)
            try:
                session.commit()
                session.close()
            except Exception as e:
                print(e)
                session.rollback()
    print(i, "done")
    time.sleep(random.uniform(1.0, 2.0))
    #         objects.append(new_data)
    # session.bulk_save_objects(objects)
    # try:
    #     session.commit()
    #     session.close()
    # except Exception as e:
    #     print(e)
    #     session.rollback()
    # print(i, "done")
    # time.sleep(random.uniform(1.0, 2.0))
