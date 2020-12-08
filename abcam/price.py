from re import sub
import requests
from lxml import etree
import json
import urllib.parse
from requests.models import encode_multipart_formdata
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time
import random


Base = declarative_base()


class Price(Base):
    __tablename__ = "abcam_elisa_kit_price"

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

headers = {
    "Host": "www.abcam.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "X-NewRelic-ID": "VQIAU1ZQGwsDU1JQBw==",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Referer": "https://www.abcam.cn/human-tnf-alpha-elisa-kit-ab181421.html",
    # 'Cookie': '_sp_id.4591=8143265cdee71a36.1607303447.4.1607327403.1607310390.acf31b3b-bfb1-4f09-b8fa-988f9e45e8c6; Hm_lvt_30fac56dd55db0f4c94ac39955a1d1f1=1607303447; Hm_lpvt_30fac56dd55db0f4c94ac39955a1d1f1=1607327402; PP=1; C2LC=CN; _gcl_au=1.1.1278091111.1607303448; Qs_lvt_186141=1607303448; Qs_pv_186141=4517183369873685500%2C1155433817271314000%2C2577811995884752000%2C643759359068852400%2C483706034910859200; check=true; mbox=PC#519a7b07ff1c43a2a702068b20209d3b.35_0#1670572204|session#5dbfa4efdae14f0cb39d209cdd01f560#1607328006; _ga=GA1.2.356824111.1607303450; _gid=GA1.2.587176512.1607303450; mediav=%7B%22eid%22%3A%2295155%22%2C%22ep%22%3A%22%22%2C%22vid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22ctn%22%3A%22%22%2C%22vvid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22_mvnf%22%3A1%2C%22_mvctn%22%3A0%2C%22_mvck%22%3A0%2C%22_refnf%22%3A1%7D; _mibhv=anon-1607303455074-6650784431_8620; ak_bmsc=3640B22DEF6F44243D0ADC75F77615C1D2C07717E75C0000A4C7CD5F19FE233A~ply/+ZJmfUGOsO9yxStJivrMYEw2hgHOWTfLovmj/ME4gu7+KQRB47MG+Lb8mXIwhHybhLG7QGdcWzp73DdpywiLcQaRMN4Blpv36aNkQ+8ALF8aFlDxZC1+XBaOniNBfjlbwZBn840qRAZeIF5P3WctKbJriLCZVAbaNhS0fYq4XJTadBZqGYnK3W4n/eLUJvtcfEnNjXBlNCsaCa8ZV5w3JVRF7+cJALOEXUOiMNfcsFcr38XiVWZ/GEeZBJZJJK; _sp_ses.4591=*; JSESSIONID=F03095EE8FBEC0F3116460A8A019B0DC.Pub2; bm_sv=B03D599ABCCD59C56D014FC028C5B38E~kjinP/qKSUCGVavxoomPA1VbsBGFaJHRD9v3IF+F1eRme1zLBtkOVkiWtcSJPxktQio1pauP0VuMdRU9LS2vFydun584feNpmGueQLaZTXQFuQg9eqpxCKH12VKdhKTWVmfiRWuUCfNh/6Qzfr/BuWxV6nDYKETd6NHSiwMJ+eY=; mboxEdgeCluster=35; _uetsid=647fa3d0383111eb96821173e067000b; _uetvid=64804db0383111ebba642974ece6f04c; _gali=description_references',
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}
# ! abXXXX --> XXXX
while r.exists("abcam_extra"):
    cata = r.lpop("abcam_extra")
    last = cata.replace("ab", "")
    url = f"https://www.abcam.cn/datasheetproperties/availability?abId={last}"
    print(url)
    results = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        # print(resp.text)
        json_str = resp.json()
        catano = json_str["size-information"]["ProductCode"]
        for item in json_str["size-information"]["Sizes"]:
            sub_catano = item["SellingSizeCode"]
            size = item["Size"]
            price = item["Price"].replace("&micro;g", "μg")
            # print(catano, sub_catano, size, price)
            new_price = Price(
                Catalog_Number=catano,
                sub_Catalog_Number=sub_catano,
                Size=size,
                Price=price,
            )
            results.append(new_price)
        session.bulk_save_objects(results)
        session.commit()
        session.close()
        print(cata, "done")
