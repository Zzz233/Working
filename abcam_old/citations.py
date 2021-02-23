import requests
from lxml import etree
import json
import urllib.parse
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time
import random

from sqlalchemy.sql.operators import like_op

Base = declarative_base()


class Citations(Base):
    __tablename__ = "abcam_elisa_kit_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    PMID = Column(String(40), nullable=True, comment="")
    Species = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(1000), nullable=True, comment="")
    Sample_type = Column(String(100), nullable=True, comment="")
    Pubmed_url = Column(String(1000), nullable=True, comment="")
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
    "Host": "www.abcam_old.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "X-NewRelic-ID": "VQIAU1ZQGwsDU1JQBw==",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
    "Referer": "https://www.abcam.cn/human-tnf-alpha-elisa-kit-ab181421.html",
    # 'Cookie': '_sp_id.4591=8143265cdee71a36.1607303447.4.1607330305.1607310390.acf31b3b-bfb1-4f09-b8fa-988f9e45e8c6; Hm_lvt_30fac56dd55db0f4c94ac39955a1d1f1=1607303447; Hm_lpvt_30fac56dd55db0f4c94ac39955a1d1f1=1607330304; PP=1; C2LC=CN; _gcl_au=1.1.1278091111.1607303448; Qs_lvt_186141=1607303448; Qs_pv_186141=3282513758284699000%2C1021251155594285800%2C1182793061729288200%2C2593845387966127600%2C1067218680860964000; check=true; mbox=PC#519a7b07ff1c43a2a702068b20209d3b.35_0#1670575106|session#ef6ce4005a0c43bf93d7e19002aba302#1607332165; _ga=GA1.2.356824111.1607303450; _gid=GA1.2.587176512.1607303450; mediav=%7B%22eid%22%3A%2295155%22%2C%22ep%22%3A%22%22%2C%22vid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22ctn%22%3A%22%22%2C%22vvid%22%3A%22RJjoU%60hWjT9%24FZ'%233!a2%22%2C%22_mvnf%22%3A1%2C%22_mvctn%22%3A0%2C%22_mvck%22%3A0%2C%22_refnf%22%3A1%7D; _mibhv=anon-1607303455074-6650784431_8620; _sp_ses.4591=*; JSESSIONID=F03095EE8FBEC0F3116460A8A019B0DC.Pub2; mboxEdgeCluster=35; ak_bmsc=222209FAC23B99662569CEB42C225E03D2C07717E75C0000DEE4CD5F0BA2B212~plXYEzcMDPotqJNxyKHFSvHl/kAXHJhqdkYOzAN7kEXCw7Zejfkw6yJavQZbsoicql8Pcz1Dpn2QI4+4xEBeGK8zf5V+py8Yj/55Ft7lfNBpzatrwbDZsUKWpF9MtrY5XbRRZuHLsbkDPzb7axkkeZ8Kn8UgkWNX5tMFNz/UIdp89yL3ca+YynpuV/Z46D0R9hKfSGDn+tnptrHc2rfbfCxOvBJlS+Sw3kGt0oRiT2YihQviBgfttyuTHwCnqSILn+; bm_sv=CA3CA64C2E71D1C56508A582BE0378F3~kjinP/qKSUCGVavxoomPA38oUCMdLxrGPp9X4413zNfs4slbl/IdGFs7FNBvbDExTHZDGdEXLjid9bIsB8k0Us2UdHvEiU+rP7Jg6VKCiepmcOD2MV2LeOLEUhlaPYY+4f1k1AbhNVjeH+wIx3AAcA==; _gali=description_references; _gat_UA-367099-9=1; _uetsid=647fa3d0383111eb96821173e067000b; _uetvid=64804db0383111ebba642974ece6f04c; _dc_gtm_UA-367099-9=1',
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}
# ! redis catano
# https://www.abcam.cn/DatasheetProperties/References?productcode=ab181421
while r.exists("abcam_citations_extra"):
    last = r.lpop("abcam_citations_extra")
    url = f"https://www.abcam.cn/DatasheetProperties/References?productcode={last}"
    print(url)
    results = []
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        json_str = resp.json()
        for item in json_str:
            catanum = last
            pmid = item["PubmedID"]
            species = item["Species"]
            title = item["Title"]
            pm_url = (
                "https://www.ncbi.nlm.nih.gov/pubmed/"
                + str(item["PubmedID"])
                + "?dopt=Abstract"
            )
            # print(catanum, pmid, species, title, pm_url)
            new_citations = Citations(
                Catalog_Number=catanum,
                PMID=pmid,
                Species=species,
                Article_title=title,
                Pubmed_url=pm_url,
            )
            results.append(new_citations)

        session.bulk_save_objects(results)
        session.commit()
        session.close()
        print("done")
        time.sleep(1)
