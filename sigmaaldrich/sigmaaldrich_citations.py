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


class Citations(Base):
    __tablename__ = "sigmaaldrich_kit_citations"

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
    "Host": "www.sigmaaldrich.com",
    "Connection": "keep-alive",
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "x-dtpc": "5$274332424_767h5vFCFMUBATPITAERGSTRTQFVVSHPFOGCLC-0e13",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.sigmaaldrich.com/catalog/product/sigma/rab0441?lang=zh&region=CN",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cookie": "SialLocaleDef=CountryCode~CN|WebLang~-7|",
}
# for i in range(1):
while r.exists("sigmaaldrich_citations"):
    catano = r.lpop("sigmaaldrich_citations")
    print(catano)
    catano_lower = catano.lower()
    url = f"https://www.sigmaaldrich.com/catalog/documents?term={catano_lower}sigma&itemsperpage=10&pageNo=1&locale=zh_CN&t=0.2958547314451786"
    resp = requests.post(url=url, headers=headers)
    json_data = resp.json()

    try:
        citations = json_data["count"]
    except Exception:
        item = catano + "," + "0"
        r.rpush("sigmaaldrich_citation_qty", item)
        print("done")
        time.sleep(random.uniform(1.0, 1.5))
        continue

    item = catano + "," + str(citations)
    r.rpush("sigmaaldrich_citation_qty", item)
    print("插入")
    print(citations)
    for item in json_data["content"]:
        title = item["text"].strip()

        new_citations = Citations(
            Catalog_Number=catano,
            Article_title=title,
        )
        session.add(new_citations)
        session.commit()
        session.close()
        print("done")
    time.sleep(random.uniform(1.0, 1.5))
