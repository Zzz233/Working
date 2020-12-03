import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time
import random

Base = declarative_base()


class Citations(Base):
    __tablename__ = "immunoway_antibody_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    PMID = Column(String(40), nullable=True, comment="")
    Application = Column(String(300), nullable=True, comment="")
    Species = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(1000), nullable=True, comment="")
    Pubmed_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019" "@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=2)
r = redis.Redis(connection_pool=pool)


class Immunoway:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/5"
        "37.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers, timeout=120)
            html_lxml = etree.HTML(resp.text)
            try:
                main_content = html_lxml.xpath('//div[@class="main dMain"]')[0]
            except Exception:
                return None
        return main_content

    def sub_citasions(self, html):
        results = []
        try:
            p = html.xpath('.//p[@style="backgroup:#f2f2f2;"]')
        except Exception:
            return results
        for item in p:
            link = (
                item.xpath('.//a[@href="/uploadfiles/"]/@onclick')[0]
                .replace("window.open('", "")
                .replace("')", "")
                .strip()
            )
            title = item.xpath('.//a[@href="/uploadfiles/"]/text()')[0]
            if "https://www.ncbi.nlm.nih.gov/pubmed/" in link:
                pmid = link.split("nlm.nih.gov/pubmed/")[1]
            else:
                pmid = None
            results.append([title, pmid, link])
        return results


if __name__ == "__main__":
    while r.exists("citations"):
        url = r.rpop("citations")
        print(url)
        catano = url.split("/")[-1]
        try:
            lxml = Immunoway().format(url)
        except Exception:
            r.lpush("citations", url)
            continue
        sub_cite = Immunoway().sub_citasions(lxml)
        if sub_cite:
            objects_citations = []
            for sub in sub_cite:
                ptitle = sub[0]
                pid = sub[1]
                pul = sub[2]
                new_citations = Citations(
                    Catalog_Number=catano,
                    PMID=pid,
                    Article_title=ptitle,
                    Pubmed_url=pul,
                )
                objects_citations.append(new_citations)
        session.bulk_save_objects(objects_citations)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("citations", url)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(1, 2.5))
