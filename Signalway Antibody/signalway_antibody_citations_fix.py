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


class Detail(Base):
    __tablename__ = "sab_antibody_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Antibody_Type = Column(String(40), nullable=True, comment="")
    Sellable = Column(String(40), nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Application = Column(String(500), nullable=True, comment="")
    Conjugated = Column(String(200), nullable=True, comment="")
    Clone_Number = Column(String(40), nullable=True, comment="")
    Recombinant_Antibody = Column(String(10), nullable=True, comment="")
    Modified = Column(String(100), nullable=True, comment="")
    Host_Species = Column(String(20), nullable=True, comment="")
    Reactivity_Species = Column(String(20), nullable=True, comment="")
    Antibody_detail_URL = Column(String(500), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Price_Status = Column(String(20), nullable=True, comment="")
    Citations_Status = Column(String(20), nullable=True, comment="")
    GeneId = Column(String(500), nullable=True, comment="")
    KO_Validation = Column(String(10), nullable=True, comment="")
    Species_Reactivity = Column(String(1000), nullable=True, comment="")
    SwissProt = Column(String(500), nullable=True, comment="")
    Immunogen = Column(String(1000), nullable=True, comment="")
    Predicted_MW = Column(String(200), nullable=True, comment="")
    Observed_MW = Column(String(200), nullable=True, comment="")
    Isotype = Column(String(200), nullable=True, comment="")
    Purify = Column(String(200), nullable=True, comment="")
    Citations = Column(String(20), nullable=True, comment="")
    Citations_url = Column(String(500), nullable=True, comment="")
    DataSheet_URL = Column(String(500), nullable=True, comment="")
    Review = Column(String(20), nullable=True, comment="")
    Price_url = Column(String(500), nullable=True, comment="")
    Image_qty = Column(Integer, nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "sab_antibody_citations"

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
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=2)
r = redis.Redis(connection_pool=pool)


class Signalway(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }

    def format(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers)
            x = etree.HTML(resp.text)
            y = x.xpath('//div[@class="detail"]')[0]
        return y

    def citations(self, html):
        try:
            citations = len(
                html.xpath('.//span[@class="list_item1" or @class="list_item"]')
            )
        except Exception:
            return 0
        return citations

    # ======================================================================== #
    # Citations表
    def sub_citations(self, html):
        results = []
        try:
            spans = html.xpath('.//span[@class="list_item1" or @class="list_item"]')
            for item in spans:
                text = item.xpath(".//text()")
                title = "".join(i for i in text).split("  PMID: ")[0].strip()
                pmid = item.xpath(".//strong/a/text()")[0].split("PMID:")[1].strip()
                link = item.xpath(".//strong/a/@href")[0].strip()
                results.append([pmid, title, link])
        except Exception:
            return results
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("signalway_fixer"):
        extract = r.lpop("signalway_fixer")
        url = extract.split(",")[0]
        catanum = extract.split(",")[-1]
        print(extract)
        # url = "https://www.sabbiotech.com.cn/g-170611-Goat-anti-Mouse-IgG-Secondary-AntibodyHRP-conjugated-L3032.html"
        try:
            lxml = Signalway().format(url)
        except Exception as e:
            print(e)
            r.rpush("signalway_fixer", extract)
            continue

        citations = Signalway().citations(lxml)
        # print(citations)
        sub_citations = Signalway().sub_citations(lxml)
        # print(sub_citations)
        session.query(Detail).filter(Detail.Catalog_Number == catanum).update(
            {Detail.Citations: str(citations)}
        )

        if isinstance(sub_citations, list):
            objects_sub_citations = []
            for sub in sub_citations:
                pid = sub[0]
                tit = sub[1]
                lin = sub[2]
                new_citations = Citations(
                    Catalog_Number=catanum,
                    PMID=pid,
                    Article_title=tit,
                    Pubmed_url=lin,
                )
                objects_sub_citations.append(new_citations)
            session.bulk_save_objects(objects_sub_citations)
        else:
            pass

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.rpush("signalway_fixer", extract)
            session.rollback()
            print(2, e)
        time.sleep(random.uniform(4.5, 6.5))
