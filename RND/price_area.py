import requests
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
    __tablename__ = "r&d_elisa_kit_price"

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
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=2)
r = redis.Redis(connection_pool=pool)

while r.exists("price_rnd"):
    catanums = r.lpop("price_rnd")
    print(catanums)
    mian = catanums.split("|||")[0]
    sub = catanums.split("|||")[1]
    payload = {
        "catnums": sub,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }
    url = "https://www.rndsystems.com/cn/ajax/atc_blocks_details"

    try:
        with requests.Session() as s:
            resp = s.post(url=url, data=payload, headers=headers)
            json_text = resp.json().items()
    except Exception as e:
        print(e)
        r.rpush("price_rnd", catanums)

    results = []
    for key, value in json_text:
        catano = mian
        sub_catano = key
        sub_size = value["size"]
        new_price = Price(
            Catalog_Number=catano, sub_Catalog_Number=sub_catano, Size=sub_size
        )
        results.append(new_price)

    session.bulk_save_objects(results)

    try:
        session.commit()
        session.close()
        print("done")
    except Exception as e:
        print(e)
        r.rpush("price_rnd", catanums)
        session.rollback()

    time.sleep(random.uniform(1, 2.5))
