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

Base = declarative_base()


class Price(Base):
    __tablename__ = "alomone_antibody_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Antibody_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)

engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
while r.exists("alomone_price"):
    extract = r.lpop("alomone_price")
    print(extract)
    num = extract.split(",")[1]
    catano = extract.split(",")[0]
    payload = {
        "nonce": "3a4aeef025",
        "product_id": num,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
        "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
    }
    url = "https://www.alomone.com/?wc-ajax=alomone_add_to_cart_block"

    results = []
    with requests.Session() as s:
        resp = s.post(url=url, data=payload, headers=headers)
        json_html = resp.json()["html"]
        html = etree.HTML(json_html)
        options = html.xpath(
            '//select[@name="product-select"]/option[@name="attribute_pa_size"]'
        )
        for option in options:
            size = option.xpath("./text()")[0].strip()
            new_price = Price(Catalog_Number=catano, Size=size)
            results.append(new_price)
    session.bulk_save_objects(results)
    session.commit()
    session.close()
    print("done")
    time.sleep(1)

    #     json_str = html.xpath("//form/@data-product_variations")[0]
    #     a = json.loads(json_str)
    #     for attributes in a:
    #         urlencode_str = attributes["attributes"]["attribute_pa_size"]
    #         size = urllib.parse.unquote(urlencode_str)
    #         price = "$ " + str(attributes["display_price"])
    #         catano = attributes["sku"]
    #         print(catano, size, price)
    #         new_price = Price(Catalog_Number=catano, Size=size, Price=Price)
    #     results.append(new_price)
    # session.bulk_save_objects(results)
    # session.commit()
    # session.close()
    # print("done")
    # time.sleep(1)
