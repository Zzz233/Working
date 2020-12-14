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
import json

Base = declarative_base()


class Price(Base):
    __tablename__ = "invitrogen_kit_price"

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
    "cookie": "pearUXVerMar=pearUX2",
    "user-agent": "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
}

for i in range(1):
    # while r.exists('xxx'):
    main_cata = "XXXX"
    sub_cata = "BMS277-2TEN"
    size = "XXXX"
    url = "https://www.thermofisher.com/api/magellan/pricing/BMS277-2TEN"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        text = resp.text
        json_data = json.loads(text)
        try:
            price = json_data["myPriceData"][sub_cata]["pricingList"]["pricingDetails"][
                0
            ]["priceString"]
        except Exception:
            price = None
        new_price = Price(
            Catalog_Number=main_cata,
            sub_Catalog_Number=sub_cata,
            Size=size,
            Price=price,
        )
        session.add(new_price)
        session.commit()
        session.close()
    time.sleep(random.uniform(1.0, 1.5))