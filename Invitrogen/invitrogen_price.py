# BMS623-3;BMS623/3FIVE,5 x 96 tests|BMS623/3TEN,10 x 96 tests|BMS623/3TWO,2 x 96 tests|BMS623/3,96 tests
# BMS626-2;BMS626/2TEN,10 x 96 tests|BMS626/2,96 tests
# BMS607-2INST;BMS607/2INST,128 tests
# BMS608-4;BMS608/4FIVE,5 x 96 tests|BMS608/4TEN,10 x 96 tests|BMS608/4TWO,2 x 96 tests|BMS608/4,96 tests
# BMS607-3;BMS607/3FIVE,5 x 96 tests|BMS607/3TEN,10 x 96 tests|BMS607/3TWO,2 x 96 tests|BMS607/3,96 tests
# BMS619-2;BMS619/2TEN,10 x 96 tests|BMS619/2,96 tests
# BMS618-3;BMS618/3TEN,10 x 96 tests|BMS618/3,96 tests
# BMS603-2;BMS603/2TEN,10 x 96 tests|BMS603/2TWO,2 x 96 tests|BMS603/2,96 tests
# BMS640-3INST;BMS640/3INST,128 tests
# BMS650-4;BMS650/4TEN,10 x 96 tests|BMS650/4,96 tests
# BMS642-2;BMS642/2TEN,10 x 96 tests|BMS642/2,96 tests
# BMS643-2;BMS643/2FIVE,5 x 96 tests|BMS643/2TEN,10 x 96 tests|BMS643/2TWO,2 x 96 tests|BMS643/2,96 tests
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
# for i in range(1):
while r.exists("invitrogen_price"):
    redis_value = r.lpop("invitrogen_price")
    print(redis_value)
    main_cata = redis_value.split(";")[0]
    ending = redis_value.split(";")[1].split("|")
    for looper in ending:
        print(looper)
        sub_cata = looper.split(",")[0]
        size = looper.split(",")[1]
        url = f"https://www.thermofisher.com/api/magellan/pricing/{sub_cata}"
        print(url)
        with requests.Session() as s:
            resp = s.get(url=url, headers=headers)
            text = resp.text
            try:
                json_data = json.loads(text)
            except Exception as e:
                print(e)
                r.rpush("invitrogen_price_fix", redis_value)
                continue
            try:
                price = json_data["myPriceData"][sub_cata]["pricingList"][
                    "pricingDetails"
                ][0]["priceString"]
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
            print("done")
            session.close()
        time.sleep(random.uniform(1.0, 1.5))
    print("DONE")
