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


class Images(Base):
    __tablename__ = "invitrogen_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
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
while r.exists("invitrogen_image"):
    catanum = r.lpop("invitrogen_image")
    url = f"https://www.thermofisher.com/order/genome-database/antibody-figures?prodType=IMMA&assayId={catanum}"
    print(catanum)
    with requests.Session() as s:
        resp = s.get(url=url)
        text = resp.text
        try:
            json_data = json.loads(text)
            img_url = json_data[0]["links"][0]["url"]["src"]
            img_des = json_data[0]["links"][0]["description"]
            print(img_url, img_des)
        except Exception:
            print("没图")
            continue
        new_images = Images(
            Catalog_Number=catanum, Image_url=img_url, Image_description=img_des
        )
        session.add(new_images)
        session.commit()
        session.close()
        print("done")
        time.sleep(random.uniform(1.0, 1.5))
