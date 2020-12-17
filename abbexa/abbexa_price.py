import requests
from lxml import etree
from requests.api import options
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


class Detail(Base):
    __tablename__ = "abbexa_kit_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Kit_Type = Column(String(100), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Tests = Column(String(200), nullable=True, comment="")
    Assay_type = Column(String(200), nullable=True, comment="")
    Detection_Method = Column(String(200), nullable=True, comment="")
    Sample_type = Column(String(1000), nullable=True, comment="")
    Assay_length = Column(String(200), nullable=True, comment="")
    Sensitivity = Column(String(200), nullable=True, comment="")
    Assay_range = Column(String(200), nullable=True, comment="")
    Specificity = Column(String(200), nullable=True, comment="")
    Target_Protein = Column(String(200), nullable=True, comment="")
    GeneId = Column(String(500), nullable=True, comment="")
    SwissProt = Column(String(500), nullable=True, comment="")
    DataSheet_URL = Column(String(500), nullable=True, comment="")
    Review = Column(String(50), nullable=True, comment="")
    Image_qty = Column(Integer, nullable=True, comment="")
    Citations = Column(Integer, nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Conjugate = Column(String(200), nullable=True, comment="")
    Species_Reactivity = Column(String(200), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "abbexa_kit_price"

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

for i in range(1):
    str = "abx595812|https://www.abbexa.com/25-hydroxyvitamin-d3-hvd3-elisa-kit|{'product_id': '419541', 'option_id': '7407122', 'array': [['96 tests', '15078720']]}"
    catano = str.split("|")[0]
    link = str.split("|")[1]
    meta = str.split("|")[2].replace("'", '"')
    json_data = json.loads(meta)
    product_id = json_data["product_id"]
    option_id = json_data["option_id"]
    for item in json_data["array"]:
        size = item[0]
        option_value = item[1]
        option_key = f"option[{option_id}]"
        with requests.session() as s:
            data = {
                option_key: option_value,
                "quantity": "1",
                "product_id": product_id,
            }
            headers = {
                "Host": "www.abbexa.com",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
                "Accept-Encoding": "gzip, deflate, br",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Length": "57",
                "Origin": "https://www.abbexa.com",
                "Connection": "keep-alive",
                "Referer": link,
                # "Cookie": "PHPSESSID=90554d0188c8e2a7fa47551bdc3065c6; language=en; currency=USD; _gcl_au=1.1.678391277.1608168886; _ga=GA1.2.61988102.1608168887; _gid=GA1.2.713055285.1608168887; _gat_gtag_UA_41647028_1=1",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            }
            url = "https://www.abbexa.com/index.php?route=module%2Foption_change_price_change%2FgetNewPrice"
            resp = s.post(url=url, data=data, headers=headers)
            result_json = resp.json()
            price = result_json["price"]
            print(catano, size, price)
