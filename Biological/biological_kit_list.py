from collections import namedtuple
import requests
from lxml import etree
from requests.api import head
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "abbexa_kit_list"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

headers = {
    "Host": "api.usbio.net",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/json;charset=utf-8",
    "Content-Length": "222",
    "Origin": "https://www.usbio.net",
    "Connection": "keep-alive",
    "Referer": "https://www.usbio.net/",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
brand = "biological"

api_url = "https://api.usbio.net/api/search"
for i in range(0, 22510, 200):
    data = {
        "size": 15,
        "from": 45,
        "querystring": "",
        "filters": [
            {
                "field": "category",
                "value": "Kits and Assays",
                "display": "Category",
                "removable": false,
            },
            {
                "field": "category2",
                "value": "ELISA",
                "display": "Secondary Category",
                "removable": true,
            },
        ],
    }
    with requests.Session() as s:
        resp = s.post(url=api_url, data=data, headers=headers)
        print(resp.text)
    break
