import requests
from lxml import etree
import redis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Detail(Base):
    __tablename__ = "alomone_antibody_detail"

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


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)

task_list = session.query(Detail.Catalog_Number, Detail.Price_url).all()
for i in task_list:
    catano = i[0]
    num = i[1].split(",")[1]
    item = catano + "," + num
    r.rpush("alomone_price", item)
    print(item)

pool.disconnect()
