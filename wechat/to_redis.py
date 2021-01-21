from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import requests
import redis
import time
import random
from lxml import etree
from html.parser import HTMLParser

company = 'BioArt'

Base = declarative_base()


class Data(Base):
    __tablename__ = "article_analytic"

    Id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Company = Column(String(100), nullable=True, comment="")
    # appmsgid = Column(Integer, nullable=True, comment="")
    digest = Column(String(200), nullable=True, comment="")
    Author = Column(String(100), nullable=True, comment="")
    Title = Column(String(400), nullable=True, comment="")
    Link = Column(String(200), nullable=True, comment="")
    Cover_Img = Column(String(100), nullable=True, comment="")
    create_time = Column(DateTime, nullable=True, comment="")
    update_time = Column(DateTime, nullable=True, comment="")
    pmId = Column(Integer, nullable=True, comment="")
    doi = Column(String(50), nullable=True, comment="")
    content = Column(Text, nullable=True, comment="")
    add_time = Column(DateTime, nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)

task_list = session.query(Data.Link).all()
for i in task_list:
    item = i[0]
    r.rpush('wechat_url', item)
    print(item)

pool.disconnect()
