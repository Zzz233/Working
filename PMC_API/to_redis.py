from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import requests
import redis
import time
import random
from contextlib import contextmanager

Base = declarative_base()


class Data(Base):
    __tablename__ = "biomedical_journal_pmc_article"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    issn = Column(String(20), nullable=True, comment="")
    journal_name = Column(String(50), nullable=True, comment="")
    pmcid = Column(String(20), nullable=True, comment="")
    pmid = Column(String(20), nullable=True, comment="")
    article_type = Column(String(20), nullable=True, comment="")
    # article_status = Column(String(5), nullable=True, comment="0 未爬取 1爬取成功 2不存在")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=14)
r = redis.Redis(connection_pool=pool)

task_list = session.query(Data).filter(Data.journal_name=='J Clin Oncol').all()
# task_list = session.query(Detail.Catalog_Number).filter(Detail.Citations != "0").all()
for i in task_list:
    issn = i.issn
    journal_name = i.journal_name
    pmcid = i.pmcid
    pmid = i.pmid
    article_type = i.article_type
    item = ','.join((pmid, pmcid, issn, journal_name, article_type))
    r.rpush("half", item)
    print(item)

pool.disconnect()
