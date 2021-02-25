from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index, JSON
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import requests
import redis
import time
import random
from contextlib import contextmanager
from sqlalchemy import and_

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


class Journal(Base):
    __tablename__ = "journal"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Abbr_Pubmed = Column(String(70), nullable=True, comment="")
    ISSN = Column(String(40), nullable=True, comment="")
    Abbr = Column(String(200), nullable=True, comment="")
    IPinfo = Column(JSON, nullable=True, comment="")
    impact_factor = Column(String(20), nullable=True, comment="")
    journal_name = Column(String(500), nullable=True, comment="")




engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=14)
r = redis.Redis(connection_pool=pool)

task_list = session.query(Data).filter(Data.issn == '2159-8274').all()
# task_list = session.query(Detail.Catalog_Number).filter(Detail.Citations != "0").all()
for i in task_list:
    issn = i.issn
    journal_name = i.journal_name
    pmcid = i.pmcid
    pmid = i.pmid
    article_type = i.article_type
    item = ','.join((pmid, pmcid, issn, journal_name, article_type))
    r.rpush("half_1", item)
    print(item)

pool.disconnect()
