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
from sqlalchemy import and_, or_

Base = declarative_base()


class Data(Base):
    __tablename__ = "biomedical_journal_pmc_article_1"

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


class Pic(Base):
    __tablename__ = "chemistry_journal_pmc_article"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    issn = Column(String(20), nullable=True, comment="")
    journal_name = Column(String(50), nullable=True, comment="")
    pmcid = Column(String(20), nullable=True, comment="")
    pmid = Column(String(20), nullable=True, comment="")
    article_type = Column(String(20), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.2.4:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="123.56.59.48", port=6379, decode_responses=True, db=14, password='!biopicky-2019')
r = redis.Redis(connection_pool=pool)
# 32 $ 44
# task_list = session.query(Data).filter(or_(Data.issn == '1741-7015',
#                                            Data.issn == '2044-5385',
#                                            Data.issn == '1467-7881',
#                                            Data.issn == '1674-800X',
#                                            Data.issn == '2211-1247',
#                                            Data.issn == '2168-6068',
#                                            Data.issn == '1527-8204',
#                                            Data.issn == '0390-6078',
#                                            Data.issn == '0091-3022',
#                                            Data.issn == '2001-1326',
#                                            Data.issn == '1044-5323',
#                                            Data.issn == '0022-3050',
#                                            Data.issn == '0945-053X',
#                                            Data.issn == '0161-6420',
#                                            Data.issn == '0168-3659',
#                                            Data.issn == '2235-1795',
#                                            Data.issn == '0300-5771',
#                                            Data.issn == '1540-1405',
#                                            Data.issn == '1949-0976',
#                                            Data.issn == '0031-5850',
#                                            Data.issn == '0028-646X',)).all()
task_list = session.query(Pic).all()

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
#
# task_list = session.query(Pic).all()
# for i in task_list:
#     issn = i.issn
#     journal_name = i.journal_name
#     pmcid = i.pmcid
#     pmid = i.pmid
#     article_type = i.article_type
#     item = ','.join((pmid, pmcid, issn, journal_name, article_type))
#     r.rpush("pic_task", item)
#     print(item)

