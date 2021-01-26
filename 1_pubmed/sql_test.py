from contextlib import contextmanager
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exists
from sqlalchemy.sql import func
import redis
import time

Base = declarative_base()


class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    # id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    PMID_version = Column(String(50), nullable=True, comment="")
    Date_revised = Column(String(20), nullable=True, comment="")
    Journal_title = Column(String(200), nullable=True, comment="")
    Journal_ISSN_print = Column(String(40), nullable=True, comment="")
    Journal_ISSN_electronic = Column(String(40), nullable=True, comment="")
    Journal_ISSN_link = Column(String(40), nullable=True, comment="")
    Journal_vol = Column(String(20), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbreviation = Column(String(50), nullable=True, comment="")
    Journal_UniqueID = Column(String(50), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pub_date = Column(String(20), nullable=True, comment="")
    Article_pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_pii = Column(String(50), nullable=True, comment="")
    Article_doi = Column(String(50), nullable=True, comment="")
    Article_pmc = Column(String(50), nullable=True, comment="")
    Article_abstract = Column(Text, nullable=True, comment="")
    Article_keyword = Column(String(2000), nullable=True, comment="")
    Article_type = Column(String(200), nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    Article_language = Column(String(10), nullable=True, comment="")


class Temp(Base):
    __tablename__ = "Temp_PMId"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmId = Column(Integer, nullable=True, comment="")
    version = Column(Integer, nullable=True, comment="")
    xmlName = Column(String(40), nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

# it_exists = session.query(
#     exists().where(Temp.pmId == Detail.Article_pmid)
# ).scalar()
list_a = []
# aaa = session.query(Temp).filter(
#     Temp.id == 960965).all()
# for i in aaa:
#     print(i.id)
#     print(i.version)
#     print(i.xmlName)
#     del i.id
#     print(i.id)
# print(list_a)
# session.query(Temp).delete(
#     synchronize_session='evaluate')
# aaa = session.query(Temp).filter(
#     Temp.pmId == Detail.Article_pmid).all()
# print(aaa)
session.commit()
session.close()
