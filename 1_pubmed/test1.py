import redis
import time
from contextlib import contextmanager
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=0)
r = redis.Redis(connection_pool=pool)

date_json = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

Base = declarative_base()


class Corrections(Base):
    __tablename__ = "article_comments_corrections"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    comments_corrections_pmid = Column(String(20), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Grant(Base):
    __tablename__ = "article_grant"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Grant_id = Column(String(500), nullable=True, comment="")
    Grant_agency = Column(String(2000), nullable=True, comment="")
    Grant_agency_acronym = Column(String(200), nullable=True, comment="")
    Grant_country = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Keyword(Base):
    __tablename__ = "Article_keyword"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Key_word = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Reference(Base):
    __tablename__ = "Article_reference"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    cited_pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Type(Base):
    __tablename__ = "article_type"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_type = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Author(Base):
    __tablename__ = "Author_info"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    LastName = Column(String(100), nullable=True, comment="")
    ForeName = Column(String(100), nullable=True, comment="")
    Initials = Column(String(20), nullable=True, comment="")
    Affiliation = Column(String(2000), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    Journal_title = Column(String(250), nullable=True, comment="")
    Journal_issn = Column(String(40), nullable=True, comment="")  # todo 原 Journal_ISSN_link
    Journal_vol = Column(String(40), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbr = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pub_date = Column(String(20), nullable=True, comment="")  # todo 原来是String现在是Date
    Article_doi = Column(String(100), nullable=True, comment="")
    Article_pmc = Column(String(100), nullable=True, comment="")
    Article_abstract = Column(Text, nullable=True, comment="")
    Article_keyword = Column(String(1000), nullable=True, comment="")
    Article_type = Column(String(300), nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    Article_language = Column(String(10), nullable=True, comment="")
    Authors = Column(String(1000), nullable=True, comment="")
    Institutions = Column(String(2000), nullable=True, comment="")
    Journal_if = Column(String(20), nullable=True, comment="")
    Cited_qty = Column(Integer, nullable=True, comment="")
    # Version = Column(Integer, nullable=True, comment="")


class Temp(Base):
    __tablename__ = "temp"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmid = Column(Integer, nullable=True, comment="")
    xmlname = Column(String(40), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Temp_1(Base):
    __tablename__ = "temp_copy1"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmid = Column(Integer, nullable=True, comment="")
    xmlname = Column(String(40), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


a = session.query(Detail).join(Temp_1, Detail.pmid == Author.pmid)
a.delete()
session.close()
print(a)
