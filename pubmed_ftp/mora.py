from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis

Base = declarative_base()


class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    PMID_version = Column(String(50), nullable=True, comment="")
    Date_revised = Column(String(20), nullable=True, comment="")
    Journal_title = Column(String(200), nullable=True, comment="")
    Journal_ISSN_print = Column(String(40), nullable=True, comment="")
    Journal_ISSN_electronic = Column(String(40), nullable=True, comment="")
    Journal_vol = Column(String(20), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbreviation = Column(String(50), nullable=True, comment="")
    Journal_UniqueID = Column(String(50), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pubdate = Column(String(20), nullable=True, comment="")
    Article_pmid = Column(String(30), nullable=True, comment="")
    Article_pii = Column(String(50), nullable=True, comment="")
    Article_doi = Column(String(50), nullable=True, comment="")
    Article_pmc = Column(String(50), nullable=True, comment="")


class Abstract(Base):
    __tablename__ = "article_abstract"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    Abctract = Column(Text, nullable=True, comment="")


class Keyword(Base):
    __tablename__ = "article_keyword"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    Key_word = Column(String(200), nullable=True, comment="")


class Info(Base):
    __tablename__ = "author_info"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    LastName = Column(String(100), nullable=True, comment="")
    ForeName = Column(String(100), nullable=True, comment="")
    Initials = Column(String(10), nullable=True, comment="")
    Affiliation = Column(String(2000), nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)


class Pubmed:
    def __init__(self):
        self.base_path = "D:/Dev/FTP"

    def get_path(self):
        while r.exists("ftp_file_path"):
            path = self.base_path + r.rpop("ftp_file_path")
            yield path

    def get_content(self, path):
        xml = etree.parse(path)
        if xml:
            return xml
        return None

    def parse_xml(self, xml):
        articles = xml.xpath("//PubmedArticle")
        for item in articles:
            yield item

    def parse_item(self, item):
        # ? pubmed_article_detai 表
        # todo PMID_version
        pmid_version_text = item.xpath('.//PMID[@Version="1"]/text()')
        pmid_version = ",".join(i for i in pmid_version_text)
        if len(pmid_version) == 0:
            pmid_version = "Error"
        # todo Date_revised
        date_revised_text = item.xpath(".//DateRevised/*/text()")
        date_revised = "-".join(i.strip() for i in date_revised_text)
        if len(date_revised) == 0:
            date_revised = None
        # todo Journal_title
        try:
            journal_title = item.xpath(".//Title/text()")[0].strip()
        except Exception:
            journal_title = None
        # todo Journal_ISSN_print
        try:
            journal_issn_print = item.xpath('.//ISSN[@IssnType="Print"]/text()')[
                0
            ].strip()
        except Exception:
            journal_issn_print = None
        # todo Journal_ISSN_electronic
        try:
            journal_issn_electronic = item.xpath(
                './/ISSN[@IssnType="Electronic"]/text()'
            )[0].strip()
        except Exception:
            journal_issn_electronic = None
        # todo Journal_vol
        try:
            journal_vol = item.xpath(".//Volume/text()")[0].strip()
        except Exception:
            journal_vol = None
        # todo Journal_issue
        try:
            journal_issue = item.xpath(".//Issue/text()")[0].strip()
        except Exception:
            journal_issue = None
        # todo Journal_abbreviation
        try:
            journal_abbreviation = item.xpath(".//ISOAbbreviation/text()")[0].strip()
        except Exception:
            journal_abbreviation = None
