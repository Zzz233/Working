from elasticsearch import Elasticsearch
from elasticsearch import helpers
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Data(Base):
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


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

# 使用之前需要修改mysql类中的self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)，使输出格式变为字典

results = session.query(Data).limit(30000).all()
print(results)
# print(results[1].platform)
es = Elasticsearch()
# mysql = MYSQL('pubmed_article')

# select_sql = 'select * from pubmed_article_detail where id between 1 and 3044037;'
# results = mysql.show_all(select_sql)
# print(results)
# print(es.indices.get_mapping(index='r6_test'))
#
alias_action = (
    {
        "_index": "pubmed_ceshi",
        "_id": i.Article_pmid,
        "_source": {
            # "id"i.id"],
            "Journal_title": i.Journal_title,
            "journal_abbreviation": i.Journal_abbreviation,
            "Article_title": i.Article_title,
            "Pub_date": i.Pub_date,
            'Article_pmid': i.Article_pmid,
            'Article_doi': i.Article_doi,
            'Article_pmc': i.Article_pmc,
            'Article_abstract': i.Article_abstract,
            'Article_keyword': i.Article_keyword,
            'Article_type': i.Article_type
        }
    } for i in results)

# print(alias_action, next(alias_action))
helpers.bulk(es, alias_action)
