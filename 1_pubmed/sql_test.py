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


class Reference(Base):
    __tablename__ = "Article_reference"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    cited_pmid = Column(Integer, primary_key=True, nullable=True, comment="")





# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

# it_exists = session.query(
#     exists().where(Temp.pmId == Detail.Article_pmid)
# ).scalar()
# list_a = []
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
temp_1 = Reference(pmid=1, cited_pmid=2)
# temp_2 = Reference(pmid=1, cited_pmid=2)
# if temp_1 == temp_2:
#     print(1)
# else:
#     print(2)
# Below will return True or False
query = session.query(Reference).filter(
    temp_1
)
print(query)

session.commit()
session.close()


# 获取3W条后比对pmid和version号 去重