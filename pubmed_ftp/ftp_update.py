"""
使用集合（set）。
相比于使用列表来存储数据集，
将列表集合化（以列表为参数，使用set函数进行初始化），而判断是否包含仍用in：

key_list = [1, 2, 3, 4, 5, 6, 7, 8]
key = 10
key_set = set(key_list)
if key in key_list:
    print("Hello!")
"""
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time

# Base = declarative_base()


# class Detail(Base):
#     __tablename__ = "pubmed_article_detail"

#     id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
#     PMID_version = Column(String(50), nullable=True, comment="")
#     Date_revised = Column(String(20), nullable=True, comment="")
#     Journal_title = Column(String(200), nullable=True, comment="")
#     Journal_ISSN_print = Column(String(40), nullable=True, comment="")
#     Journal_ISSN_electronic = Column(String(40), nullable=True, comment="")
#     Journal_vol = Column(String(20), nullable=True, comment="")
#     Journal_issue = Column(String(20), nullable=True, comment="")
#     Journal_abbreviation = Column(String(50), nullable=True, comment="")
#     Journal_UniqueID = Column(String(50), nullable=True, comment="")
#     Article_title = Column(String(2000), nullable=True, comment="")
#     Pubdate = Column(String(20), nullable=True, comment="")
#     Article_pmid = Column(String(30), nullable=True, comment="")
#     Article_pii = Column(String(50), nullable=True, comment="")
#     Article_doi = Column(String(50), nullable=True, comment="")
#     Article_pmc = Column(String(50), nullable=True, comment="")


# engine = create_engine(
#     "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8mb4"
# )
# DBSession = sessionmaker(bind=engine)
# session = DBSession()

# task = session.query(Detail.PMID_version).all()
# data = (i[0] for i in task)
# for item in data:
#     print(item)
#     print(type(item))
#     break
def check_data(path):
    data_list = []
    xml = etree.parse(path)
    articles = xml.xpath("//PubmedArticle")
    for item in articles:
        try:
            article_pmid = item.xpath('.//ArticleId[@IdType="pubmed"]/text()')[
                0
            ].strip()
            data_list.append(article_pmid)
        except Exception:
            article_pmid = None
    return data_list


if __name__ == "__main__":
    data_1 = check_data(r"D:\Dev\FTP_DATA\pubmed21n1062.xml")
    data_2 = check_data(r"D:\Dev\FTP_DATA\pubmed21n1063.xml")
    print(list(set(data_1).intersection(set(data_2))))