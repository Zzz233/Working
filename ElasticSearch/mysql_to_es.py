from elasticsearch import Elasticsearch
from elasticsearch import helpers
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Data(Base):
    __tablename__ = "dataDump_s5_summary_operator_loadout"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    platform = Column(String(10), nullable=True, comment="")
    dateid = Column(String(10), nullable=True, comment="")
    skillrank = Column(String(10), nullable=True, comment="")
    role = Column(String(10), nullable=True, comment="")
    operator = Column(String(25), nullable=True, comment="")
    primaryweapon = Column(String(25), nullable=True, comment="")
    secondaryweapon = Column(String(25), nullable=True, comment="")
    secondarygadget = Column(String(25), nullable=True, comment="")
    nbwins = Column(String(5), nullable=True, comment="")
    nbkills = Column(String(5), nullable=True, comment="")
    nbdeaths = Column(String(5), nullable=True, comment="")
    nbpicks = Column(String(5), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:pop645649968@127.0.0.1:3306/try_csv?charset=utf8")
DBSession = sessionmaker(bind=engine)
session = DBSession()

# 使用之前需要修改mysql类中的self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)，使输出格式变为字典

results = session.query(Data).filter(Data.id < 1000).all()
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
        "_index": "r6_test",
        "_id": i.id,
        "_source": {
            # "id": i["id"],
            "platform": i.platform,
            "dateid": i.dateid,
            "skillrank": i.skillrank,
            "role": i.role,
            "operator": i.operator,
            "primaryweapon": i.primaryweapon,
            "secondaryweapon": i.secondaryweapon,
            "secondarygadget": i.secondarygadget,
            "nbwins": i.nbwins,
            "nbkills": i.nbkills,
            "nbdeaths": i.nbdeaths,
            "nbpicks": i.nbpicks,
        },
    } for i in results)

# print(alias_action, next(alias_action))
helpers.bulk(es, alias_action)
