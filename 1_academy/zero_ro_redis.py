import requests
from lxml import etree
import redis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random
from sqlalchemy import and_

Base = declarative_base()


class Data(Base):
    __tablename__ = "academy"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    AcademyId = Column(String(50), nullable=True, comment="")
    AcademyName = Column(String(200), nullable=True, comment="")
    S1Id = Column(String(50), nullable=True, comment="")
    S1Name = Column(String(200), nullable=True, comment="")
    S2Id = Column(String(50), nullable=True, comment="")
    S2Name = Column(String(200), nullable=True, comment="")
    S3Id = Column(String(50), nullable=True, comment="")
    S3Name = Column(String(200), nullable=True, comment="")
    TotalRecord = Column(Integer, nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=4)
r = redis.Redis(connection_pool=pool)

task_list = (
    session.query(Data.AcademyId, Data.S1Id, Data.S2Id, Data.S3Id)
    .filter(and_(Data.TotalRecord == 0, Data.AcademyName == None))
    .all()
)
# task_list = (
#     session.query(Data.S1Id, Data.S2Id, Data.S3Id).filter(Data.TotalRecord == 0).all()
# )
# print(task_list)

for i in task_list:
    print(i)
    s1 = str(i[0])
    s2 = str(i[1])
    s3 = str(i[2])
    s4 = str(i[3])
    item = ",".join((s1, s2, s3, s4))
    r.rpush("academy", item)
    print(item)

pool.disconnect()
