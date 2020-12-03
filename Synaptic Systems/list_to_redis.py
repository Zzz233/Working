import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
from sqlalchemy_sql import List

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=1)
r = redis.Redis(connection_pool=pool)

engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

task_list = session.query(List.Antibody_detail_URL, List.Note).all()
for i in task_list:
    print(i[0], i[1])
    r.rpush('sysy', i[0] + ',' + i[1])

pool.disconnect()
