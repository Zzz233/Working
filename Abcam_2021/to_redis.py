import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from Random_UserAgent import get_request_headers
import time
import random
import redis

Base = declarative_base()


class Data(Base):
    __tablename__ = "abcam_antibody_list"

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Antibody_Type = Column(String(40),
                           nullable=True, comment='')
    Recombinant_Antibody = Column(String(10),
                                  nullable=True, comment='')
    Host_Species = Column(String(20),
                          nullable=True, comment='')
    Reactivity = Column(String(20),
                        nullable=True, comment='')
    Applications = Column(String(500),
                          nullable=True, comment='')
    Antibody_detail_URL = Column(String(2000),
                                 nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    GeneId = Column(String(20),
                    nullable=True, comment='')
    KO_Validation = Column(String(10),
                           nullable=True, comment='')
    Species_Reactivity = Column(String(500),
                                nullable=True, comment='')
    Recommened_Dilution = Column(String(200),
                                 nullable=True, comment='')
    SwissProt = Column(String(100),
                       nullable=True, comment='')
    Immunogen = Column(String(1000),
                       nullable=True, comment='')
    Calculated_MW = Column(String(20),
                           nullable=True, comment='')
    Observed_MW = Column(String(20),
                         nullable=True, comment='')
    Synonyms = Column(String(500),
                      nullable=True, comment='')
    Isotype = Column(String(20),
                     nullable=True, comment='')
    Purity = Column(String(20),
                    nullable=True, comment='')
    Citation_Amount = Column(Integer,
                             nullable=True, comment='')
    DataSheet_URL = Column(String(500),
                           nullable=True, comment='')

# Mysql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/2021_antibody_info?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)


task_list = session.query(Data.Antibody_detail_URL).all()
for i in task_list:
    r.rpush('abcam', i[0])
print('done')