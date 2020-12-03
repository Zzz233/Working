import redis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_, and_
from sqlalchemy.sql import func

Base = declarative_base()


class Data(Base):
    __tablename__ = 'citeab_citations2'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(400),
                            nullable=True, comment='')
    Company = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    pmid = Column(String(100),
                  nullable=True, comment='')
    citeab_href = Column(String(500),
                         nullable=True, comment='')
    application = Column(String(300),
                         nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Pdf_url = Column(String(1000),
                     nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class List(Base):
    __tablename__ = 'citeab_citations_list_urls'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Company = Column(String(100),
                     nullable=True, comment='')
    list_url = Column(String(1000),
                      nullable=True, comment='')
    url_status = Column(String(20),
                        nullable=True, comment='')
    Crawl_Date = Column(DateTime,
                        nullable=True, comment='')

    def to_dict(self):
        return {
            'id': self.id,
            'Company': self.Company,
            'list_url': self.list_url,
            'url_status': self.url_status,
            'Crawl_Date': self.Crawl_Date,
        }


engine = create_engine(
    'mysql+pymysql://root:app1234@192.168.124.10:3306/citeab?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()
