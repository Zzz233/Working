from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Citations(Base):
    __tablename__ = 'elisa_kit_citations_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    PMID = Column(String(40),
                  nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    Sample_type = Column(String(100),
                         nullable=True, comment='')
    Pubmed_url = Column(String(1000),
                        nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Detail(Base):
    __tablename__ = 'elisa_kit_detail_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(100),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Detail_url = Column(String(1000),
                        nullable=True, comment='')
    Tests = Column(String(200),
                   nullable=True, comment='')
    Assay_type = Column(String(200),
                        nullable=True, comment='')
    Sample_type = Column(String(1000),
                         nullable=True, comment='')
    Assay_length = Column(String(200),
                          nullable=True, comment='')
    Sensitivity = Column(String(200),
                         nullable=True, comment='')
    Assay_range = Column(String(200),
                         nullable=True, comment='')
    Specificity = Column(String(200),
                         nullable=True, comment='')
    GeneId = Column(String(500),
                    nullable=True, comment='')
    SwissProt = Column(String(500),
                       nullable=True, comment='')
    DataSheet_URL = Column(String(500),
                           nullable=True, comment='')
    Review = Column(String(20),
                    nullable=True, comment='')
    Image_qty = Column(Integer,
                       nullable=True, comment='')
    Citations = Column(String(20),
                       nullable=True, comment='')
    Synonyms = Column(String(3000),
                      nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


class Images(Base):
    __tablename__ = 'elisa_kit_images_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Image_url = Column(String(500),
                       nullable=True, comment='')
    Image_description = Column(String(2000),
                               nullable=True, comment='')
    Kit_Status = Column(String(20),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()
