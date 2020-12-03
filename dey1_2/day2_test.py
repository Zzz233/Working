import requests
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker

working_list = [
    'https://www.citeab.com/antibodies/1473591-mabd64-anti-achaete-scute-homolog-1-antibody-clone/publications?page=1',
    'https://www.citeab.com/antibodies/1072326-orb10143-mash1-achaete-scute-homolog-1-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/1665845-bs-1155r-ascl1-polyclonal-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/1488995-hpa029217-anti-ascl1/publications?page=1',
    'https://www.citeab.com/antibodies/82835-61271-mash1-antibody-mab/publications?page=1',
    'https://www.citeab.com/antibodies/1472806-gp155-anti-arvcf-guinea-pig-polyclonal-serum/publications?page=1',
    'https://www.citeab.com/antibodies/787998-sc-23874-arvcf-antibody-4b1/publications?page=1',
    'https://www.citeab.com/antibodies/252364-h00000421-m01-arvcf-monoclonal-antibody-m01-clone-5d2/publications?page=1',
    'https://www.citeab.com/antibodies/657581-a303-310a-arvcf-antibody/publications?page=1',
    'https://www.citeab.com/antibodies/407246-h00000421-m01-arvcf-antibody-5d2/publications?page=1', ]

Base = declarative_base()


class Data(Base):
    __tablename__ = 'citeab_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    company = Column(String(45),
                     nullable=True, comment='')
    catno = Column(String(45),
                   nullable=True, comment='')
    citationQty = Column(Integer,
                         nullable=True, comment='')
    title = Column(String(300),
                   nullable=True, comment='')
    title_link = Column(String(300),
                        nullable=True, comment='')
    pdf_link = Column(String(100),
                      nullable=True, comment='')
    journal = Column(String(100),
                     nullable=True, comment='')
    public_date = Column(DateTime,
                         nullable=True, comment='')
    application = Column(String(200),
                         nullable=True, comment='')
    reactivity = Column(String(50),
                        nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

a = ('Progen Biotechnik', 'GP155', 4,
     'Involvement of the Areae Compositae of the Heart in Endemic Pemphigus Foliaceus.',
     '/publication/7734796-31384490-involvement-of-the-areae-compositae-of-the-heart-in',
     'http://europepmc.org/articles/PMC6659599?pdf=render',
     'Dermatology Practical Conceptual', '1 July 2019', 'IHC-Fr-IF',
     'Homo sapiens (Human)')
new_data = Data(company=a[0], catno=a[1],
                citationQty=a[2], title=a[3],
                title_link=a[4], pdf_link=a[5],
                journal=a[6], public_date=a[7],
                application=a[8], reactivity=a[9])

print(type(a))
session.add(new_data)
session.commit()
session.close()
