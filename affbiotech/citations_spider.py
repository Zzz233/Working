import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Data(Base):
    __tablename__ = 'bp_antibody_citations_test'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    PMID = Column(String(40),
                  nullable=True, comment='')
    Application = Column(String(300),
                         nullable=True, comment='')
    Species = Column(String(100),
                     nullable=True, comment='')
    Article_title = Column(String(1000),
                           nullable=True, comment='')
    Pubmed_url = Column(String(1000),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')

    def to_dict(self):
        return {
            'id': self.id,
            'company': self.company,
            'catno': self.catno,
            'citationQty': self.citationQty,
            'title': self.title,
            'title_link': self.title_link,
            'pdf_link': self.pdf_link,
            'journal': self.journal,
            'public_date': self.public_date,
            'application': self.application,
            'reactivity': self.reactivity,
        }


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
print(engine)
DBSession = sessionmaker(bind=engine)
session = DBSession()

brand = 'Affinity Biosciences'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0)'
                  ' Gecko/20100101 Firefox/82.0'
}
for i in range(1, 359):
    url = f'http://www.affbiotech.cn/citation.php?act=list&page={i}'

    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        html = etree.HTML(resp.text)

    list_single = html.xpath(
        '//table[@class="table table-bordered table-striped table-hover"]/tr[count(td)=5]')
    for item in list_single:
        cat_no = item.xpath('./td/a[contains(@href,"goods-")]/text()')
        if not cat_no:
            cat_no = None
        pm_id = item.xpath('./td[4]/a[contains(@href,"pubmed")]/text()')
        if not pm_id:
            pm_id = None
        application = None
        species = None
        article_title = item.xpath('./td[2]/text()')
        if not article_title:
            article_title = None
        pub_med_url = item.xpath('./td[4]/a[contains(@href,"pubmed")]/@href')
        if not pub_med_url:
            pub_med_url = None
        note = None
        # print(cat_no, pm_id, article_title, pub_med_url)

        new_data = Data(Catalog_Number=cat_no,
                        PMID=pm_id,
                        Application=application,
                        Species=species,
                        Article_title=article_title,
                        Pubmed_url=pub_med_url,
                        Note=note
                        )
        session.add(new_data)
        session.commit()
        session.close()
    print(i, 'done')
