import requests
from lxml import etree
from requests.api import head
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "origene_kit_list"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

headers = {
    "Host": "www.origene.com.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.origene.com.cn/category/assay-kits/elisa-kits",
    # "Cookie": "JSESSIONIDSITE=E0E8C5C4FF9793F280918DF3F8748530; OrigeneActiveID=OZZY-5JBV-LFHG-XFGB-K379-FSA3-0XVK-JVK7; ActiveID=Z0JQ-5005-SJXH-W89I-I7FK-5YC3-8QNL-KGD1; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=CN; Hm_lvt_3edf31df29cb41b05124e59e6257d775=1607577704; Hm_lpvt_3edf31df29cb41b05124e59e6257d775=1607577850; cookieconsent_status=dismiss; _uetsid=984795203aa711eb80fb4b4f5e67729e; _uetvid=984802503aa711eb8cff59229bd7a4d7",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
brand = "origene"
for i in range(2, 203):
    url = f"https://www.origene.com.cn/category/assay-kits/elisa-kits?page={i}"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=60)
        lxml = etree.HTML(resp.text)
        items = lxml.xpath('//article[@class="container"]')
        for item in items:
            link = (
                "https://www.origene.com.cn"
                + item.xpath('.//a[@class="name"]/@href')[0].strip()
            )
            name = item.xpath('.//a[@class="name"]/text()')[0].strip()
            sku = item.xpath('.//a[@class="sku"]/text()')[0].strip()
            # print(sku, name, link)
            new_data = Data(
                Brand=brand, Catalog_Number=sku, Product_Name=name, Detail_url=link
            )
            session.add(new_data)
            try:
                session.commit()
                session.close()
            except Exception as e:
                session.rollback()
                print(e)
                pass
    print(i, "done")
    time.sleep(random.uniform(0.5, 1.0))
