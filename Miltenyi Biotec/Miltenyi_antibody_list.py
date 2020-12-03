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

Base = declarative_base()


class Data(Base):
    __tablename__ = 'bp_miltenyi_list'

    id = Column(Integer,
                primary_key=True, autoincrement=True, comment='id')
    Brand = Column(String(40),
                   nullable=True, comment='')
    Catalog_Number = Column(String(40),
                            nullable=True, comment='')
    Product_Name = Column(String(200),
                          nullable=True, comment='')
    Application = Column(String(1000),
                         nullable=True, comment='')
    Antibody_detail_URL = Column(String(500),
                                 nullable=True, comment='')
    Crawl_Date = Column(DateTime, server_default=func.now(),
                        nullable=True, comment='')
    Note = Column(String(500),
                  nullable=True, comment='')
    Antibody_Status = Column(String(20),
                             nullable=True, comment='')
    Antibody_Type = Column(String(100),
                           nullable=True, comment='')


engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()

brand = 'Miltenyi Biotec'

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    # 'Cookie': '__cfduid=d4b312a48f4131ae67acee8c88767a2561605100165; Neos_Session=WzpmCmGkVe3TpBOtr9lU8kh401XNsTlh; MILTENYI_COUNTRY=CN; MILTENYI_SELECT_COUNTRY=CN; MILTENYI_APPLICATION=%5B%5D; cookieConsent={"technical":{"cookies":["cookieConsent","cookieConsentAccepted","MILTENYI_APPLICATION","MILTENYI_CART_COOKIE","MILTENYI_SSO","MILTENYI_COUNTRY","MILTENYI_SELECT_COUNTRY","displayCookieConsent","Neos_Session","_cfduid","__cfduid","invoiceUnitId","inventoryUnitId","transactionUnitId","PHPSESSID","XSRF-TOKEN","miltenyi_live_session","FluroFinder","Appcues"],"accepted":true},"statistics":{"cookies":["google","mouseflow","MovingImage"],"accepted":true},"comfort":{"cookies":["WYSIWYG_AB_TESTING","MILTENYI_SEARCH_TOGGLE_STATE","displayMaintenanceInfoConsent","albacross"],"accepted":true}}; cookieConsentAccepted=f71bdb49fa7673ec593e174296f1b4e5559f73f8; mf_a3088070-d6d9-4c82-9578-ad4203e624de=1f4e1378743838125caf745cf5cab95c|111752173c3bdf5f1cc00e63c0488cc8c161447d.2563751198.1605590032321|1605590132327||1|||0|17.26|49.48402; mf_user=cc1432282002cea4f44790fd95189ba7|; _ga_212KRGVEGB=GS1.1.1605590032.1.1.1605590035.56; _ga=GA1.2.512275361.1605590032; nQ_cookieId=bc760092-21c9-b5c1-2dac-b6f4a5a00321; nQ_userVisitId=afd5cb24-551e-340d-fb21-4442144f8570; _gid=GA1.2.116368359.1605590033; _fbp=fb.1.1605590033935.2055883271; _gat_UA-114945929-2=1',
    'Host': 'www.miltenyibiotec.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.miltenyibiotec.com/CN-en/search.html?search=antibody',
    'TE': 'Trailers',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
}
for i in range(2000, 21500, 100):
    url = f'https://www.miltenyibiotec.com/miltenyi-endpoints/ajax-searc' \
          f'h?node=/sites/site@live;country=CN,INTERNATIONAL&language=en&' \
          f'type=familyAjaxSearch&limit=100&from={i}&search=antibody&familyT' \
          f'oggleState=collapsed'
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=120)
        html_text = resp.json()['searchResult']
        # print(html_content)
        html = etree.HTML(html_text)
        # print(1)
        # TODO 条件
        if html.xpath(
                '//h3[@class="product-name"][@data-label="Product"]'):
            html_content = html.xpath(
                '//h3[@class="product-name"][@data-label="Product"]')
            status = 1
        else:
            status = 0

    for item in html_content:
        objects = []
        link = 'https://www.miltenyibiotec.com' + item.xpath('.//a/@href')[0]
        catno = item.xpath('.//a/@data-vendorproductids')[0].strip('["]')
        if catno == '':
            catno = None
        name = item.xpath('.//a//text()')
        name_str = ''.join(item for item in name).strip()
        print(link, catno, name_str)
        new_data = Data(Brand=brand,
                        Catalog_Number=catno,
                        Product_Name=name_str,
                        Antibody_detail_URL=link)
        objects.append(new_data)
        try:
            session.bulk_save_objects(objects)
            session.commit()
            session.close()
        except Exception as e:
            session.rollback()
            print(e)
    print(i, 'done')
    time.sleep(random.uniform(2, 2.5))
