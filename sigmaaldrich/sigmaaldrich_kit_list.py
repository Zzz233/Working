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
    "Host": "www.sigmaaldrich.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "x-dtpc": "1$188337578_888h30vFCMAOCSJCHEPWBKICVPNKPCCNHIUKAQG-0e11",
    "Content-Length": "13",
    "Origin": "https://www.sigmaaldrich.com",
    "Connection": "keep-alive",
    "Referer": "https://www.sigmaaldrich.com/life-science/cell-biology/antibodies/antibody-products.html?TablePage=111681178",
    # "Cookie": "TLTSID=01F6C6823AC0103A03C79A8031C9CEF3; TLTUID=01F6C6823AC0103A03C79A8031C9CEF3; dtCookie=v_4_srv_1_sn_13BB5D0ED5948141AEA6ED9E034DA3CC_perc_79544_ol_0_mul_2_app-3A1686b6dcc9c2ca9b_1; ak_bmsc=CD1D1FFC441ED218F209C5F22CE7E6FD~000000000000000000000000000000~YAAQxTArF+SEXgV2AQAAZ+q6Swow/nDQ/E7RVdSs/gX0yeLTzaNdpMQyE7fMVn6YxJJWiIisKw2QPjIb/mC9+5mrvOP6srJaDQuEmdHwqj1LamzPx0Tgl9cGG45hTgyi4L13l4FQq04GwjRGRbKARFgThf4zVwQrT4YQLB9fhN1cMZ2qDwp3qLRkbIm/XL0mSyP+AtSVR6Sf64fK4NeoZKkZ0OqXlEfnZlrAFqRLZ+maeQF5JPvpht5IkemDyyhBBGaS9I7mntUpnlQDFcdqtNaVAc2u5eTS7WolDH1ESRZLdfwiwdrMLN+9oQ2+rNxxAvZGsIrWOJo54ZLyzCvQnQRivGTJfhNzhY+c+fqGsLmv57VWpgYYLlTjauq8QCZOCqqcaGYkqlgTMT+fu4T3ucdXaPzp/UxrgfOBcU2RBKuQIy1KhLMdqnK/7f0WXsDwL4qU4ITCT9PJKzXhL3Xu4RHLMOp3iunZLcAgV49R7Bl+Gif6UA==; rxVisitor=1607588308074S1JAFMH6UTCL4A05QFBSLEUB17SBAS36; dtPC=1$188337578_888h30vFCMAOCSJCHEPWBKICVPNKPCCNHIUKAQG-0e11; rxvt=1607590397350|1607588308076; dtLatC=9; SessionPersistence=CLIENTCONTEXT%3A%3DvisitorId%253Danonymous%7CPROFILEDATA%3A%3Davatar%253D%252Fetc%252Fdesigns%252Fdefault%252Fimages%252Fsocial%252Favatar.png%252Cpath%253D%252Fhome%252Fusers%252Fa%252Fanonymous%252Fprofile%252CisLoggedIn%253Dfalse%252CisLoggedIn_xss%253Dfalse%252CauthorizableId%253Danonymous%252CauthorizableId_xss%253Danonymous%252CformattedName%253Danonymous%252CformattedName_xss%253Danonymous; bm_sv=BC9C44875D7F376CF0597F7BD8CDA95B~zigGEdCp29SkUbsjT/09WS1qSN3hM12BS/9cbUSAm1k/NvT5xZRVjQH9+6OsMmSAFDZbexGZnX3eodDktCT0Bdk3BmE8arsXQUW3rjhiDCt1Qt2ykJyNwwd2TW5buTPxpDmOI95lzb8z/eUgD3iXzyH1CqBz7Q5grAwgRAvfkC8=; _gcl_au=1.1.99104000.1607588309; UrCapture=f50d6ca8-cd23-0a3c-ab41-c4c9ff18b19a; _ga=GA1.2.471338417.1607588311; _gid=GA1.2.1208638310.1607588311; JabmoSP0ses.5e8a=*; JabmoSP0id.5e8a=35e2819b-2324-4946-b357-a24b7cd27fcb.1607588311.1.1607588548.1607588311.230dc772-2185-4735-adf6-0f3a6819fce6; _4c_=jVNdT9swFP0rKA97wq2%2FEtuV0FQFGBUdeygdj8ixnTYiJJGbNjAKv33XaaAak6blIbrn%2BNx7fT%2F8EnVrV0UTkmARSxljQiU%2BjR7c8yaavESmCf9d%2BG19GU2idds2m8l43HXdaFOsHrUurS%2FMemTqx7HRrS7r1bjxtd2adtwLxl5nhFLxtdTV6sxVX7xbFXV1dnUdnUaugthR4y3YprYOAFEjQkcEiPYXQC4xmEPE%2B%2Fa5CZrOZScb%2BwAH1u0K4%2B67wrbr3pniI7t2xWrdBhr3UWzjAwCrKypbd0c3zPGR%2FXBTnAOb1tuq9c%2BA06vZbSDKwlXt7BwYQN%2BWvWWdpVxQg4jONeKSJUhnsUVYJZIqnBiVZ%2Fub5Xy%2BH1rNpYiZgAC389tFHwKTyyQFNZummGA2xSwVaioxI6lKLy7ZQbv8X%2B1395g5%2F37NRaHLeW106c5dHmo5VJVC09%2Burvd3LpvDfN4Q2YM4mFu9Cq1GoV1Ln%2Bqm3fpA5DG2idESGUsZwpoZKJQTZDiUmBOZEaXB5af2ha5CF2c3i4vFYvbjBtjM193GhSlcFt7l9dOJpEDXsGzRXd%2F9DUA4cd73MkCbog15P2%2FbcAKb%2BukwrEvYqhiMsq8YAOw46J1pYfUAShUmN70%2FNJMLwpjkRIyG2TBCjg20oFhWD1XdVdHrafR0eCyJ4FIpxhTsaQsvQyYchw8UvrDDq4E8guaJlIjRhCNOqECSJwYleZxoYWSuVMjUxyQxSzjDSmABQXbFewyhuDUxj5EgiiFuuEPK5DFSNKNY4NhwqqOPe8UKUyUYwcO9iHy%2FVlMOEcmxCEwFJOTsXcw%2Fimh2gzr%2Bs2TBEvx3yYe5UiQVctU%2FXMln19fX3w%3D%3D; _vwo_uuid_v2=DEC9695FDE9BF51BD7DF2F8C79E25B29A|7688053e5449a74a7245d27a451d9bf5; fpestid=LDlFXa4PBcLRbOVyD784Xr3sR99_cZiIVyyP33YTFLbqMincXREmnfe1IG8wMd1UznuQKg; country=CHIT; SialLocaleDef=CountryCode~HK|WebLang~-1|; _fbp=fb.1.1607588322975.1756345820; _vwo_ssm=1; _vis_opt_s=1%7C; _vis_opt_test_cookie=1; _vwo_uuid=D688DAA22B40E3867F7C5266B22EA04CA; _vwo_ds=3%3Aa_1%2Ct_1%3A0%241607588322%3A34.24796932%3A%3A45_1%2C44_1%2C39_1%2C38_1%2C34_1%2C21_1%2C23_1%2C22_1%3A68_1%2C2_1%3A0; _vwo_sn=0%3A4%3Ar3.visualwebsiteoptimizer.com%3A4%3A1; _wingify_pc_uuid=6bfc5104f7c2426c9361d45f82d7de6d; JSESSIONID=04A0272E891E4A05F437397ABD824B79.stltcat11c; _vis_opt_exp_281_combi=2; _vis_opt_exp_285_combi=3; _uetsid=48dc87903ac011eb8f19910b54d97b51; _uetvid=48dcce703ac011ebb96ecfdbc7bf83ec; _urDomainCheck=sigmaaldrich.com",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
brand = "sigmaaldrich"

for i in range(1):
    url = "https://www.sigmaaldrich.com/life-science/cell-biology/antibodies/antibody-products.html?TablePage=111681178"
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        lxml = etree.HTML(resp.text)
        td = lxml.xpath('//tbody[@id="111681178PrdTblBdy"]')

        print(td)