import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "novus_kit_list"

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
    "Host": "www.novusbio.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    # 'Cookie': 'ak_bmsc=127D39600132966EF44E88EBC48AF8C4D2C0772F832A0000CA23D05F1594EE75~pl7p0p1AoR/jo1Yp7Z5tqxY+qvspcSZk9clxuJDuZcNvA5e0jCRnFaVAOeh7qGyMXjUNqZ1Geb5S66inJCmoCz5M+1flEn9VLDOUv/v4oahOY5SxC/8cPtEtx+sARgCWoPPbFTKa7b2Oynloiz5hoGOzHnyIPwiHsPhgRXaIyWsC+ijLjSxokQbUKWMT8puLjfPYPylp78COwQtptJ5xptAfHvd/U/bepBNcq0dw/vKWwle69I2bNK+emKLVzu1U8L; has_js=1; Hm_lvt_35811d88f61196befe2e1f070a5036df=1607476173; Hm_lpvt_35811d88f61196befe2e1f070a5036df=1607476974; bm_sv=DFAAFD1B93A7AE1F33B0BA7EA5EB119B~MDvJEZGSgfsRMJtenPzcm7P8k0eE0Br2IvATXZnsCGT5IrxerEHcj/7OiVnH2Y/NNGSFfkNpE8B8dg6SJOfZLIxBdLtYYy7RW2Sj/BQZCVrv/Pmc8JhIJo9hQovLYWBu3+8vq9sCgRCeEH0qz5MA614D6z0GJHb5Ho2Wk3NsDz4=; _gcl_au=1.1.183622685.1607476174; _Country=CN; _Country_Name=China; _Currency=CNY; Drupal.visitor.commerce_currency=CNY; _username=%7B%22uid%22%3A0%2C%22username%22%3A%22%22%2C%22et_ctry%22%3A%22CN%22%2C%22et_prevctry%22%3A%22%22%2C%22redirect_url%22%3A%22%22%2C%22novus_dist%22%3A%7B%22name%22%3A%22Bio-Techne+China+Co.+Ltd.+Bio-Techne%5Cu4e2d%5Cu56fd%5Cu5206%5Cu516c%5Cu53f8%22%2C%22address%22%3A%22%5Cu4e0a%5Cu6d77%5Cu5e02%5Cu957f%5Cu5b81%5Cu533a%5Cu957f%5Cu5b81%5Cu8def1193%5Cu53f7+++%5Cu957f%5Cu5b81%5Cu6765%5Cu798f%5Cu58eb%5Cu5e7f%5Cu573a3%5Cu53f7%5Cu529e%5Cu516c%5Cu697c19%5Cu5c4201%5Cu5355%5Cu5143%22%2C%22city%22%3A%22%22%2C%22state%22%3A%22%22%2C%22zip%22%3A%22%22%2C%22country%22%3A%22China%22%2C%22phone%22%3A%22800-988-1270%22%2C%22sphone%22%3A%22+800-988-1270+400-821-3475+%22%2C%22fax%22%3A%22021-52371001%22%2C%22nomen%22%3A%22%5Cu4e2d%5Cu56fd%22%2C%22email%22%3A%22%22%2C%22semail%22%3A%22++info.cn%40bio-techne.com+%22%2C%22icp_china%22%3A%22%5Cu6caaICP%5Cu590707019069%5Cu53f7-3%22%2C%22icp_china_link%22%3A%22https%3A%5C%2F%5C%2Fbeian.miit.gov.cn%5C%2F%22%7D%7D; _topbarcache=true; wcs_bt=79d33cacf3e18:1607476974; _ga=GA1.2.69797052.1607476175; _gid=GA1.2.331650985.1607476175; __adroll_fpc=73ffd4c5caadb885cbf39434cf2d2216-1607476175534; __ar_v4=NTFYL76H7VHQHAGEYH5QWR%3A20210008%3A2%7CSQYOJFD4XFBIBHATO72LAA%3A20210008%3A6%7CFKPGFD4LPJDPNPU5F7VENF%3A20210008%3A6%7CWZDZ22BJYRAGLPDHKWXWBG%3A20210008%3A4; _mkto_trk=id:584-XSF-168&token:_mch-novusbio.com-1607476175984-93669; covid-19=read; cto_bundle=j765rl9kRXpoeVoyWDJuMzY4NHJIR0UxYjVYJTJGVmJDQVJiVFU0ZjlxdGZOcUc4VEdReG1OME1zbyUyQlFoWXdsNlVCdW05MmFMRUlaZkRkc3VjWHpBOGpUcVdQJTJCNEQ5SW50dkVWdGd6T2JrVnNkSXJPR0w3WVpoSmE4TEFiTVVyc2lQaXVYRnRjeGxOUkcwaVVWYUhUMlJUY3R0YWclM0QlM0Q; _uetsid=32b88d5039bb11eba1e01b7c318fd6df; _uetvid=32b8bdc039bb11eb99546d24b72e53f9; _gat_UA-2844751-1=1; cookiepolicy=accepted',
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
brand = "novus"
for i in range(1, 446):
    url = f"https://www.novusbio.com/product-type/elisa-kits?page={i}"
    print(url)
    with requests.Session() as s:
        resp = s.get(url=url, headers=headers)
        # print(resp.status_code)
        lxml = etree.HTML(resp.text)
        a = lxml.xpath('//a[@class="ecommerce_link"]')
        # results = []
        for item in a:
            catano = item.xpath("./@data-id")[0].strip()
            name = item.xpath("./@data-name")[0].strip()
            link = "https://www.novusbio.com" + item.xpath("./@href")[0].strip()
            # print(catano, name, link)
            new_data = Data(
                Brand=brand, Catalog_Number=catano, Product_Name=name, Detail_url=link
            )
            # results.append(new_data)
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
