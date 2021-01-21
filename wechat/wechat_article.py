from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import requests
import redis
import time
import random
from lxml import etree
from html.parser import HTMLParser

company = 'BioArt'

Base = declarative_base()


class Data(Base):
    __tablename__ = "article_analytic"

    Id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Company = Column(String(100), nullable=True, comment="")
    # appmsgid = Column(Integer, nullable=True, comment="")
    digest = Column(String(200), nullable=True, comment="")
    Author = Column(String(100), nullable=True, comment="")
    Title = Column(String(400), nullable=True, comment="")
    Link = Column(String(200), nullable=True, comment="")
    Cover_Img = Column(String(100), nullable=True, comment="")
    create_time = Column(DateTime, nullable=True, comment="")
    update_time = Column(DateTime, nullable=True, comment="")
    pmId = Column(Integer, nullable=True, comment="")
    doi = Column(String(50), nullable=True, comment="")
    content = Column(Text, nullable=True, comment="")
    add_time = Column(DateTime, nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)

headers = {
            # 'Host': 'mp.weixin.qq.com',
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
            # 'Accept': '*/*',
            # 'Accept-Language':
            # 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'X-Requested-With': 'XMLHttpRequest',
            # 'Connection': 'keep-alive',
            # 'Referer':
            # 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=10&createType=0&token=108424269&lang=zh_CN',
            # 'Cookie':
            # 'ua_id=NqRMQzT37VpJKMncAAAAAJtRaYelsoLj6YyXUadzuQ0=; uuid=6c34f914d18ece28bd47a5d9d24dc291; cert=ZZL_aMSKHPWeiI9w7H_Y1HN471XZTat3; rand_info=CAESIJ8epP/XFSJZRbd9rxbAc/s3CUYXe0Hy+22ethkIc7iw; slave_bizuin=3882566298; data_bizuin=3882566298; bizuin=3882566298; data_ticket=FP0+te4Y9WY8gw0nlL+PPiESOM+tJKdZS9C0RDFuFZUJMwsQfSbTcjLg2d/IBO/E; slave_sid=RmRjX2xQelg1SndaZjByUGp2a2V2aGtCdExSczJNYTFjRnNzTm1sT2hmYVBrTUlIM1p0NmxTWnFPRndWQ0prVUFrcU4yRDNEWWdQTFJXQXBoZTI4NURlWDBjcEYzS0FnWmVONm9MSHk0YWVvNlNoSVFwcmlrdkVWSWEyS2doNXJ1S2pRbkpTQ1pEN1ZvQXh3; slave_user=gh_c1f740f2ddf6; xid=60afca852ed1a09b2ee3d9275b6ce515; openid2ticket_omkJr54_8EhMTR-UYnHn2sKcPAWk=; mm_lang=zh_CN; rewardsn=; wxtokenkey=777',
            # 'Pragma': 'no-cache',
            # 'Cache-Control': 'no-cache',
        }
while r.exists('wechat_url'):
    url = r.rpop('wechat_url')
    print(url)
    with requests.session() as s:
        resp = s.get(url = url)
        html = etree.HTML(resp.text)
        contents = html.xpath('//div[@class="rich_media_content "][@id="js_content"]')[0]
        name1 = etree.tostring(contents, method='html')
        name2 = HTMLParser().unescape(name1.decode()).split(r'<div class="rich_media_content " id="js_content" style="visibility: hidden;">')[-1].strip().rstrip('</div>').strip()
        # print(name2)
        data_info = session.query(Data).filter(Data.Link==url).first()
        print(data_info)
        data_info.content = name2
        session.commit()
        session.close()
        break
