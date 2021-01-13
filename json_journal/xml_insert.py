from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index, JSON
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import requests
import time
import random

Base = declarative_base()


class Journal(Base):
    __tablename__ = "journal_ip"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    ISSN = Column(String(40), nullable=True, comment="")
    Abbr = Column(String(200), nullable=True, comment="")
    IPinfo = Column(JSON, nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=1)
r = redis.Redis(connection_pool=pool)


headers = {
    "Host": "webapi.fenqubiao.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

for line in open("D:/jourcache.txt", encoding="utf-8"):
    try:
        line_issn = []
        if '<Issn type="print">' in line:
            print_issn = line.split('<Issn type="print">')[-1].split("</Issn>")[0]
            line_issn.append(print_issn)
        if '<Issn type="electronic">' in line:
            print_electronic = line.split('<Issn type="electronic">')[-1].split(
                "</Issn>"
            )[0]
            line_issn.append(print_electronic)
        for item in line_issn:
            with requests.Session() as s:
                resp = s.get(
                    url="https://webapi.fenqubiao.com/api/journal?year=2020&abbr="
                    + item,
                    timeout=30,
                    headers=headers,
                )
                print(item)
                if resp.status_code == 200:
                    content = resp.json()
                    issn = content["ISSN"]
                    abbrTitle = content["AbbrTitle"]
                    new_journal = Journal(ISSN=issn, Abbr=abbrTitle, IPinfo=content)
                    session.add(new_journal)
                    session.commit()
                    session.close()
                    print(item, "done")
                    break
        time.sleep(random.uniform(2.0, 4.5))
    except Exception as e:
        print(e)
        print("sleeping...")
        time.sleep(30)
