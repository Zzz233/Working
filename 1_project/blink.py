"""
1261开始 增加 page字段
12155 取消session

"""
import requests
from lxml import etree
import datetime
import time
from queue import Queue
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
from sqlalchemy.sql.functions import random
from sqlalchemy.sql.sqltypes import DATE, DECIMAL
import random

Base = declarative_base()


class Data(Base):
    __tablename__ = "projectgrants_1986"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    ProjectCode = Column(String(20), nullable=True, comment="项目批准号")
    ProjectName = Column(String(200), nullable=True, comment="")
    ProjectType = Column(String(50), nullable=True, comment="")
    Institution = Column(String(100), nullable=False, comment="")
    Leader = Column(String(50), nullable=True, comment="")
    Amount = Column(DECIMAL(7), nullable=True, comment="")
    YearOfApproval = Column(DATE, nullable=True, comment="")
    ClassificationCode = Column(String(10), nullable=True, comment="")
    Classification = Column(String(100), nullable=True, comment="")
    StartDate = Column(DATE, nullable=False, comment="")
    EndDate = Column(DATE, nullable=False, comment="")
    KeyWord = Column(String(500), nullable=False, comment="")
    EnKeyWord = Column(String(500), nullable=False, comment="")
    Url = Column(String(500), nullable=True, comment="")
    Status = Column(Integer, nullable=True, comment="")
    curPage = Column(Integer, nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=5)
r = redis.Redis(connection_pool=pool)


def crawler(year, page):
    referer_page = int(page) - 1
    UA = {
        "Host": "fund.keyanzhiku.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": f"http://fund.keyanzhiku.com/Index/index/start_year/{year}/end_year/{year}/xmid/0/search/1/px_year/desc/p/{referer_page}.html",
        "Cookie": "PHPSESSID=djcet75ohhmtadbgq3lp20r2c3",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    url = f"http://fund.keyanzhiku.com/Index/index/start_year/{year}/end_year/{year}/xmid/0/search/1/px_year/desc/p/{page}.html"
    results = []
    resp = requests.get(url=url, headers=UA)
    lxml = etree.HTML(resp.text)
    li_tags = lxml.xpath(
        '//ul[@class="layuiadmin-card-status layuiadmin-home2-usernote"]/a/li'
    )
    for li in li_tags:
        # 详情链接
        detail_link = "http://fund.keyanzhiku.com/" + li.xpath("./../@href")[0].strip()
        # 标题
        title = li.xpath("./h3/text()")[0].strip()
        # 负责人
        try:
            leader = (
                li.xpath('./span[contains(text(), "负责人：")]/text()')[0]
                .split("负责人：")[-1]
                .strip()
            )
        except Exception:
            leader = "0"
        # 申请单位
        try:
            organization = (
                li.xpath('./span[contains(text(), "申请单位：")]/text()')[0]
                .split("申请单位：")[-1]
                .strip()
            )
        except Exception:
            organization = "0"
        # 研究类型
        try:
            research_type = (
                li.xpath('./span[contains(text(), "研究类型：")]/text()')[0]
                .split("研究类型：")[-1]
                .strip()
            )
        except Exception:
            research_type = "0"
        # 项目批准号
        try:
            approve_num = (
                li.xpath('./span[contains(text(), "项目批准号：")]/text()')[0]
                .split("项目批准号：")[-1]
                .strip()
            )
        except Exception:
            approve_num = "0"
        # 批准年度：
        try:
            year_text = (
                li.xpath('./span[contains(text(), "批准年度：")]/text()')[0]
                .split("批准年度：")[-1]
                .strip()
            )
            year = datetime.datetime.strptime(year_text, "%Y").date()
        except Exception:
            year_error = "1949"
            year = datetime.datetime.strptime(year_error, "%Y").date()
        # 金额
        try:
            cash = (
                li.xpath('./span[contains(text(), "金额：")]/text()')[0]
                .split("金额：")[-1]
                .strip()
                .replace("万", "")
            )
        except Exception:
            cash = "0"
        # 关键字
        try:
            key_word = (
                li.xpath('./span[contains(text(), "关键词：")]/text()')[0]
                .split("关键词：")[-1]
                .strip()
            )
        except Exception:
            key_word = "0"
        results.append(
            [
                detail_link,
                title,
                leader,
                organization,
                research_type,
                approve_num,
                year,
                cash,
                key_word,
            ]
        )
    return results


if __name__ == "__main__":
    # for page_no in range(1):
    while r.exists("keyanzhiku_pagenum"):
        extract = r.lpop("keyanzhiku_pagenum")
        page_no = extract.split(",")[1]
        year_redis = extract.split(",")[0]
        print(extract)
        final_data = []
        for item in crawler(year_redis, page_no):
            l_url = item[0]
            l_title = item[1]
            l_leader = item[2]
            l_organization = item[3]
            l_research_type = item[4]
            l_approve_num = item[5]
            l_year = item[6]
            l_cash = item[7]
            l_key_word = item[8]
            new_data = Data(
                ProjectCode=l_approve_num,
                ProjectName=l_title,
                ProjectType=l_research_type,
                Institution=l_organization,
                Leader=l_leader,
                Amount=l_cash,
                YearOfApproval=l_year,
                KeyWord=l_key_word,
                Url=l_url,
                ClassificationCode="0",
                Classification="0",
                Status="0",
                curPage=int(page_no),
            )
            final_data.append(new_data)
        session.bulk_save_objects(final_data)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            session.rollback()
            r.rpush("keyanzhiku_pagenum", page_no)
            print(e)
        time.sleep(random.uniform(1.5, 1.5))
