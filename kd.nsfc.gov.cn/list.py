import requests
import redis
import math
import time
import random
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Index,
    DECIMAL,
    Date,
    SmallInteger,
)
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Detail(Base):
    __tablename__ = "projectgrants_fromharvest"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    ProjectCode = Column(String(20), nullable=True, comment="")
    ProjectName = Column(String(300), nullable=True, comment="")
    ProjectType = Column(String(50), nullable=True, comment="")
    appliedCode = Column(String(20), nullable=True, comment="")
    Institution = Column(String(100), nullable=True, comment="")
    Leader = Column(String(50), nullable=True, comment="")
    Amount = Column(DECIMAL(7), nullable=True, comment="")
    YearOfApproval = Column(Integer, nullable=True, comment="")
    Classification = Column(String(100), nullable=True, comment="")
    StartDate = Column(Date, nullable=True, comment="")
    EndDate = Column(Date, nullable=True, comment="")
    KeyWord = Column(String(500), nullable=True, comment="")
    EnKeyWord = Column(String(500), nullable=True, comment="")
    Url = Column(String(200), nullable=True, comment="")
    Status = Column(Integer, nullable=True, comment="")
    Summary = Column(String(4000), nullable=True, comment="")
    CnSummary = Column(String(4000), nullable=True, comment="")
    Conclusion = Column(String(4000), nullable=True, comment="")
    publishQty = Column(SmallInteger, nullable=True, comment="")
    hyqty = Column(SmallInteger, nullable=True, comment="")
    zzqty = Column(SmallInteger, nullable=True, comment="")
    jlqty = Column(SmallInteger, nullable=True, comment="")
    zlqty = Column(SmallInteger, nullable=True, comment="")


pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=1)
r = redis.Redis(connection_pool=pool)
# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/biology_grant?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Nsfc:
    def __init__(self):
        self.base_url = "http://kd.nsfc.gov.cn/baseQuery/data/supportQueryResultsData"
        self.headers = {
            "Host": "kd.nsfc.gov.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Length": "336",
            "Origin": "http://kd.nsfc.gov.cn",
            "Connection": "keep-alive",
            "Referer": "http://kd.nsfc.gov.cn/baseQuery/conclusionQuery",
            # "Cookie": "JSESSIONID=2665EBE24CDA3B8E637A5D252299914E",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    def get_request_data(self):  # 从redis获取payload参数
        while r.exists("search_data"):
            extract = r.lpop("search_data")
            yield extract

    def crawler(self, redis_data, proxies):  # int 返回该请求参数的总结果数量 & 保存第一页的数据
        code = redis_data.split(",")[0]
        projectType = redis_data.split(",")[2]
        conclusionYear = redis_data.split(",")[3]
        payload = {
            "ratifyNo": "",
            "projectName": "",
            "personInCharge": "",
            "dependUnit": "",
            # todo 申请代码
            "code": code,
            # todo 资助类别
            "projectType": projectType,
            "subPType": "",
            "psPType": "",
            "keywords": "",
            "ratifyYear": "",
            # todo 结题年度
            "conclusionYear": conclusionYear,
            "beginYear": "",
            "endYear": "",
            "checkDep": "",
            "checkType": "",
            "quickQueryInput": "",
            "adminID": "",
            "complete": "true",
            # ! 页数
            "pageNum": 0,
            # ! 分页内数量
            "pageSize": 10,
            "queryType": "input",
        }
        with requests.Session() as s:
            json_data = s.post(
                url=self.base_url, json=payload, headers=self.headers, proxies=proxies
            ).json()
            flag_code = json_data["code"]
            totalRecords = json_data["data"]["iTotalRecords"]
            results = json_data["data"]["resultsData"]

        return flag_code, totalRecords, results  # int

    def to_new_redis(self, old_redis_str, total):
        if total > 10:
            page_size = 10
            pages = math.ceil(total / page_size)
            for i in range(1, pages + 1):
                item = old_redis_str + "," + str(i)
                r.rpush("search_with_pNum", item)
                print(item)
        else:
            pass

    def parse(self, results):
        if len(results) > 0:
            objects = []
            for item in results:
                url = item[0]
                projectName = item[1]
                projectCode = item[2]
                projectType = item[3]
                institution = item[4]
                leader = item[5]
                amount = item[6]
                yearOfApproval = int(item[7])
                keyWord = item[8]
                # asd = item[9]
                result_todo = item[10].split(";")
                publishQty = result_todo[0]
                hyqty = result_todo[1]
                zzqty = result_todo[2]
                jlqty = result_todo[3]
                zlqty = result_todo[4]
                # asd = item[11]
                # asd = item[12]
                # asd = item[13]
                appliedCode = item[14]
                # asd = item[15]
                # asd = item[16]
                new_detail = Detail(
                    ProjectCode=projectCode,
                    ProjectName=projectName,
                    ProjectType=projectType,
                    appliedCode=appliedCode,
                    Institution=institution,
                    Leader=leader,
                    Amount=amount,
                    YearOfApproval=yearOfApproval,
                    KeyWord=keyWord,
                    Url=url,
                    publishQty=publishQty,
                    hyqty=hyqty,
                    zzqty=zzqty,
                    jlqty=jlqty,
                    zlqty=zlqty,
                )
                objects.append(new_detail)
            return objects

    def insert(self, obj):
        session.bulk_save_objects(obj)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            session.rollback()
            print(e)

    def get_proxy(self):
        proxy_url = "http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text"
        content = requests.get(url=proxy_url).text.strip()
        proxies = {
            "http": "http://" + content,
            "https": "http://" + content,
        }
        print("获取新代理", content)
        return proxies

    def back_to_redis(self, item):
        r.rpush("search_data", item)
        print("push back")

    def run(self):
        proxies = self.get_proxy()
        for item in self.get_request_data():
            print(item)
            try:
                flag_code, total, results = self.crawler(item, proxies)
            except Exception as e:
                proxies = self.get_proxy()
                self.back_to_redis(item)
                print(e)
                continue
            if flag_code == 200:
                self.to_new_redis(item, total)
                objects = self.parse(results)
                if objects:
                    self.insert(objects)
            else:
                proxies = self.get_proxy()
                self.back_to_redis(item)
                print("Error", item)
            time.sleep(random.uniform(5.5, 7.0))


if __name__ == "__main__":
    one = Nsfc()
    one.run()
