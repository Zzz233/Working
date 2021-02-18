from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index, JSON
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
import requests
import redis
import time
import random
from contextlib import contextmanager
from lxml import etree

Base = declarative_base()


class Data(Base):
    __tablename__ = "journal_history"

    Id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    year = Column(Integer, nullable=True, comment="")
    ISSN = Column(String(40), nullable=True, comment="")
    IPinfo = Column(JSON, nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://biopick:bp@2019@123.56.59.48:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=14)
r_redis = redis.Redis(connection_pool=pool)

class Fenqubiao:
    def get_task(self):
        # pmid, pmcid, issn, journal_name, article_type
        while r_redis.exists('fenqubiao'):
            task = r_redis.rpop('fenqubiao')
            # r_redis.rpush('fenqubiao', task)
            yield task

    def get_content(self, issn, year): #  , proxies
        resp = requests.get(url=f'https://webapi.fenqubiao.com/api/journal?year={year}&abbr={issn}', timeout=45) # , proxies=proxies
        if resp.status_code == 500:
            print('Message	"出现错误。"')
            return 0
        else:
            ip_info = resp.json()
            new_data = Data(year=year, ISSN=issn, IPinfo=ip_info)
            print('有数据')
            return new_data

    @contextmanager
    def session_maker(self, session=session):
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def insert(self, objs):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(objs)
            print('done')

    def get_proxy(self):
        proxy_url = 'http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text'
        content = requests.get(url=proxy_url).text.strip()
        proxies = {
            "http": "http://" + content,
            "https": "http://" + content,
        }
        print("获取新代理", content)
        return proxies

    def error_push(self, item):
        r_redis.lpush('fenqubiao', item)

    def run(self):
        # proxies = self.get_proxy()
        for task in self.get_task():
            print(task)
            results_objs = []
            try:
                for year in range(2005, 2019+1):
                    print(task, year)
                    single_obj = self.get_content(task, year) # , proxies
                    if single_obj != 0:
                        results_objs.append(single_obj)
                        print('+1')
                    time.sleep(random.uniform(1.0, 1.5))
                self.insert(results_objs)
            except Exception:
                self.error_push(task)
                # proxies = self.get_proxy()
                break
            time.sleep(random.uniform(1.0, 1.5))


if __name__ == '__main__':
    fenqubiao = Fenqubiao()
    fenqubiao.run()
