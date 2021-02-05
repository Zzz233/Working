from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import requests
import redis
import time
import random
from contextlib import contextmanager


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
    add_time = Column(DateTime, nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://biopick:bp@2019@123.56.59.48:3306/qdm765045126_db?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)


company = '阜呐核酸情报站'


class Wechat:
    def __init__(self):
        self.headers = {
            'Host': 'mp.weixin.qq.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=10&createType=0&token=1381148804&lang=zh_CN',
            'Cookie': 'ua_id=E5gv7Z2f95VXOrhmAAAAACAkPgNGjwPCozTGj1NPZfg=; uuid=1e08d7a62e5ae03b0e7b42264515b663; xid=2acf4b893908e32e5fef5fbb25b329d8; openid2ticket_omkJr54_8EhMTR-UYnHn2sKcPAWk=; mm_lang=zh_CN; rand_info=CAESIMMuWOakgg6EVR/BXCzz1L/HZLV7E0t96aKQy7zeym+I; slave_bizuin=3276978245; data_bizuin=3276978245; bizuin=3276978245; data_ticket=T3twG/uDFHLAj3y7xvteZfYPYxYc+JJwm34N2KnO3zUyO0ElDYBl1dezsusapsV3; slave_sid=X0hMUlRsQTBnREZoWTRmcVVjVXJCcWduSDVlbEZNN0JjcG9fS3NvblJ1azdNb0ozV0tZbjJBUE9ZYnloWTZqMVlLVDJBMWxTV1dhVzhfV21UQndwZ1lfa0s0SzFiQXdSaGJ6Qlp1WkxvZ2NfY29DZlpjMWJFbjVLbnA5dm9tenM1R0VBdFB0N0lzTkxwMzJK; slave_user=gh_2dcc445c6278; openid2ticket_oRbhSwxvnpgBizeeWXAgSS58f2fk=',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'TE': 'Trailers',
        }
        # self.data = {
        #     "action": "list_ex",
        #     "begin": number,
        #     "count": "5",
        #     "fakeid": yourfakeid,
        #     "type": "9",
        #     "token": yourtoken,
        #     "lang": "zh_CN",
        #     "f": "json",
        #     "ajax": "1",
        #     "query": "",
        # }

    def get_url(self):
        for i in range(135, 330, 5):
            url = f'https://mp.weixin.qq.com/cgi-bin/appmsg?action=list_ex&begin={i}&count=5&fakeid=Mzg4MzEzMzczMA==&type=9&query=&token=1381148804&lang=zh_CN&f=json&ajax=1'
            yield url

    def get_list_page(self, url):  #  , proxy
        with requests.session() as s:
            json_data = s.get(url=url, headers=self.headers, timeout=30).json()
        return json_data

    def parse_json(self, json_data):
        if json_data['base_resp']['err_msg'] == 'ok':
            objects = []
            results = json_data['app_msg_list']
            for item in results:
                # id
                # appmsgid = item['appmsgid']
                digest = item['digest'].strip()
                # author
                title = item['title'].strip()
                link = item['link'].strip()
                cover_img = item['cover'].strip()
                create_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', (time.localtime(item['create_time'])))
                update_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', (time.localtime(item['update_time'])))
                # pmId
                # doi
                # add_time
                new_data = Data(Company=company,
                                # appmsgid=appmsgid,
                                digest=digest,
                                Title=title,
                                Link=link,
                                Cover_Img=cover_img,
                                create_time=create_time,
                                update_time=update_time)
                objects.append(new_data)
            return objects
        else:
            print(json_data['base_resp']['err_msg'])
            return 'not ok'

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

    def insert(self, results):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(results)
            db_session.commit()
            flag = 1
        return flag

    def error_push(self, item):
        r.rpush('error_task', item)

    def get_proxy(self):
        proxy_url = 'http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text'
        content = requests.get(url=proxy_url).text.strip()
        proxies = {
            "http": "http://" + content,
            "https": "http://" + content,
        }
        print("获取新代理", content)
        return proxies

    def run(self):
        # proxy = self.get_proxy()
        for url in self.get_url():
            print(url)
            # print(proxy)
            json_obj = self.get_list_page(url)
            data_objs = self.parse_json(json_obj)
            if data_objs == 'not ok':
                self.error_push(url)
                break
            flag = self.insert(data_objs)
            if flag == 1:
                print('done')
            elif flag != 2:
                self.error_push(url)
                # proxy = self.get_proxy()
                break
            time.sleep(random.uniform(30, 60))


if __name__ == "__main__":
    task = Wechat()
    task.run()
