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
    __tablename__ = "bio_article_new"

    Id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    teamid = Column(Integer, nullable=True, comment="")
    teamname = Column(String(100), nullable=True, comment="")
    # appmsgid = Column(Integer, nullable=True, comment="")
    digest = Column(String(200), nullable=True, comment="")
    Author = Column(String(100), nullable=True, comment="")
    Title = Column(String(400), nullable=True, comment="")
    Link = Column(String(200), nullable=True, comment="")
    Cover_Img = Column(String(100), nullable=True, comment="")
    create_time = Column(DateTime, nullable=True, comment="")
    update_time = Column(DateTime, nullable=True, comment="")
    pmId = Column(Integer, nullable=True, comment="")
    ToHomePage = Column(Integer, nullable=True, comment="")
    status = Column(Integer, nullable=True, comment="")
    doi = Column(String(50), nullable=True, comment="")
    add_time = Column(DateTime, nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.2.4:3306/biopick_oa?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)

company = '药用植物研究前沿 '


class Wechat:
    def __init__(self):
        self.headers = {
            'Host': 'mp.weixin.qq.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&createType=0&token=1742984158&lang=zh_CN',
            'Cookie': 'ua_id=TpTILW7V3SXRvruFAAAAAPicLxg9Zl2QBANyvFl1PEw=; xid=ec5a1d553992e78787d4a6f19246f288; mm_lang=zh_CN; pvid=5851655270; pgv_pvid=4548717900; wxuin=15766856153630; openid2ticket_oT54j1UgMCNoCKsdNH1O1udUtocs=; RK=ybY5sB+ecL; ptcz=db395e2df9879fba2e3d2027480e44142f9f19369293ea49aa6e8062645ca88b; openid2ticket_oEC_b6rW3VqJhi4cpHdQsNH9dNSY=; tvfe_boss_uuid=e91067a2d951c61e; pac_uid=0_68c016a23a466; rand_info=CAESILCHHIGh2IJ1qL3cdDdB++kUbB8tykYaFI8e65ppfVTW; slave_bizuin=3882566298; data_bizuin=3882566298; bizuin=3882566298; data_ticket=4sS+sci4N3jNHMkSwHOMj9DH2WTq7xqSJkkTo/s638VEbj3nwr4f8LzG0wnwI6HV; slave_sid=Z18xcGVqT0lyZWVPeFVPQTJXME56UWNlTExPR2RuUkViZ01LbTlsNGJ5aThnV092RGhZaGdsUnlGRV9fSWlQamZTNGJjdU50T0VXR2VLa05yM2kwdWdpS2dEWDFIVkZrX3ZrMWo1amJ4eFJnOHJoWFlJUHl0MUd4QnZqaFpOT05WVUlGZU1QeFpLU2JVVU80; slave_user=gh_c1f740f2ddf6',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'TE': 'trailers',
        }

    def get_url(self):
        for i in range(975, 1060 + 5, 5):
            url = f'https://mp.weixin.qq.com/cgi-bin/appmsg?action=list_ex&begin={i}&count=5&fakeid=MzU1ODQ5ODMzMw==&type=9&query=&token=1742984158&lang=zh_CN&f=json&ajax=1'
            yield url

    def get_list_page(self, url):  # , proxy
        with requests.session() as s:
            json_data = s.get(url=url, headers=self.headers, timeout=30).json()
        return json_data

    def parse_json(self, json_data):
        if json_data['base_resp']['err_msg'] == 'ok':
            objects = []
            results = json_data['app_msg_list']
            for item in results:
                digest = item['digest'].strip()[0:300]
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
                new_data = Data(teamname=company,
                                teamid=568,
                                # appmsgid=appmsgid,
                                digest=digest,
                                Title=title,
                                Link=link,
                                Cover_Img=cover_img,
                                ToHomePage=0,
                                create_time=create_time,
                                update_time=update_time,
                                status=-1)
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
