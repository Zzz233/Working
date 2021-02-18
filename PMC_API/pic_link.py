from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
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
    __tablename__ = "article_image"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    thumbnail_img = Column(String(400), nullable=True, comment="")
    normal_img = Column(String(400), nullable=True, comment="")
    title = Column(String(500), nullable=True, comment="")
    des = Column(String(2500), nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=14)
r_redis = redis.Redis(connection_pool=pool)


class API:
    def __init__(self):
        self.base_url = 'https://pubmed.ncbi.nlm.nih.gov/'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
        }

    def get_task(self):
        # pmid, pmcid, issn, journal_name, article_type
        while r_redis.exists('pic_task'):
            task = r_redis.rpop('pic_task')
            # r_redis.rpush('pic_task', task)
            yield task

    def split_func(self, task):
        item = task.split(',')
        pmid = item[0]
        pmcid = item[1]
        issn = item[2]
        journal_name = item[3]
        article_type = item[4]
        return pmid, pmcid, issn, journal_name, article_type

    def get_content(self, pmid):
        with requests.session() as s:
            # resp = s.get(self.base_url + pmc).text
            resp = s.get(self.base_url + pmid, timeout=45, headers=self.headers).text
            xml = etree.HTML(resp)
            return xml

    def get_url_text(self, xml, pmid):
        if xml.xpath('//div[@class="figures" and @id="figures"]/div'
                     '[@class="figures-list"]/figure[@class="figure-item " or @class="figure-item tail"]'):
            results_list = []
            figures = xml.xpath('//div[@class="figures" and @id="figures"]/div'
                                '[@class="figures-list"]/figure[@class="figure-item " or @class="figure-item tail"]')
            for i, item in enumerate(figures):
                big_img = item.xpath('./a[@class="figure-link"]/@href')[0].strip()
                small_img = item.xpath('./a[@class="figure-link"]/img[@class="figure-thumb"]/@src')[0].strip()
                try:
                    desc_text = item.xpath('./figcaption[@itemprop="description"]/div'
                                           '[@class="figure-caption-contents"]//text()')
                    desc = ''.join(d.strip() for d in desc_text).lstrip('.')
                    if 0 < len(desc) <= 2500:
                        title = desc.split('. ')[0][0:500]
                        new_data = Data(pmid=pmid, ext_id=i, thumbnail_img=small_img,
                                        normal_img=big_img, title=title, des=desc)
                        if len(new_data.des) > 0:
                            results_list.append(new_data)
                except Exception as e:
                    print(e)
                    print('desc为空或>2500')
                    pass
            return results_list

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

    def push_back(self, task):
        r_redis.lpush("pic_task", task)
        print('放回去')

    def run(self):
        for task in self.get_task():
            print(task)
            pmid, pmcid, issn, journal_name, article_type = self.split_func(task)
            try:
                xml = self.get_content(pmcid)
            except Exception as e:
                print(e)
                self.push_back(task)
                time.sleep(30)
                continue
            try:
                if xml is not None:
                    obj_list = self.get_url_text(xml, pmid)
                    if obj_list:
                        self.insert(obj_list)
                else:
                    self.push_back(task)
                    time.sleep(3)
            except Exception:
                pass
            time.sleep(3)
            # break


if __name__ == '__main__':
    api = API()
    api.run()




