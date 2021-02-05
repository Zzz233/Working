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
    __tablename__ = "article_weixin"

    Id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    weixin_name = Column(String(200), nullable=True, comment="")
    weixin_fakeId = Column(String(50), nullable=True, comment="")
    pageNum = Column(Integer, nullable=True, comment="")
    total = Column(Integer, nullable=True, comment="")
    researchArea = Column(String(200), nullable=True, comment="")
    des = Column(String(500), nullable=True, comment="")
    registrationDate = Column(DateTime, nullable=True, comment="")
    lastCrawDate = Column(DateTime, nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://biopick:bp@2019@123.56.59.48:3306/qdm765045126_db?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()


class Task:
    def get_lines(self):
        results_list = []
        for line in open('C:/Users/NING MEI/Desktop/公众号.txt', encoding='utf-8'):
            if ':' in line:
                item = line.split(':')
                name = item[0].strip()
                fake_id = item[1].strip()
                if fake_id == '':
                    fake_id = None
                new_data = Data(weixin_name=name,weixin_fakeId=fake_id)
                results_list.append(new_data)
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

    def insert(self, results):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(results)
            db_session.commit()
            flag = 1
        return flag

    def run(self):
        a = self.get_lines()
        self.insert(a)
        print('done')


if __name__ == '__main__':
    task = Task()
    task.run()
