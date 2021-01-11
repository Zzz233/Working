from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index, JSON
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import requests

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


class Fen:
    def __init__(self):
        self.base_url = "https://webapi.fenqubiao.com/api/journal?year=2020&abbr="

    def get_url(self):
        while r.exists("search_key"):
            url = self.base_url + r.lpop("search_key")
            print(url)
            yield url

    def get_content(self, url):
        with requests.Session() as s:
            content = s.get(url=url).json()
            issn = content["ISSN"]
            abbrTitle = content["AbbrTitle"]
            new_journal = Journal(ISSN=issn, Abbr=abbrTitle, IPinfo=content)
            return new_journal

    def insert(self, journal_obj):
        session.add(journal_obj)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            session.rollback()
            print(e)

    def run(self):
        for url in self.get_url():
            new_journal = self.get_content(url)
            self.insert(new_journal)


if __name__ == "__main__":
    fen = Fen()
    fen.run()
