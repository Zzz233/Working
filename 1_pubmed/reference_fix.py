import redis
import time
from contextlib import contextmanager
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=0)
r = redis.Redis(connection_pool=pool)


Base = declarative_base()


class Reference(Base):
    __tablename__ = "article_reference"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    cited_pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Temp(Base):
    __tablename__ = "temp_copy1"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmid = Column(Integer, nullable=True, comment="")
    xmlname = Column(String(40), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")

class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    Journal_title = Column(String(250), nullable=True, comment="")
    Journal_issn = Column(String(40), nullable=True, comment="")  # todo 原 Journal_ISSN_link
    Journal_vol = Column(String(40), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbr = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pub_date = Column(String(20), nullable=True, comment="")  # todo 原来是String现在是Date
    Article_doi = Column(String(100), nullable=True, comment="")
    Article_pmc = Column(String(100), nullable=True, comment="")
    Article_abstract = Column(Text, nullable=True, comment="")
    Article_keyword = Column(String(1000), nullable=True, comment="")
    Article_type = Column(String(300), nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    Article_language = Column(String(10), nullable=True, comment="")
    Authors = Column(String(1000), nullable=True, comment="")
    Institutions = Column(String(2000), nullable=True, comment="")
    Journal_if = Column(String(20), nullable=True, comment="")
    Cited_qty = Column(Integer, nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")



class Pubmed:
    def __init__(self):
        self.base_path = 'D:/'

    def get_path(self):
        while r.exists('xml_task'):
            path = self.base_path + r.lpop("xml_task")
            yield path

    def get_content(self, path):
        xml = etree.parse(path)
        articles = xml.xpath("//PubmedArticle")
        for item in articles:
            yield item

    def parse_item(self, item, path):
        # todo version 字段
        version = int(
            item.xpath('./MedlineCitation/PMID[@Version]/@Version')[0].strip())
        # todo pmid 字段
        try:
            main_pmid = int(
                item.xpath('.//ArticleId[@IdType="pubmed"]/text()')[0].strip())
        except Exception:
            main_pmid = None

        # todo article_reference 表
        reference_list = item.xpath(".//ReferenceList/Reference")
        reference_results_list = []
        for reference in reference_list:
            try:
                citation_pmid = reference.xpath(
                    './ArticleIdList/ArticleId[@IdType="pubmed"]/text()'
                )[0].strip()
                if isinstance(citation_pmid, str):
                    new_reference = Reference(
                        pmid=main_pmid,
                        cited_pmid=int(citation_pmid),
                        Version=version
                    )
                    reference_results_list.append(new_reference)  # todo 结果
            except Exception:
                pass

        new_temp = Temp(
            pmid=main_pmid,
            xmlname=path,
            Version=version
        )
        return reference_results_list, new_temp

    def pre_removal(self, sample_list):
        smaple_dict = {}
        sample_results = []
        for item_1 in sample_list:
            t_key = str(item_1.pmid)
            t_value = str(item_1.Version)
            if t_key not in smaple_dict:
                smaple_dict[t_key] = t_value
            elif t_key in smaple_dict:
                if smaple_dict[t_key] < t_value:
                    smaple_dict[t_key] = t_value
        for item_2 in sample_list:
            tt_key = str(item_2.pmid)
            tt_value = str(item_2.Version)
            if tt_key in smaple_dict.keys() and tt_value == smaple_dict[tt_key]:
                del item_2.Version
                sample_results.append(item_2)
        return sample_results

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

    def insert_to_temp(self, temp_objs):
        duplicated_list = []
        with self.session_maker() as db_session:
            db_session.query(Temp).delete(synchronize_session='evaluate')
            db_session.bulk_save_objects(temp_objs)
            print('临时表已经写入' + '\n' + '==========')
            duplicated = session.query(Temp.pmid).filter(
                Temp.pmid == Detail.pmid).all()
            for i in duplicated:
                duplicated_list.append(i[0])
            if len(duplicated_list) == 0:
                print('莫得重复' + '\n' + '==========')
            elif len(duplicated_list) > 0:
                print('有重复' + '\n' + '==========')
            print(len(duplicated_list))
        return duplicated_list

    def delete_items(self, duplicated_list):
        with self.session_maker() as db_session:
            for item in duplicated_list:
                db_session.query(Reference).filter(Reference.pmid == item).delete()
        print('删除完成' + '\n' + '==========')

    def insert(self, reference):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(reference)
            print('全部写入' + '\n' + '==========')

    def run(self):
        for path in self.get_path():
            print(path)
            reference_data = []
            temp_data = []
            for item in self.get_content(path):
                reference_list, temp_obj = self.parse_item(item, path)
                if reference_list:
                    reference_data.extend(reference_list)
                if temp_obj:
                    temp_data.append(temp_obj)
            final_reference_data = self.pre_removal(reference_data)
            final_temp_data = self.pre_removal(temp_data)
            print('pre_removal')
            dup_show = self.insert_to_temp(final_temp_data)
            if len(dup_show) > 0:
                # todo 删除
                self.delete_items(dup_show)
                self.insert(final_reference_data)
            elif len(dup_show) == 0:
                self.insert(final_reference_data)
            r.lpush('done', path.replace('D:/', ''))
            print('文件结束' + '\n' + '==========')
            break


if __name__ == "__main__":
    pubmed = Pubmed()
    pubmed.run()
