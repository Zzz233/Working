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
    __tablename__ = "biomedical_ai_help_info"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    article_title = Column(String(1000), nullable=True, comment="")
    article_pmid = Column(Integer, nullable=True, comment="")
    article_pmcid = Column(String(20), nullable=True, comment="")
    issn = Column(String(20), nullable=True, comment="")
    journal_name = Column(String(50), nullable=True, comment="")
    article_type = Column(String(20), nullable=True, comment="0 未爬取 1爬取成功 2不存在")
    article_structure_type = Column(String(20), nullable=True, comment="")
    content = Column(Text, nullable=True, comment="")


engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=14)
r_redis = redis.Redis(connection_pool=pool)


class API:
    def __init__(self):
        self.base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id='

    def get_task(self):
        # pmid, pmcid, issn, journal_name, article_type
        while r_redis.exists('half'):
            task = r_redis.rpop('half')
            # r_redis.rpush('half', task)
            yield task

    def split_func(self, task):
        item = task.split(',')
        pmid = item[0]
        pmcid = item[1]
        issn = item[2]
        journal_name = item[3]
        article_type = item[4]
        return pmid, pmcid, issn, journal_name, article_type

    def get_content(self, pmc):
        with requests.session() as s:
            # resp = s.get(self.base_url + pmc).text
            resp = s.get(self.base_url + 'PMC6225502').text
            if 'The publisher of this article does not allow downloading of the full text in XML form' in resp:
                return 0
            return resp

    def pre_judge(self, resp):
        xml = etree.HTML(resp)
        article_type = xml.xpath('//article[@article-type]/@article-type')[0].strip()
        return xml, article_type

    def judge(self, resp, xml):
        # todo title  --> str or 0
        if '<title-group>' in resp:
            title = xml.xpath('//title-group/article-title/text()')[0].strip()
        else:
            title = 0
        # todo abstract  --> list or 0
        if '<title>Abstract</title>' in resp or '<title>ABSTRACT</title>' in resp:
            abstract_list = []
            ps = xml.xpath('//title[contains(text(), "Abstract") or contains(text(), "ABSTRACT")]/..//p')
            for p in ps:
                a_text = p.xpath('.//text()')
                a_str = ''.join(a.strip() for a in a_text)
                if len(a_str) > 100:
                    abstract_list.append(a_str)
        elif '<abstract>' in resp:
            abstract_list = []
            ps = xml.xpath('//abstract//p')
            for p in ps:
                a_text = p.xpath('.//text()')
                a_str = ''.join(a.strip() for a in a_text)
                if len(a_str) > 100:
                    abstract_list.append(a_str)
        else:
            abstract_list = []
        # todo keyword  --> list or 0
        if '<kwd>' in resp:
            kwd_list = []
            kwds = xml.xpath('.//kwd')
            for kwd in kwds:
                key_word = kwd.xpath('./text()')[0].strip()
                kwd_list.append(key_word)
        else:
            kwd_list = []
        # todo introduction  --> list or 0
        if '<title>Introduction</title>' in resp or '<title>INTRODUCTION</title>' in resp:
            introduction_list = []
            introductions = xml.xpath('//body/sec/title[contains(text(), "Introduction") or contains(text(), "INTRODUCTION")]/..//p')
            for introduction in introductions:
                i_text = introduction.xpath('.//text()')
                i_str = ''.join(i.strip() for i in i_text)
                if len(i_str) > 100:
                    introduction_list.append(i_str)
        else:
            introduction_list = []
        # todo results  --> list or 0
        if '<sec sec-type="results"' in resp:
            results_list = []
            results = xml.xpath('//sec[@sec-type="results"]//p')
            for result in results:
                r_text = result.xpath('.//text()')
                r_str = ''.join(r.strip() for r in r_text)
                if len(r_str) > 100:
                    results_list.append(r_str)
        else:
            results_list = []
        # todo discussion  --> list or 0
        if '<sec sec-type="discussion"' in resp:
            discussions_list = []
            discussions = xml.xpath('//sec[@sec-type="discussion"]//p')
            for discussion in discussions:
                d_text = discussion.xpath('.//text()')
                d_str = ''.join(d.strip() for d in d_text)
                if len(d_str) > 100:
                    discussions_list.append(d_str)
        else:
            discussions_list = []
        # todo acknowledgements  --> list or 0
        if '<ack>' in resp:
            acks_list = []
            acks = xml.xpath('//ack//p')
            for ack in acks:
                ack_text = ack.xpath('.//text()')
                ack_str = ''.join(aa.strip() for aa in ack_text)
                if len(ack_str) > 100:
                    acks_list.append(ack_str)
        else:
            acks_list = []

        # todo conclusions  --> list or 0
        if '<title>Conclusions</title>' in resp or '<title>CONCLUSIONS</title>' in resp or '<title>Conclusion</title>' in resp or '<title>CONCLUSION</title>' in resp:
            conclusions_list = []
            if xml.xpath('.//sec/title[contains(text(), "Conclusions") or contains(text(), "CONCLUSIONS") or contains(text(), "Conclusion") or contains(text(), "CONCLUSION")]/..//p'):
                conclusions = xml.xpath('.//sec/title[contains(text(), "Conclusions") or contains(text(), "CONCLUSIONS") or contains(text(), "Conclusion") or contains(text(), "CONCLUSION")]/..//p')
                for conclusion in conclusions:
                    c_text = conclusion.xpath('.//text()')
                    c_str = ''.join(c.strip() for c in c_text)
                    if len(c_str) > 100:
                        conclusions_list.append(c_str)
        else:
            conclusions_list = []

        # todo Materials and methods  --> list or 0
        if '<title>Materials and methods</title>' in resp or '<title>MATERIALS AND METHODS</title>' in resp:
            mandms_list = []
            if xml.xpath('.//sec[@sec-type="materials|methods"]//p'):
                mandms = xml.xpath('.//sec[@sec-type="materials|methods"]//p')
                for mandm in mandms:
                    m_text = mandm.xpath('.//text()')
                    m_str = ''.join(c.strip() for c in m_text)
                    if len(m_str) > 100:
                        mandms_list.append(m_str)
        else:
            mandms_list = []

        # todo Research Design and Methods  --> list or 0 去除小于100
        if '<title>Research Design and Methods</title>' in resp or '<title>RESEARCH DESIGN AND METHODS</title>' in resp or '<title>Patients and Methods</title>' in resp or '<title>PATIENTS AND METHODS</title>' in resp:
            rdandms_list = []
            if xml.xpath(
                    './/body/sec/title[contains(text(), "Research Design and Methods") or contains(text(), "RESEARCH DESIGN AND METHODS") or contains(text(), "Patients and Methods") or contains(text(), "PATIENTS AND METHODS")]/..//p'):
                rdandms = xml.xpath(
                    './/body/sec/title[contains(text(), "Research Design and Methods") or contains(text(), "RESEARCH DESIGN AND METHODS") or contains(text(), "Patients and Methods") or contains(text(), "PATIENTS AND METHODS")]/..//p')
                for rdandm in rdandms:
                    rd_text = rdandm.xpath('.//text()')
                    rd_str = ''.join(c.strip() for c in rd_text)
                    if len(rd_str) > 100:
                        rdandms_list.append(rd_str)
        elif '<title>METHODS</title>' in resp:
            rdandms_list = []
            if xml.xpath('.//body/sec/title[contians(text(), "METHODS")]/..//p'):
                rdandms = xml.xpath('.//body/sec/title[contians(text(), "METHODS")]/..//p')
                for rdandm in rdandms:
                    rd_text = rdandm.xpath('.//text()')
                    rd_str = ''.join(c.strip() for c in rd_text)
                    if len(rd_str) > 100:
                        rdandms_list.append(rd_str)
        else:
            rdandms_list = []

        return (title,
                abstract_list,
                kwd_list,
                introduction_list,
                results_list,
                discussions_list,
                acks_list,
                conclusions_list,
                mandms_list,
                rdandms_list)

    def organize(self, pmid_1, pmcid_1, issn_1, journal_name_1, article_type_1, title_1, data_list, data_type):
        results_list = []
        if isinstance(title_1, int):
            title = None
        if len(data_list) > 0:
            for item in data_list:
                new_ab = Data(
                    article_title=title_1,
                    article_pmid=pmid_1,
                    article_pmcid=pmcid_1,
                    issn=issn_1,
                    journal_name=journal_name_1,
                    article_type=article_type_1,
                    article_structure_type=data_type,
                    content=item
                )
                results_list.append(new_ab)
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

    def run(self):
        # pmid, pmcid, issn, journal_name, article_type
        for task in self.get_task():
            task_results = []
            print(task)
            (pmid, pmcid, issn, journal_name, article_type) = self.split_func(task)
            resp = self.get_content(pmcid)
            if isinstance(resp, int):
                print(1)
                time.sleep(15)
                continue
            if isinstance(resp, str):
                print(2)
                xml, article_type = self.pre_judge(resp)
                if article_type == 'research-article':
                    (title,
                     abstract_list,
                     kwd_list,
                     introduction_list,
                     results_list,
                     discussions_list,
                     acks_list,
                     conclusions_list,
                     mandms_list,
                     rdandms_list) = self.judge(resp, xml)

                    final_abstract = self.organize(pmid, pmcid, issn, journal_name, article_type, title, abstract_list, 'abstract')
                    final_kwd = self.organize(pmid, pmcid, issn, journal_name, article_type, title, kwd_list, 'keyword')
                    final_introduction = self.organize(pmid, pmcid, issn, journal_name, article_type, title, introduction_list, 'introduction')
                    final_results = self.organize(pmid, pmcid, issn, journal_name, article_type, title, results_list, 'results')
                    final_discussions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, discussions_list, 'discussions')
                    final_acks = self.organize(pmid, pmcid, issn, journal_name, article_type, title, acks_list, 'ack')
                    final_conclusions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, conclusions_list, 'conclusions')
                    final_mandms = self.organize(pmid, pmcid, issn, journal_name, article_type, title, mandms_list, 'methods')
                    final_rdandms = self.organize(pmid, pmcid, issn, journal_name, article_type, title, rdandms_list, 'methods')
                    task_results.extend(final_rdandms)
                    task_results.extend(final_kwd)
                    task_results.extend(final_abstract)
                    task_results.extend(final_introduction)
                    task_results.extend(final_results)
                    task_results.extend(final_discussions)
                    task_results.extend(final_acks)
                    task_results.extend(final_conclusions)
                    task_results.extend(final_mandms)
                    self.insert(task_results)
            time.sleep(15)
            break


if __name__ == '__main__':
    api = API()
    api.run()
