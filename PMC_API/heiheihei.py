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
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
        }

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
            resp = s.get(self.base_url + pmc, timeout=45, headers=self.headers).text
            xml = etree.HTML(resp)
            article_type = xml.xpath('//article[@article-type]/@article-type')[0].strip()
            if article_type == 'research-article' and 'The publisher of this article does not allow dow' \
                                                      'nloading of the full text in XML form' not in resp:
                print(2)
                return resp, xml
            else:
                print(1)
                resp = None
                xml = None
                return resp, xml

    def judge(self, resp, xml, pmid, pmcid, issn, journal_name, article_type):
        results_list = []

        # todo title  --> str or 0
        if '<title-group>' in resp:
            title = xml.xpath('//title-group/article-title/text()')[0].strip()
        else:
            title = 0
        # print(title)

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
        # print(abstract_list)

        # todo keyword  --> list or 0
        if '<kwd>' in resp:
            kwd_list = []
            kwds = xml.xpath('.//kwd')
            for kwd in kwds:
                key_word = kwd.xpath('./text()')[0].strip()
                kwd_list.append(key_word)
        else:
            kwd_list = []
        # print(kwd_list)

        # todo introduction  --> list or 0
        if xml.xpath('//front/following-sibling::*/title[contains(text(), "Introduction")]/..//p'):
            introduction_list = []
            introductions = xml.xpath('//front/following-sibling::*/title[contains(text(), "Introduction")]/..//p')
            for introduction in introductions:
                i_text = introduction.xpath('.//text()')
                i_str = ''.join(i.strip() for i in i_text)
                if len(i_str) > 100:
                    introduction_list.append(i_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "INTRODUCTION")]/..//p'):
            introduction_list = []
            introductions = xml.xpath('//front/following-sibling::*/title[contains(text(), "INTRODUCTION")]/..//p')
            for introduction in introductions:
                i_text = introduction.xpath('.//text()')
                i_str = ''.join(i.strip() for i in i_text)
                if len(i_str) > 100:
                    introduction_list.append(i_str)
        else:
            introduction_list = []
        # print(introduction_list)

        # todo results  --> list or 0
        if xml.xpath('//front/following-sibling::*/title[contains(text(), "Results")]/..//p'):
            results_list = []
            results = xml.xpath('//front/following-sibling::*/title[contains(text(), "Results")]/..//p')
            for result in results:
                r_text = result.xpath('.//text()')
                r_str = ''.join(r.strip() for r in r_text)
                if len(r_str) > 100:
                    results_list.append(r_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "RESULTS")]/..//p'):
            results_list = []
            results = xml.xpath('//front/following-sibling::*/title[contains(text(), "RESULTS")]/..//p')
            for result in results:
                r_text = result.xpath('.//text()')
                r_str = ''.join(r.strip() for r in r_text)
                if len(r_str) > 100:
                    results_list.append(r_str)
        else:
            results_list = []
        # print(results_list)

        # todo discussion  --> list or 0
        if xml.xpath('//front/following-sibling::*/title[contains(text(), "discussion")]/..//p'):
            discussions_list = []
            discussions = xml.xpath('//front/following-sibling::*/title[contains(text(), "discussion")]/..//p')
            for discussion in discussions:
                d_text = discussion.xpath('.//text()')
                d_str = ''.join(d.strip() for d in d_text)
                if len(d_str) > 100:
                    discussions_list.append(d_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "DISCUSSION")]/..//p'):
            discussions_list = []
            discussions = xml.xpath('//front/following-sibling::*/title[contains(text(), "DISCUSSION")]/..//p')
            for discussion in discussions:
                d_text = discussion.xpath('.//text()')
                d_str = ''.join(d.strip() for d in d_text)
                if len(d_str) > 100:
                    discussions_list.append(d_str)
        else:
            discussions_list = []
        # print(discussions_list)

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
        # print(acks_list)

        # todo Materials and methods  --> list or 0
        if xml.xpath('//front/following-sibling::*/title[contains(text(), "Patients and Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "Patients and Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "PATIENTS AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "PATIENTS AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "Materials and methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "Materials and methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "MATERIALS AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "MATERIALS AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "Research Design and Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "Research Design and Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "RESEARCH DESIGN AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "RESEARCH DESIGN AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//front/following-sibling::*/title[contains(text(), "METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//front/following-sibling::*/title[contains(text(), "METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        else:
            methods_list = []
        # print(methods_list)

        # todo conclusions  --> list or 0
        if xml.xpath('.//sec/title[contains(text(), "Conclusions")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('.//sec/title[contains(text(), "Conclusions")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('.//sec/title[contains(text(), "CONCLUSIONS")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('.//sec/title[contains(text(), "CONCLUSIONS")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('.//sec/title[contains(text(), "Conclusion")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('.//sec/title[contains(text(), "Conclusion")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('.//sec/title[contains(text(), "CONCLUSION")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('.//sec/title[contains(text(), "CONCLUSION")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        else:
            conclusions_list = []
        # print(conclusions_list)

        return (title, abstract_list, kwd_list, introduction_list,
                results_list, discussions_list, acks_list, methods_list,
                conclusions_list)

    def organize(self, pmid, pmcid, issn, journal_name, article_type, title, data_list, data_type):
        results_list = []
        if isinstance(title, int):
            title = None
        if len(data_list) > 0:
            for item in data_list:
                new_data = Data(
                    article_title=title,
                    article_pmid=pmid,
                    article_pmcid=pmcid,
                    issn=issn,
                    journal_name=journal_name,
                    article_type=article_type,
                    article_structure_type=data_type,
                    content=item
                )
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

    def insert(self, objs):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(objs)
            print('done')



    def run(self):
        for task in self.get_task():
            task_results = []
            print(task)
            pmid, pmcid, issn, journal_name, article_type = self.split_func(task)
            resp, xml = self.get_content(pmcid)
            if resp is not None and xml is not None:
                (title, abstract_list, kwd_list, introduction_list,
                 results_list, discussions_list, acks_list, methods_list,
                 conclusions_list) = self.judge(resp, xml, pmid, pmcid, issn, journal_name, article_type)
                f_abstract = self.organize(pmid, pmcid, issn, journal_name, article_type, title, abstract_list, 'abstract')
                f_kwd = self.organize(pmid, pmcid, issn, journal_name, article_type, title, kwd_list, 'kwd')
                f_introduction = self.organize(pmid, pmcid, issn, journal_name, article_type, title, introduction_list, 'introduction')
                f_results = self.organize(pmid, pmcid, issn, journal_name, article_type, title, results_list, 'results')
                f_discussions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, discussions_list, 'discussions')
                f_acks = self.organize(pmid, pmcid, issn, journal_name, article_type, title, acks_list, 'acks')
                f_methods = self.organize(pmid, pmcid, issn, journal_name, article_type, title, methods_list, 'methods')
                f_conclusions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, conclusions_list, 'conclusions')
                task_results.extend(f_abstract)
                task_results.extend(f_kwd)
                task_results.extend(f_introduction)
                task_results.extend(f_results)
                task_results.extend(f_discussions)
                task_results.extend(f_acks)
                task_results.extend(f_methods)
                task_results.extend(f_conclusions)
                self.insert(task_results)
            else:
                time.sleep(15)
                continue
            time.sleep(15)
            # break


if __name__ == '__main__':
    api = API()
    api.run()




















