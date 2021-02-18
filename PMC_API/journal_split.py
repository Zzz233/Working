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
    __tablename__ = "biomedical_ai_help_info_copy1"

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
        self.base_url = 'https://www.ncbi.nlm.nih.gov/pmc/articles/'
        self.ua = [
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;AvantBrowser)',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;360SE)',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;Trident/4.0;SE2.XMetaSr1.0;SE2.XMetaSr1.0;.NETCLR2.0.50727;SE2.XMetaSr1.0)',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;TheWorld)',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;TencentTraveler4.0)',
            'Opera/9.80(Macintosh;IntelMacOSX10.6.8;U;en)Presto/2.8.131Version/11.11',
            'Mozilla/5.0(WindowsNT6.1;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
            'Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)',
            'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36',
            'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
            "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
            "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
            "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
            "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        ]
        self.headers = {
            'User-Agent': random.choice(self.ua),
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'http://ir.nsfc.gov.cn/fieldPaper/H',
            'Accept-Ecoding': 'gzip, deflate',
            'Content-Type': 'application/json',
            'Cookie': 'JSESSIONID=430C29A8D3576BBEA81866DD312C8619',
            'X-Requested-With': 'XMLHttpRequest',
            'Host': 'ir.nsfc.gov.cn',
            'Origin': 'http://ir.nsfc.gov.cn'
        }

    def get_task(self):
        # pmid, pmcid, issn, journal_name, article_type
        while r_redis.exists('half_1'):
            task = r_redis.rpop('half_1')
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
        resp = requests.get(self.base_url + pmc, timeout=45, headers=self.headers)
        aa = resp.content.decode()
        html1 = bytes(bytearray(aa, encoding='utf-8'))
        # print(resp)
        xml = etree.HTML(html1)
        return html1, xml

    def judge(self, xml, pmid, pmcid, issn, journal_name, article_type):
        results_list = []

        # todo title  --> str or 0
        if xml.xpath('//h1[@class="content-title"]/text()'):
            title = xml.xpath('//h1[@class="content-title"]/text()')[0].strip()
        else:
            title = 0
        # print('title', title)

        # todo abstract  --> list or 0
        if xml.xpath('//h2[contains(text(), "Abstract")]/..//p'):
            abstract_list = []
            ps = xml.xpath('//h2[contains(text(), "Abstract")]/..//p')
            for p in ps:
                a_text = p.xpath('.//text()')
                a_str = ''.join(a.strip() for a in a_text)
                if len(a_str) > 100:
                    abstract_list.append(a_str)
        elif xml.xpath('//h2[contains(text(), "ABSTRACT")]/..//p'):
            abstract_list = []
            ps = xml.xpath('//h2[contains(text(), "ABSTRACT")]/..//p')
            for p in ps:
                a_text = p.xpath('.//text()')
                a_str = ''.join(a.strip() for a in a_text)
                if len(a_str) > 100:
                    abstract_list.append(a_str)
        else:
            abstract_list = []
        # print('abstract_list', abstract_list)

        # todo keyword  --> list or 0
        if xml.xpath('.//kwd'):
            kwd_list = []
            kwds = xml.xpath('.//kwd')
            for kwd in kwds:
                key_word = kwd.xpath('./text()')[0].strip()
                kwd_list.append(key_word)
        else:
            kwd_list = []
        # print('kwd', kwd_list)

        # todo introduction  --> list or 0
        if xml.xpath('//h2[contains(text(), "Introduction")]/..//p'):
            introduction_list = []
            introductions = xml.xpath('//h2[contains(text(), "Introduction")]/..//p')
            for introduction in introductions:
                i_text = introduction.xpath('.//text()')
                i_str = ''.join(i.strip() for i in i_text)
                if len(i_str) > 100:
                    introduction_list.append(i_str)
        elif xml.xpath('//h2[contains(text(), "INTRODUCTION")]/..//p'):
            introduction_list = []
            introductions = xml.xpath('//h2[contains(text(), "INTRODUCTION")]/..//p')
            for introduction in introductions:
                i_text = introduction.xpath('.//text()')
                i_str = ''.join(i.strip() for i in i_text)
                if len(i_str) > 100:
                    introduction_list.append(i_str)
        else:
            introduction_list = []
        # print('introduction_list', introduction_list)

        # todo results  --> list or 0
        if xml.xpath('//h2[contains(text(), "Results")]/..//p'):
            results_list = []
            results = xml.xpath('//h2[contains(text(), "Results")]/..//p')
            for result in results:
                r_text = result.xpath('.//text()')
                r_str = ''.join(r.strip() for r in r_text)
                if len(r_str) > 100:
                    results_list.append(r_str)
        elif xml.xpath('//h2[contains(text(), "RESULTS")]/..//p'):
            results_list = []
            results = xml.xpath('//h2[contains(text(), "RESULTS")]/..//p')
            for result in results:
                r_text = result.xpath('.//text()')
                r_str = ''.join(r.strip() for r in r_text)
                if len(r_str) > 100:
                    results_list.append(r_str)
        else:
            results_list = []
        # print('results_list', results_list)

        # todo discussion  --> list or 0
        if xml.xpath('//h2[contains(text(), "discussion")]/..//p'):
            discussions_list = []
            discussions = xml.xpath('//h2[contains(text(), "discussion")]/..//p')
            for discussion in discussions:
                d_text = discussion.xpath('.//text()')
                d_str = ''.join(d.strip() for d in d_text)
                if len(d_str) > 100:
                    discussions_list.append(d_str)
        elif xml.xpath('//h2[contains(text(), "DISCUSSION")]/..//p'):
            discussions_list = []
            discussions = xml.xpath('//h2[contains(text(), "DISCUSSION")]/..//p')
            for discussion in discussions:
                d_text = discussion.xpath('.//text()')
                d_str = ''.join(d.strip() for d in d_text)
                if len(d_str) > 100:
                    discussions_list.append(d_str)
        else:
            discussions_list = []
        # print('discussions_list', discussions_list)

        # todo acknowledgements  --> list or 0
        if xml.xpath('//h2[contains(text(), "Acknowledgment")]/..//p'):
            acks_list = []
            acks = xml.xpath('//h2[contains(text(), "Acknowledgment")]/..//p')
            for ack in acks:
                ack_text = ack.xpath('.//text()')
                ack_str = ''.join(aa.strip() for aa in ack_text)
                if len(ack_str) > 100:
                    acks_list.append(ack_str)
        else:
            acks_list = []
        # print('acks_list', acks_list)

        # todo Materials and methods  --> list or 0
        if xml.xpath('//h2[contains(text(), "Patients and Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "Patients and Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "PATIENTS AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "PATIENTS AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "Materials and methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "Materials and methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "MATERIALS AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "MATERIALS AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "Research Design and Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "Research Design and Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "RESEARCH DESIGN AND METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "RESEARCH DESIGN AND METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "Methods")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "Methods")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        elif xml.xpath('//h2[contains(text(), "METHODS")]/..//p'):
            methods_list = []
            mandms = xml.xpath('//h2[contains(text(), "METHODS")]/..//p')
            for mandm in mandms:
                m_text = mandm.xpath('.//text()')
                m_str = ''.join(c.strip() for c in m_text)
                if len(m_str) > 100:
                    methods_list.append(m_str)
        else:
            methods_list = []
        # print('methods_list', methods_list)

        # todo conclusions  --> list or 0
        if xml.xpath('//h2[contains(text(), "Conclusions")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('//h2[contains(text(), "Conclusions")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('//h2[contains(text(), "CONCLUSIONS")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('//h2[contains(text(), "CONCLUSIONS")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('//h2[contains(text(), "Conclusion")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('//h2[contains(text(), "Conclusion")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        elif xml.xpath('//h2[contains(text(), "CONCLUSION")]/..//p'):
            conclusions_list = []
            conclusions = xml.xpath('//h2[contains(text(), "CONCLUSION")]/..//p')
            for conclusion in conclusions:
                c_text = conclusion.xpath('.//text()')
                c_str = ''.join(c.strip() for c in c_text)
                if len(c_str) > 100:
                    conclusions_list.append(c_str)
        else:
            conclusions_list = []
        # print('conclusions_list', conclusions_list)

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
                    article_type='research-article',
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

    def push_back(self, task):
        r_redis.lpush("half_1", task)
        print('放回去')

    def run(self):
        for task in self.get_task():
            task_results = []
            print(task)
            pmid, pmcid, issn, journal_name, article_type = self.split_func(task)
            try:
                resp, xml = self.get_content(pmcid)
                # print(resp)
            except Exception as e:
                print(e)
                self.push_back(task)
                time.sleep(60)
                continue
            if resp is not None and xml is not None:
                (title, abstract_list, kwd_list, introduction_list,
                 results_list, discussions_list, acks_list, methods_list,
                 conclusions_list) = self.judge(xml, pmid, pmcid, issn, journal_name, article_type)
                f_abstract = self.organize(pmid, pmcid, issn, journal_name, article_type, title, abstract_list,
                                           'abstract')
                f_kwd = self.organize(pmid, pmcid, issn, journal_name, article_type, title, kwd_list, 'kwd')
                f_introduction = self.organize(pmid, pmcid, issn, journal_name, article_type, title, introduction_list,
                                               'introduction')
                f_results = self.organize(pmid, pmcid, issn, journal_name, article_type, title, results_list, 'results')
                f_discussions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, discussions_list,
                                              'discussions')
                f_acks = self.organize(pmid, pmcid, issn, journal_name, article_type, title, acks_list, 'acks')
                f_methods = self.organize(pmid, pmcid, issn, journal_name, article_type, title, methods_list, 'methods')
                f_conclusions = self.organize(pmid, pmcid, issn, journal_name, article_type, title, conclusions_list,
                                              'conclusions')
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


if __name__ == '__main__':
    api = API()
    api.run()
