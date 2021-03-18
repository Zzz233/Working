import redis
import time
from contextlib import contextmanager
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
import pymysql
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

date_json = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

Base = declarative_base()


class Corrections(Base):
    __tablename__ = "1_article_comments_corrections"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    comments_corrections_pmid = Column(String(20), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Grant(Base):
    __tablename__ = "1_article_grant"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Grant_id = Column(String(500), nullable=True, comment="")
    Grant_agency = Column(String(2000), nullable=True, comment="")
    Grant_agency_acronym = Column(String(200), nullable=True, comment="")
    Grant_country = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Keyword(Base):
    __tablename__ = "1_Article_keyword"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Key_word = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Reference(Base):
    __tablename__ = "1_Article_reference"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    cited_pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Type(Base):
    __tablename__ = "1_article_type"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_type = Column(String(200), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Author(Base):
    __tablename__ = "1_Author_info"

    pmid = Column(Integer, primary_key=True, nullable=True, comment="id")
    ext_id = Column(Integer, primary_key=True, nullable=True, comment="")
    LastName = Column(String(100), nullable=True, comment="")
    ForeName = Column(String(100), nullable=True, comment="")
    Initials = Column(String(20), nullable=True, comment="")
    Affiliation = Column(String(2000), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Detail(Base):
    __tablename__ = "1_pubmed_article_detail"

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


class Temp(Base):
    __tablename__ = "temp"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmid = Column(Integer, nullable=True, comment="")
    xmlname = Column(String(40), nullable=True, comment="")
    Version = Column(Integer, nullable=True, comment="")


class Temp1(Base):
    __tablename__ = "temp_1"

    pmid = Column(Integer, primary_key=True, comment="id")


class Pubmed:
    def __init__(self):
        self.base_path = 'C:/Pubmed/update/'

    def get_path(self):
        while r.exists('xml_task1'):
            path = self.base_path + r.lpop("xml_task1")
            yield path

    def get_content(self, path):
        xml = etree.parse(path)
        articles = xml.xpath("//PubmedArticle")
        for item in articles:
            yield item
    # def get_content(self, path):
    #     xml = etree.parse(path)
    #     if xml:
    #         return xml
    #     return None
    #
    # def parse_xml(self, xml):
    #     articles = xml.xpath("//PubmedArticle")
    #     for item in articles:
    #         yield item

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

        # todo article_comments_corrections 表
        corrections_list = item.xpath(
            ".//CommentsCorrectionsList/CommentsCorrections[@RefType]")
        corrections_results_list = []
        corrections_results_list_2 = []
        for flag_c, correct in enumerate(corrections_list):
            try:
                ref_type = correct.xpath("@RefType")[0].strip()
            except Exception:
                ref_type = None
            try:
                corrections_pmid = correct.xpath(
                    './PMID[@Version]/text()')[0].strip()
            except Exception:
                corrections_pmid = None
            if ref_type is None:
                pass
            else:
                new_corrections = Corrections(
                    pmid=main_pmid,
                    ext_id=flag_c,
                    Article_reftype=ref_type,
                    comments_corrections_pmid=corrections_pmid,
                    Version=version,
                )
                corrections_results_list_2.append(ref_type)
                corrections_results_list.append(new_corrections)  # todo 结果

        # todo article_grant 表
        grants = item.xpath('.//GrantList[@CompleteYN="Y"]/Grant')
        grants_results_list = []
        for flag_g, grant in enumerate(grants):
            try:
                grand_id = grant.xpath("./GrantID/text()")[0].strip()[0:500]
            except Exception:
                grand_id = None
            try:
                grand_agency = grant.xpath("./Agency/text()")[0].strip()
            except Exception:
                grand_agency = None
            try:
                grand_agency_acronym = grant.xpath("./Acronym/text()")[0].strip()
            except Exception:
                grand_agency_acronym = None
            try:
                grant_country = grant.xpath("./Country/text()")[0].strip()
            except Exception:
                grant_country = None
            new_grant = Grant(
                pmid=main_pmid,
                ext_id=flag_g,
                Grant_id=grand_id,
                Grant_agency=grand_agency,
                Grant_agency_acronym=grand_agency_acronym,
                Grant_country=grant_country,
                Version=version,
            )
            grants_results_list.append(new_grant)  # todo 结果

        # todo Article_keyword 表
        keyword_list = item.xpath(
            './/KeywordList[@Owner]/Keyword[@MajorTopicYN="N" or @MajorTopicYN="Y"]'
        )
        keyword_results_list = []
        keyword_results_list_2 = []
        for flag_k, keywords in enumerate(keyword_list):
            try:
                key_word = keywords.xpath("./text()")[0].strip()[0:200]
                new_keyword = Keyword(pmid=main_pmid,
                                      ext_id=flag_k,
                                      Key_word=key_word,
                                      Version=version,
                                      )
                keyword_results_list_2.append(key_word)
                keyword_results_list.append(new_keyword)  # todo 结果
            except Exception:
                pass

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

        # todo article_type 表
        article_type_list = item.xpath(
            ".//PublicationTypeList/PublicationType[@UI]")
        article_type_results_list = []
        article_type_results_list_2 = []
        for flag_t, a_type_item in enumerate(article_type_list):
            try:
                a_type = a_type_item.xpath("./text()")[0].strip()
                new_a_type = Type(pmid=main_pmid,
                                  ext_id=flag_t,
                                  Article_type=a_type,
                                  Version=version)
                article_type_results_list_2.append(a_type)
                article_type_results_list.append(new_a_type)  # todo 结果
            except Exception:
                pass

        # todo author_info 表
        author_list = item.xpath('.//Author[@ValidYN="Y"]')
        author_results_list = []
        author_results_list_2 = []
        author_results_list_3 = []
        for flag_a, author in enumerate(author_list):
            try:
                last_name = author.xpath(".//LastName/text()")[0].strip()[0:100]
            except Exception:
                last_name = None
            try:
                fore_name = author.xpath(".//ForeName/text()")[0].strip()[0:100]
            except Exception:
                fore_name = None
            try:
                initials = author.xpath(".//Initials/text()")[0].strip()
            except Exception:
                initials = None
            try:
                affiliation_text = author.xpath(
                    ".//AffiliationInfo/Affiliation/text()")
                affiliation = "~".join(i.strip()
                                       for i in affiliation_text)[0:2000]
            except Exception:
                affiliation = None
            if (last_name is None and fore_name is None and initials is None
                    and affiliation is None):
                pass
            else:
                new_info = Author(
                    pmid=main_pmid,
                    ext_id=flag_a,
                    LastName=last_name,
                    ForeName=fore_name,
                    Initials=initials,
                    Affiliation=affiliation,
                    Version=version
                )
                if flag_a <= 10:
                    author_results_list_3.append(affiliation)
                if flag_a <= 20:
                    if fore_name is None:
                        fore_name = ''
                    if last_name is None:
                        last_name = ''
                    hehe = fore_name + ' ' + last_name
                    if len(hehe) > 1:
                        author_results_list_2.append(hehe)
                author_results_list.append(new_info)  # todo 结果

        # todo pubmed_article_detail 表
        # Journal_title
        try:
            journal_title = item.xpath(".//Title/text()")[0].strip()
        except Exception:
            journal_title = None
        # Journal_issn 原 Journal_ISSN_link
        try:
            journal_issn_link = item.xpath(
                ".//MedlineJournalInfo/ISSNLinking/text()")[0].strip()
        except Exception:
            journal_issn_link = None
        # Journal_vol
        try:
            journal_vol = item.xpath(".//Volume/text()")[0].strip()[0:40]
        except Exception:
            journal_vol = None
        # Journal_issue
        try:
            journal_issue = item.xpath(".//Issue/text()")[0].strip()[0:20]
        except Exception:
            journal_issue = None
        # Journal_abbr
        try:
            journal_abbr = item.xpath(
                ".//ISOAbbreviation/text()")[0].strip()[0:100]
        except Exception:
            journal_abbr = None
        # Article_title
        try:
            article_title_text = item.xpath(".//ArticleTitle//text()")
            article_title = "".join(i for i in article_title_text).rstrip('.')
            if len(article_title) == 0:
                vernacular_text = item.xpath(".//VernacularTitle//text()")
                article_title = " ".join(v for v in vernacular_text).rstrip('.')
        except Exception:
            article_title = None
        # Pub_date
        try:
            year = item.xpath(".//PubDate/Year/text()")[0].strip()
        except Exception:
            year = "1800"
        try:
            pub_date_month = item.xpath(".//PubDate/Month/text()")[0].strip()
            if pub_date_month.isdigit():
                month = pub_date_month
            else:
                month = date_json[pub_date_month]
        except Exception:
            month = "01"
        try:
            day = item.xpath(".//PubDate/Day/text()")[0].strip()
        except Exception:
            day = "01"
        pub_date = "-".join(i for i in (year, month, day))[0:40]
        if len(pub_date) == 0:
            pub_date = None
        # Article_doi
        try:
            article_doi = item.xpath(
                './/ArticleId[@IdType="doi"]/text()')[0].strip()
        except Exception:
            article_doi = None
        # Article_pmc
        try:
            article_pmc = item.xpath(
                './/ArticleId[@IdType="pmc"]/text()')[0].strip()
        except Exception:
            article_pmc = None
        # Article_abstract
        if item.xpath(".//Abstract[not(@Label)]/AbstractText[@Label]"):
            abstract_list = item.xpath(".//Abstract[not(@Label)]/AbstractText")
            ab_list = []
            if len(abstract_list) > 0:
                for dust in abstract_list:
                    try:
                        text_content = dust.xpath(".//text()")
                        single_abstract_text = "".join(v for v in text_content)
                        single_abstract_label = dust.xpath("./@Label")[0].strip()
                        single_abstract = '#' + single_abstract_label + '#' + single_abstract_text
                        ab_list.append(single_abstract)
                    except Exception:
                        pass
                abstract = "~".join(f for f in ab_list)
            else:
                abstract = None
        elif item.xpath(".//Abstract[not(@Label)]/AbstractText[not(@Label)]"):
            abstract_list = item.xpath(".//Abstract[not(@Label)]/AbstractText")
            ab_list = []
            if len(abstract_list) > 0:
                for dust in abstract_list:
                    try:
                        text_content = dust.xpath(".//text()")
                        single_abstract = "".join(v for v in text_content)
                        ab_list.append(single_abstract)
                    except Exception:
                        pass
                abstract = "~".join(f for f in ab_list)
            else:
                abstract = None
        else:
            abstract = None
        # Article_keyword
        kwd = ";".join(i for i in keyword_results_list_2)[0:1000]
        if len(kwd) == 0:
            kwd = None
        # Article_type
        article_type = ";".join(i for i in article_type_results_list_2)
        if len(article_type) == 0:
            article_type = None
        # Article_reftype
        ids_1 = list(set(corrections_results_list_2))
        d_ref_type = ";".join(i for i in ids_1)
        if len(d_ref_type) == 0:
            d_ref_type = None
        # Article_language
        try:
            article_language = item.xpath(".//Language/text()")[0].strip()
        except Exception:
            article_language = None
        # Authors
        authors = ";".join(i for i in author_results_list_2)
        if len(authors) == 0:
            authors = None
        # Institutions
        if author_results_list_3 is not None:
            ids_2 = list(set(author_results_list_3))
            institutions = "~".join(i for i in ids_2)[0:2000]
        else:
            institutions = None

        new_detail = Detail(
            pmid=main_pmid,
            Journal_title=journal_title,
            Journal_issn=journal_issn_link,
            Journal_vol=journal_vol,
            Journal_issue=journal_issue,
            Journal_abbr=journal_abbr,
            Article_title=article_title,
            Pub_date=pub_date,
            Article_doi=article_doi,
            Article_pmc=article_pmc,
            Article_abstract=abstract,
            Article_keyword=kwd,
            Article_type=article_type,
            Article_reftype=d_ref_type,
            Article_language=article_language,
            Authors=authors,
            Institutions=institutions,
            Version=version
        )

        new_temp = Temp(
            pmid=main_pmid,
            xmlname=path,
            Version=version
        )

        return (corrections_results_list,
                grants_results_list,
                keyword_results_list,
                reference_results_list,
                article_type_results_list,
                author_results_list,
                new_detail,
                new_temp)

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

    def insert_to_temp_1(self, pmid_list):
        dup_list = []
        with self.session_maker() as db_session:
            db_session.query(Temp1).delete(synchronize_session='evaluate')
            for dup_item in pmid_list:
                new_temp_1 = Temp1(pmid=dup_item)
                dup_list.append(new_temp_1)
            db_session.bulk_save_objects(dup_list)
            print('temp_1表写入')

    def delete_items(self):
        py_db = pymysql.connect(host='192.168.124.10',
                                user='root',
                                password='app1234',
                                db='pubmed_article',
                                charset='utf8mb4',)
        cursor = py_db.cursor()
        sql = ["delete g from pubmed_article.1_article_comments_corrections as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;",
               "delete g from pubmed_article.1_article_grant as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;",
               "delete g from pubmed_article.1_article_keyword as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;",
               "delete g from pubmed_article.1_article_type as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;",
               "delete g from pubmed_article.1_author_info as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;",
               "delete g from pubmed_article.1_pubmed_article_detail as g inner join pubmed_article.temp_1 as d on g.pmid = d.pmid;"]
        for sql_str in sql:
            try:
                # 执行SQL语句
                cursor.execute(sql_str)
                # 提交修改
                py_db.commit()
                print(sql_str + '\n' + '==========')
            except Exception as e:
                print(e)
                # 发生错误时回滚
                py_db.rollback()
        # py_db.close()

    def insert(self, corrections, grant, keyword, reference, r_type, author, detail):
        with self.session_maker() as db_session:
            db_session.bulk_save_objects(author)
            db_session.bulk_save_objects(corrections)
            db_session.bulk_save_objects(grant)
            db_session.bulk_save_objects(keyword)
            db_session.bulk_save_objects(reference)
            db_session.bulk_save_objects(r_type)
            db_session.bulk_save_objects(detail)
            print('全部写入' + '\n' + '==========')

    def run(self):
        for path in self.get_path():
            print(path)
            corrections_data = []
            grants_data = []
            keyword_data = []
            reference_data = []
            article_type_data = []
            author_data = []
            detail_data = []
            temp_data = []
            # xml = self.get_content(path)
            for item in self.get_content(path):
                (
                    corrections_list,
                    grants_list,
                    keyword_list,
                    reference_list,
                    article_type_list,
                    author_list,
                    detail_obj,
                    temp_obj
                ) = self.parse_item(item, path)
                if corrections_list:
                    corrections_data.extend(corrections_list)
                if grants_list:
                    grants_data.extend(grants_list)
                if keyword_list:
                    keyword_data.extend(keyword_list)
                # if reference_list:
                #     reference_data.extend(reference_list)
                if article_type_list:
                    article_type_data.extend(article_type_list)
                if author_list:
                    author_data.extend(author_list)
                if detail_obj:
                    detail_data.append(detail_obj)
                if temp_obj:
                    temp_data.append(temp_obj)
            final_corrections_data = self.pre_removal(corrections_data)
            final_grants_data = self.pre_removal(grants_data)
            final_keyword_data = self.pre_removal(keyword_data)
            # final_reference_data = self.pre_removal(reference_data)
            final_reference_data = []
            final_article_type_data = self.pre_removal(article_type_data)
            final_author_data = self.pre_removal(author_data)
            final_detail_data = self.pre_removal(detail_data)
            final_temp_data = self.pre_removal(temp_data)
            print('pre_removal')
            dup_show = self.insert_to_temp(final_temp_data)

            if len(dup_show) > 0:
                # todo 写入temp_1 删除
                self.insert_to_temp_1(dup_show)
                self.delete_items()
                self.insert(final_corrections_data,
                            final_grants_data,
                            final_keyword_data,
                            final_reference_data,
                            final_article_type_data,
                            final_author_data,
                            final_detail_data)
            elif len(dup_show) == 0:
                self.insert(final_corrections_data,
                            final_grants_data,
                            final_keyword_data,
                            final_reference_data,
                            final_article_type_data,
                            final_author_data,
                            final_detail_data)
            r.lpush('done', path.replace('C:/Pubmed/update/', ''))
            print('文件结束' + '\n' + '==========')
            # break


if __name__ == "__main__":
    pubmed = Pubmed()
    pubmed.run()
