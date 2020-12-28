from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis

Base = declarative_base()


class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    PMID_version = Column(String(50), nullable=True, comment="")
    Date_revised = Column(String(20), nullable=True, comment="")
    Journal_title = Column(String(200), nullable=True, comment="")
    Journal_ISSN_print = Column(String(40), nullable=True, comment="")
    Journal_ISSN_electronic = Column(String(40), nullable=True, comment="")
    Journal_vol = Column(String(20), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbreviation = Column(String(50), nullable=True, comment="")
    Journal_UniqueID = Column(String(50), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pubdate = Column(String(20), nullable=True, comment="")
    Article_pmid = Column(String(30), nullable=True, comment="")
    Article_pii = Column(String(50), nullable=True, comment="")
    Article_doi = Column(String(50), nullable=True, comment="")
    Article_pmc = Column(String(50), nullable=True, comment="")


class Abstract(Base):
    __tablename__ = "article_abstract"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    Abstract = Column(Text, nullable=True, comment="")


class Keyword(Base):
    __tablename__ = "article_keyword"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    Key_word = Column(String(200), nullable=True, comment="")


class Info(Base):
    __tablename__ = "author_info"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    LastName = Column(String(100), nullable=True, comment="")
    ForeName = Column(String(100), nullable=True, comment="")
    Initials = Column(String(10), nullable=True, comment="")
    Affiliation = Column(String(2000), nullable=True, comment="")


class Grant(Base):
    __tablename__ = "article_grant"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(String(20), nullable=True, comment="")
    Grant_id = Column(String(200), nullable=True, comment="")
    Grant_agency = Column(String(2000), nullable=True, comment="")
    Grant_agency_acronym = Column(String(200), nullable=True, comment="")
    Grant_country = Column(String(200), nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/pubmed_article?charset=utf8mb4"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)


class Pubmed:
    def __init__(self):
        self.base_path = "D:/Dev/FTP"

    def get_path(self):
        # while r.exists("ftp_file_path"):
        #     path = self.base_path + r.rpop("ftp_file_path")
        for i in (
            r"D:\Dev\FTP_DATA\pubmed21n1061.xml",
            r"D:\Dev\FTP_DATA\pubmed21n1059.xml",
            r"D:\Dev\FTP_DATA\pubmed21n1056.xml",
        ):
            yield i

    def get_content(self, path):
        xml = etree.parse(path)
        if xml:
            return xml
        return None

    def parse_xml(self, xml):
        articles = xml.xpath("//PubmedArticle")
        for item in articles:
            yield item

    def parse_item(self, item):
        # ? pubmed_article_detail 表
        # todo PMID_version
        pmid_version_text = item.xpath('.//PMID[@Version="1"]/text()')
        pmid_version = ",".join(i for i in pmid_version_text)
        if len(pmid_version) == 0:
            pmid_version = "Error"
        # todo Date_revised
        date_revised_text = item.xpath(".//DateRevised/*/text()")
        date_revised = "-".join(i.strip() for i in date_revised_text)
        if len(date_revised) == 0:
            date_revised = None
        # todo Journal_title
        try:
            journal_title = item.xpath(".//Title/text()")[0].strip()
        except Exception:
            journal_title = None
        # todo Journal_ISSN_print
        try:
            journal_issn_print = item.xpath('.//ISSN[@IssnType="Print"]/text()')[
                0
            ].strip()
        except Exception:
            journal_issn_print = None
        # todo Journal_ISSN_electronic
        try:
            journal_issn_electronic = item.xpath(
                './/ISSN[@IssnType="Electronic"]/text()'
            )[0].strip()
        except Exception:
            journal_issn_electronic = None
        # todo Journal_vol
        try:
            journal_vol = item.xpath(".//Volume/text()")[0].strip()
        except Exception:
            journal_vol = None
        # todo Journal_issue
        try:
            journal_issue = item.xpath(".//Issue/text()")[0].strip()
        except Exception:
            journal_issue = None
        # todo Journal_abbreviation
        try:
            journal_abbreviation = item.xpath(".//ISOAbbreviation/text()")[0].strip()
        except Exception:
            journal_abbreviation = None
        # todo Journal_UniqueID
        try:
            journal_uniqueid = item.xpath(".//NlmUniqueID/text()")[0].strip()
        except Exception:
            journal_uniqueid = None
        # todo Article_title
        try:
            article_title = item.xpath(".//ArticleTitle/text()")[0].strip()
        except Exception:
            article_title = None
        # todo Pubdate
        pub_date_text = item.xpath(".//PubDate/*//text()")
        pub_date = "-".join(i for i in pub_date_text)
        if len(pub_date) == 0:
            pub_date = None
        # todo Article_pmid
        try:
            article_pmid = item.xpath('.//ArticleId[@IdType="pubmed"]/text()')[
                0
            ].strip()
        except Exception:
            article_pmid = None
        # todo Article_pii
        try:
            article_pii = item.xpath('.//ArticleId[@IdType="pii"]/text()')[0].strip()
        except Exception:
            article_pii = None
        # todo Article_doi
        try:
            article_doi = item.xpath('.//ArticleId[@IdType="doi"]/text()')[0].strip()
        except Exception:
            article_doi = None
        # todo Article_pmc
        try:
            article_pmc = item.xpath('.//ArticleId[@IdType="pmc"]/text()')[0].strip()
        except Exception:
            article_pmc = None

        new_detail = Detail(
            PMID_version=pmid_version,
            Date_revised=date_revised,
            Journal_title=journal_title,
            Journal_ISSN_print=journal_issn_print,
            Journal_ISSN_electronic=journal_issn_electronic,
            Journal_vol=journal_vol,
            Journal_issue=journal_issue,
            Journal_abbreviation=journal_abbreviation,
            Journal_UniqueID=journal_uniqueid,
            Article_title=article_title,
            Pubdate=pub_date,
            Article_pmid=article_pmid,
            Article_pii=article_pii,
            Article_doi=article_doi,
            Article_pmc=article_pmc,
        )
        # session.add(new_detail)

        # ? author_info 表
        authorList = item.xpath('.//Author[@ValidYN="Y"]')
        author_results = []
        for author in authorList:
            try:
                lastName = author.xpath(".//LastName/text()")[0].strip()
            except Exception:
                lastName = None
            try:
                foreName = author.xpath(".//ForeName/text()")[0].strip()
            except Exception:
                foreName = None
            try:
                initials = author.xpath(".//Initials/text()")[0].strip()
            except Exception:
                initials = None
            try:
                affiliation_text = author.xpath(".//AffiliationInfo/Affiliation/text()")
                affiliation = "~".join(i.strip() for i in affiliation_text)[0:2000]
                if len(affiliation) == 0:
                    affiliation = None
            except Exception:
                affiliation = None
            new_info = Info(
                Article_pmid=article_pmid,
                LastName=lastName,
                ForeName=foreName,
                Initials=initials,
                Affiliation=affiliation,
            )
            author_results.append(new_info)
        # session.bulk_save_objects(author_results)

        # ? article_keyword 表
        keywordList = item.xpath('.//KeywordList[@Owner]/Keyword[@MajorTopicYN="N"]')
        keyword_results = []
        for keywords in keywordList:
            try:
                keyword = keywords.xpath("./text()")[0].strip()
                new_keyword = Keyword(Article_pmid=article_pmid, Key_word=keyword)
                keyword_results.append(new_keyword)
            except Exception:
                pass
        # session.bulk_save_objects(keyword_results)

        # ? article_abstract 表
        abctract_text = item.xpath(".//Abstract[not(@Label)]/AbstractText/text()")
        abstract = "~".join(i.strip() for i in abctract_text)
        if len(abstract) == 0:
            new_abstract = None
        elif str(abstract):
            new_abstract = Abstract(Article_pmid=article_pmid, Abstract=abstract)
        # session.add(new_abstract)

        # ? article_grant 表
        grants = item.xpath('.//GrantList[@CompleteYN="Y"]/Grant')
        grants_results = []
        for grant in grants:
            try:
                grandId = grant.xpath("./GrantID/text()")[0].strip()
            except Exception:
                grandId = None
            try:
                grandAgency = grant.xpath("./Agency/text()")[0].strip()
            except Exception:
                grandAgency = None
            try:
                grandAgencyAcronym = grant.xpath("./Acronym/text()")[0].strip()
            except Exception:
                grandAgencyAcronym = None
            try:
                grantCountry = grant.xpath("./Country/text()")[0].strip()
            except Exception:
                grantCountry = None
            new_grant = Grant(
                Article_pmid=article_pmid,
                Grant_id=grandId,
                Grant_agency=grandAgency,
                Grant_agency_acronym=grandAgencyAcronym,
                Grant_country=grantCountry,
            )
            grants_results.append(new_grant)
        # session.bulk_save_objects(grants_results)

        # try:
        #     session.commit()
        #     session.close()
        #     print(article_pmid, "done")
        # except Exception as e:
        #     session.rollback()
        #     print(e)

        return new_detail, author_results, keyword_results, new_abstract, grants_results

    def insert(self, detail, author, keyword, abstract, grants):
        session.bulk_save_objects(detail)
        session.bulk_save_objects(author)
        session.bulk_save_objects(keyword)
        session.bulk_save_objects(abstract)
        session.bulk_save_objects(grants)
        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            session.rollback()
            print(e)

    def run(self):
        for path in self.get_path():
            detail_data = []
            author_data = []
            keyword_data = []
            abstract_data = []
            grants_data = []
            xml = self.get_content(path)
            for item in self.parse_xml(xml):
                (
                    detail_obj,
                    author_list,
                    keyword_list,
                    abstract_obj,
                    grants_list,
                ) = self.parse_item(item)
                if detail_obj:
                    detail_data.append(detail_obj)
                if author_list:
                    author_data.extend(author_list)
                if keyword_list:
                    keyword_data.extend(keyword_list)
                if abstract_obj:
                    abstract_data.append(abstract_obj)
                if grants_list:
                    grants_data.extend(grants_list)
            self.insert(
                detail_data, author_data, keyword_data, abstract_data, grants_data
            )


if __name__ == "__main__":
    pubmed = Pubmed()
    pubmed.run()
