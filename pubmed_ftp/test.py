from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import redis
import time

Base = declarative_base()

class Temp(Base):
    __tablename__ = "Temp_PMId"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    pmId=Column(Integer, nullable=True, comment="")
    xmlname = Column(String(40), nullable=True, comment="")

class Detail(Base):
    __tablename__ = "pubmed_article_detail"

    # id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    PMID_version = Column(String(50), nullable=True, comment="")
    Date_revised = Column(String(20), nullable=True, comment="")
    Journal_title = Column(String(200), nullable=True, comment="")
    Journal_ISSN_print = Column(String(40), nullable=True, comment="")
    Journal_ISSN_electronic = Column(String(40), nullable=True, comment="")
    Journal_ISSN_link = Column(String(40), nullable=True, comment="")
    Journal_vol = Column(String(20), nullable=True, comment="")
    Journal_issue = Column(String(20), nullable=True, comment="")
    Journal_abbreviation = Column(String(50), nullable=True, comment="")
    Journal_UniqueID = Column(String(50), nullable=True, comment="")
    Article_title = Column(String(2000), nullable=True, comment="")
    Pub_date = Column(String(20), nullable=True, comment="")
    Article_pmid = Column(Integer, primary_key=True, nullable=True, comment="")
    Article_pii = Column(String(50), nullable=True, comment="")
    Article_doi = Column(String(50), nullable=True, comment="")
    Article_pmc = Column(String(50), nullable=True, comment="")
    Article_abstract = Column(Text, nullable=True, comment="")
    Article_keyword = Column(String(2000), nullable=True, comment="")
    Article_type = Column(String(200), nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    Article_language = Column(String(10), nullable=True, comment="")


# class Abstract(Base):
#     __tablename__ = "article_abstract"

#     id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
#     Article_pmid = Column(String(20), nullable=True, comment="")
#     Abstract = Column(Text, nullable=True, comment="")


class Keyword(Base):
    __tablename__ = "Article_keyword"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    Key_word = Column(String(200), nullable=True, comment="")


class Info(Base):
    __tablename__ = "Author_info"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    LastName = Column(String(100), nullable=True, comment="")
    ForeName = Column(String(100), nullable=True, comment="")
    Initials = Column(String(10), nullable=True, comment="")
    Affiliation = Column(String(2000), nullable=True, comment="")


class Grant(Base):
    __tablename__ = "Article_grant"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    Grant_id = Column(String(200), nullable=True, comment="")
    Grant_agency = Column(String(2000), nullable=True, comment="")
    Grant_agency_acronym = Column(String(200), nullable=True, comment="")
    Grant_country = Column(String(200), nullable=True, comment="")


class Reference(Base):
    __tablename__ = "Article_reference"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    Citation_detail = Column(String(100), nullable=True, comment="")
    Citation_pmid = Column(String(100), nullable=True, comment="")
    # Citation_pmc = Column(String(100), nullable=True, comment="")
    # Citation_doi = Column(String(100), nullable=True, comment="")


class Publication(Base):
    __tablename__ = "Publication_type"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    Article_type = Column(String(200), nullable=True, comment="")


class Corrections(Base):
    __tablename__ = "Article_comments_corrections"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Article_pmid = Column(Integer, nullable=True, comment="")
    Article_reftype = Column(String(200), nullable=True, comment="")
    comments_corrections_pmid = Column(String(20), nullable=True, comment="")


# MySql
engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8mb4"
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


class Pubmed:
    def __init__(self):
        self.base_path = "C:/Pubmed/baseline/"

    def get_path(self):
        while r.exists("task_error"):
            path = r.lpop("task_error")
            # for i in (
            #     r"D:/pubmed21n1057.xml",
            #     # r"D:\Dev\FTP_DATA\pubmed21n0851.xml",
            #     # r"D:\Dev\FTP_DATA\pubmed21n0852.xml",
            #     # r"D:\Dev\FTP_DATA\pubmed21n0853.xml",
            #     # r"D:\Dev\FTP_DATA\pubmed21n0849.xml",
            # ):
            yield path

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
        # ! article_pmid
        # todo Article_pmid
        try:
            article_pmid = int(
                item.xpath('.//ArticleId[@IdType="pubmed"]/text()')[0].strip())
        except Exception:
            article_pmid = None

        # ? article_keyword 表
        keywordList = item.xpath(
            './/KeywordList[@Owner]/Keyword[@MajorTopicYN="N" or @MajorTopicYN="Y"]'
        )
        keyword_results = []
        keyword_results_2 = []
        for keywords in keywordList:
            try:
                key_word = keywords.xpath("./text()")[0].strip()[0:200]
                new_keyword = Keyword(Article_pmid=article_pmid,
                                      Key_word=key_word)
                keyword_results_2.append(key_word)
                keyword_results.append(new_keyword)
            except Exception:
                pass

        # session.bulk_save_objects(keyword_results)

        # ? Publication_type 表
        publicationTypeList = item.xpath(
            ".//PublicationTypeList/PublicationType[@UI]")
        pType_results = []
        pTypeList_2 = []
        for pType_item in publicationTypeList:
            try:
                pType = pType_item.xpath("./text()")[0].strip()
                new_pType = Publication(Article_pmid=article_pmid,
                                        Article_type=pType)
                pTypeList_2.append(pType)
                pType_results.append(new_pType)
            except Exception:
                pass

        # ? Article_comments_corrections 表
        # correctionsList = item.xpath(
        #     ".//CommentsCorrectionsList/CommentsCorrections[@RefType]")
        # correctionsList_results = []
        # correctionsList_results_2 = []
        # for correct in correctionsList:
        #     try:
        #         refType = correct.xpath("@RefType")[0].strip()
        #     except Exception:
        #         refType = None
        #     try:
        #         corrections_pmid = correct.xpath(
        #             './PMID[@Version="1"]/text()')[0].strip()
        #     except Exception:
        #         corrections_pmid = None
        #     if refType is None:
        #         pass
        #     else:
        #         new_corrections = Corrections(
        #             Article_pmid=article_pmid,
        #             Article_reftype=refType,
        #             comments_corrections_pmid=corrections_pmid,
        #         )
        #         correctionsList_results_2.append(refType)
        #         correctionsList_results.append(new_corrections)

        # ? pubmed_article_detail 表
        # todo PMID_version
        pmid_version_text = item.xpath(
            './MedlineCitation/PMID[@Version="1"]/text()')
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
            journal_issn_print = item.xpath(
                './/ISSN[@IssnType="Print"]/text()')[0].strip()
        except Exception:
            journal_issn_print = None
        # todo Journal_ISSN_electronic
        try:
            journal_issn_electronic = item.xpath(
                './/ISSN[@IssnType="Electronic"]/text()')[0].strip()
        except Exception:
            journal_issn_electronic = None
        # todo Journal_ISSN_link
        try:
            journal_issn_link = item.xpath(
                ".//MedlineJournalInfo/ISSNLinking/text()")[0].strip()
        except Exception:
            journal_issn_link = None
        # todo Journal_vol
        try:
            journal_vol = item.xpath(".//Volume/text()")[0].strip()[0:40]
        except Exception:
            journal_vol = None
        # todo Journal_issue
        try:
            journal_issue = item.xpath(".//Issue/text()")[0].strip()[0:20]
        except Exception:
            journal_issue = None
        # todo Journal_abbreviation
        try:
            journal_abbreviation = item.xpath(
                ".//ISOAbbreviation/text()")[0].strip()
        except Exception:
            journal_abbreviation = None
        # todo Journal_UniqueID
        try:
            journal_uniqueid = item.xpath(".//NlmUniqueID/text()")[0].strip()
        except Exception:
            journal_uniqueid = None
        # todo Article_title
        try:
            article_title_text = item.xpath(".//ArticleTitle//text()")
            article_title = "".join(i for i in article_title_text)
            if len(article_title) == 0:
                vernacular_text = item.xpath(".//VernacularTitle//text()")
                article_title = " ".join(v for v in vernacular_text)
        except Exception:
            article_title = None
        # todo Pub_date
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

        # todo Article_pii
        try:
            article_pii = item.xpath(
                './/ArticleId[@IdType="pii"]/text()')[0].strip()[0:100]
        except Exception:
            article_pii = None
        # todo Article_doi
        try:
            article_doi = item.xpath(
                './/ArticleId[@IdType="doi"]/text()')[0].strip()
        except Exception:
            article_doi = None
        # todo Article_pmc
        try:
            article_pmc = item.xpath(
                './/ArticleId[@IdType="pmc"]/text()')[0].strip()
        except Exception:
            article_pmc = None
        # todo Article_abstract
        abstract_list = item.xpath(".//Abstract[not(@Label)]/AbstractText")
        ab_list = []
        if len(abstract_list) > 0:
            for dust in abstract_list:
                text_content = dust.xpath(".//text()")
                single_abstract = "".join(v for v in text_content)
                ab_list.append(single_abstract)
            abstract = ";".join(f for f in ab_list)
        else:
            abstract = None
        # abstract = ";".join(i.strip() for i in abctract_text)
        # if len(abstract) == 0:
        #     abstract = None
        # todo Article_keyword
        kwd = ";".join(i for i in keyword_results_2)
        if len(kwd) == 0:
            kwd = None
        # todo Article_type
        article_type = ";".join(i for i in pTypeList_2)
        if len(article_type) == 0:
            article_type = None

        # todo d_refType
        ids = list(set(correctionsList_results_2))
        d_refType = ";".join(i for i in ids)
        if len(d_refType) == 0:
            d_refType = None

        # todo Article_language
        try:
            article_language = item.xpath(".//Language/text()")[0].strip()
        except Exception:
            article_language = None

        new_detail = Detail(
            PMID_version=pmid_version,
            Date_revised=date_revised,
            Journal_title=journal_title,
            Journal_ISSN_print=journal_issn_print,
            Journal_ISSN_electronic=journal_issn_electronic,
            Journal_ISSN_link=journal_issn_link,
            Journal_vol=journal_vol,
            Journal_issue=journal_issue,
            Journal_abbreviation=journal_abbreviation,
            Journal_UniqueID=journal_uniqueid,
            Article_title=article_title,
            Pub_date=pub_date,
            Article_pmid=article_pmid,
            Article_pii=article_pii,
            Article_doi=article_doi,
            Article_pmc=article_pmc,
            Article_abstract=abstract,
            Article_keyword=kwd,
            Article_type=article_type,
            Article_reftype=d_refType,
            Article_language=article_language,
        )
        # session.add(new_detail)

        # ? author_info 表
        authorList = item.xpath('.//Author[@ValidYN="Y"]')
        author_results = []
        for author in authorList:
            try:
                lastName = author.xpath(".//LastName/text()")[0].strip()[0:100]
            except Exception:
                lastName = None
            try:
                foreName = author.xpath(".//ForeName/text()")[0].strip()[0:100]
            except Exception:
                foreName = None
            try:
                initials = author.xpath(".//Initials/text()")[0].strip()
            except Exception:
                initials = None
            try:
                affiliation_text = author.xpath(
                    ".//AffiliationInfo/Affiliation/text()")
                affiliation = "~".join(i.strip()
                                       for i in affiliation_text)[0:2000]
                if len(affiliation) == 0:
                    affiliation = None
            except Exception:
                affiliation = None
            if (lastName is None and foreName is None and initials is None
                    and affiliation is None):
                pass
            else:
                new_info = Info(
                    Article_pmid=article_pmid,
                    LastName=lastName,
                    ForeName=foreName,
                    Initials=initials,
                    Affiliation=affiliation,
                )
                author_results.append(new_info)
        # session.bulk_save_objects(author_results)

        # # ? article_abstract 表
        # abctract_text = item.xpath(".//Abstract[not(@Label)]/AbstractText/text()")
        # abstract = ";".join(i.strip() for i in abctract_text)
        # if len(abstract) == 0:
        #     new_abstract = None
        # elif str(abstract):
        #     new_abstract = Abstract(Article_pmid=article_pmid, Abstract=abstract)
        # # session.add(new_abstract)

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

        # ? Temp_PMId 表
        temp_results = []
        new_temp = Temp(
                Article_pmid=article_pmid,
                xmlname='C:/Pubmed/baseline/pubmed21n0839.xml'
            )
        temp_results.append(new_temp)


        # # ? Article_reference 表
        # referenceList = item.xpath(".//ReferenceList/Reference")
        # reference_results = []
        # for reference in referenceList:
        #     try:
        #         citation_detail = reference.xpath("./Citation/text()")[0].strip()
        #         if len(citation_detail) > 100:
        #             citation_detail = None
        #     except Exception:
        #         citation_detail = None
        #     try:
        #         citation_pmid = reference.xpath(
        #             './ArticleIdList/ArticleId[@IdType="pubmed"]/text()'
        #         )[0].strip()
        #     except Exception:
        #         citation_pmid = None
        #     # try:
        #     #     citation_doi = reference.xpath(
        #     #         './ArticleIdList/ArticleId[@IdType="doi"]/text()'
        #     #     )[0].strip()
        #     # except Exception:
        #     #     citation_doi = None
        #     # try:
        #     #     citation_pmc = reference.xpath(
        #     #         './ArticleIdList/ArticleId[@IdType="pmc"]/text()'
        #     #     )[0].strip()
        #     # except Exception:
        #     #     citation_pmc = None
        #     if citation_pmid is None:
        #         pass
        #     else:
        #         new_reference = Reference(
        #             Article_pmid=article_pmid,
        #             Citation_detail=citation_detail,
        #             Citation_pmid=citation_pmid,
        #             # Citation_doi=citation_doi,
        #             # Citation_pmc=citation_pmc,
        #         )
        #         reference_results.append(new_reference)

        return (
            temp_results
            # new_detail,
            # author_results,
            # keyword_results,
            # grants_results,
            # pType_results,
            # correctionsList_results,
            # reference_results,
        )

    def insert(self, temp):  # detail, author, keyword, grants, p_type,correctionsreference
        time_start = time.time()
        session.bulk_save_objects(temp)
        # session.bulk_save_objects(detail)
        # session.bulk_save_objects(author)
        # session.bulk_save_objects(keyword)
        # session.bulk_save_objects(grants)
        # session.bulk_save_objects(p_type)
        # session.bulk_save_objects(corrections)
        # session.bulk_save_objects(reference)
        try:
            session.commit()
            print("done")
            flag = 1
            session.close()
        except Exception as e:
            session.rollback()
            print(e)
            flag = 2
            session.close()
            pass
        time_end = time.time()
        print("insert cost", (time_end - time_start) / 60)
        return flag

    def push_back(self, path):
        r.rpush("task_error_new", path)
        print("error push")

    def run(self):
        # MySql
        engine = create_engine(
            "mysql+pymysql://root:app1234@192.168.124.2:3306/pubmed_article?charset=utf8mb4"
        )
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        # Redis
        pool = redis.ConnectionPool(host="localhost",
                                    port=6379,
                                    decode_responses=True,
                                    db=0)
        r = redis.Redis(connection_pool=pool)
        for path in self.get_path():
            print(path)
            detail_data = []
            author_data = []
            keyword_data = []
            grants_data = []
            p_type_data = []
            corrections_data = []
            temp_data = []
            # reference_data = []
            xml = self.get_content(path)
            time_start = time.time()
            for item in self.parse_xml(xml):
                (
                    # detail_obj,
                    # author_list,
                    # keyword_list,
                    # grants_list,
                    # p_type_list,
                    temp_results
                    # corrections_list,
                    # reference_list,
                ) = self.parse_item(item)
                # if detail_obj:
                #     detail_data.append(detail_obj)
                # if author_list:
                #     author_data.extend(author_list)
                # if keyword_list:
                #     keyword_data.extend(keyword_list)
                # if grants_list:
                #     grants_data.extend(grants_list)
                # if p_type_list:
                #     p_type_data.extend(p_type_list)
                if temp_results:
                    temp_data.extend(temp_results)
                # if corrections_list:
                #     corrections_data.extend(corrections_list)
                # if reference_list:
                #     reference_data.extend(reference_list)
            time_end = time.time()
            print("parse cost", (time_end - time_start) / 60)
            # try:
            flag = self.insert(
                temp_data,
                # detail_data,
                # author_data,
                # keyword_data,
                # grants_data,
                # p_type_data,
                # corrections_data,
                # reference_data,

            )
            if flag != 1:
                self.push_back(path)
                # continue
                break
            

if __name__ == "__main__":
    pubmed = Pubmed()
    pubmed.run()
