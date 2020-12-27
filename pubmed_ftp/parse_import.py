import os
import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

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
    Abctract = Column(Text, nullable=True, comment="")


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


engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()


for root, dirs, files in os.walk(r"D:\Dev\FTP_DATA"):
    for file in files:
        # 获取文件路径
        path = os.path.join(root, file)
        lxml = etree.parse(path)
        article = lxml.xpath("//PubmedArticle")

        for item in article:
            # # todo PMID_version
            # pmid_version_text = item.xpath('.//PMID[@Version="1"]/text()')
            # pmid_version = ",".join(i for i in pmid_version_text)
            # if len(pmid_version) == 0:
            #     pmid_version = "Error"

            # # todo Date_revised
            # date_revised_text = item.xpath(".//DateRevised/*/text()")
            # date_revised = "-".join(i.strip() for i in date_revised_text)
            # if len(pmid_version) == 0:
            #     date_revised = None

            # # todo Journal_title
            # journal_title = item.xpath(".//Title/text()")[0].strip()

            # # todo Journal_ISSN_print
            # journal_issn_print = item.xpath('.//ISSN[@IssnType="Print"]/text()')[
            #     0
            # ].strip()
            # # print(journal_issn_print)

            # # todo Journal_ISSN_electronic
            # journal_issn_electronic = item.xpath(
            #     './/ISSN[@IssnType="Electronic"]/text()'
            # )[0].strip()
            # # print(journal_issn_electronic)

            # # todo Journal_vol
            # journal_vol = item.xpath(".//Volume/text()")[0].strip()
            # # print(journal_vol)

            # # todo Journal_issue
            # journal_issue = item.xpath(".//Issue/text()")[0].strip()
            # print(journal_issue)

            # # todo Journal_abbreviation
            # journal_abbreviation = item.xpath(".//ISOAbbreviation/text()")[0].strip()

            # # todo Journal_UniqueID
            # journal_uniqueid = item.xpath(".//NlmUniqueID/text()")[0].strip()

            # # todo Article_title
            # article_title = item.xpath(".//ArticleTitle/text()")[0].strip()
            # print(article_title)

            # todo Pubdate
            # pub_date_text = item.xpath(".//PubDate/*//text()")
            # pub_date = "-".join(i for i in pub_date_text)
            # if len(pub_date) == 0:
            #     pub_date = "Error"
            # print(pub_date)

            # todo Article_pmid
            # article_pmid = item.xpath('.//ArticleId[@IdType="pubmed"]/text()')[
            #     0
            # ].strip()
            # print(article_pmid)

            # todo Article_pii
            # article_pii = item.xpath('.//ArticleId[@IdType="pii"]/text()')[0].strip()
            # print(article_pii)

            # todo Article_doi
            # article_doi = item.xpath('.//ArticleId[@IdType="doi"]/text()')[0].strip()
            # print(article_doi)

            # todo Article_pmc
            # article_pmc = item.xpath('.//ArticleId[@IdType="pmc"]/text()')[0].strip()
            # print(article_pmc)

            # ? author_info 表
            # authorList = item.xpath('.//AuthorList[@CompleteYN="Y"]')
            # results = []
            # for author in authorList:
            #     try:
            #         lastName = author.xpath(".//LastName/text()")[0].strip()
            #     except Exception:
            #         lastName = None
            #     try:
            #         foreName = author.xpath(".//ForeName/text()")[0].strip()
            #     except Exception:
            #         foreName = None
            #     try:
            #         initials = author.xpath(".//Initials/text()")[0].strip()
            #     except Exception:
            #         initials = None
            #     try:
            #         affiliation_text = author.xpath(
            #             ".//AffiliationInfo/Affiliation/text()"
            #         )
            #         affiliation = "~".join(i.strip() for i in affiliation_text)
            #         if len(affiliation) == 0:
            #             affiliation = None
            #     except Exception:
            #         affiliation = None

            #     results.append([lastName, foreName, initials, affiliation])
            #     print(results)

            # ? article_keyword 表
            # keywordList = item.xpath(
            #     './/KeywordList[@Owner]/Keyword[@MajorTopicYN="N"]'
            # )
            # results = []
            # for keywords in keywordList:
            #     keyword = keywords.xpath("./text()")[0].strip()
            #     results.append([keyword])
            # print(results)

            # ? article_abstract 表
            # abctract_text = item.xpath(".//Abstract[not(@Label)]/AbstractText/text()")
            # abstract = "~".join(i.strip() for i in abctract_text)
            # if len(abstract) == 0:
            #     abstract = None
            # print(abstract)

            # ? article_grant 表
            grants = item.xpath('.//GrantList[@CompleteYN="Y"]/Grant')
            results = []
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
                results.append([grandId, grandAgency, grandAgencyAcronym, grantCountry])
            print(results)

        break