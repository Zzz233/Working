"""
Suitable Sample    sample type
Label conjugated 

Calibration Range    assay range
"""
import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random
import redis

Base = declarative_base()


class Detail(Base):
    __tablename__ = "sigmaaldrich_kit_detail"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Brand = Column(String(40), nullable=True, comment="")
    Kit_Type = Column(String(100), nullable=True, comment="")
    Catalog_Number = Column(String(100), nullable=True, comment="")
    Product_Name = Column(String(200), nullable=True, comment="")
    Detail_url = Column(String(1000), nullable=True, comment="")
    Tests = Column(String(200), nullable=True, comment="")
    Assay_type = Column(String(200), nullable=True, comment="")
    Detection_Method = Column(String(200), nullable=True, comment="")
    Sample_type = Column(String(1000), nullable=True, comment="")
    Assay_length = Column(String(200), nullable=True, comment="")
    Sensitivity = Column(String(200), nullable=True, comment="")
    Assay_range = Column(String(200), nullable=True, comment="")
    Specificity = Column(String(200), nullable=True, comment="")
    Target_Protein = Column(String(200), nullable=True, comment="")
    GeneId = Column(String(500), nullable=True, comment="")
    SwissProt = Column(String(500), nullable=True, comment="")
    DataSheet_URL = Column(String(500), nullable=True, comment="")
    Review = Column(String(50), nullable=True, comment="")
    Image_qty = Column(Integer, nullable=True, comment="")
    Citations = Column(String(20), nullable=True, comment="")
    Synonyms = Column(String(3000), nullable=True, comment="")
    Conjugate = Column(String(200), nullable=True, comment="")
    Species_Reactivity = Column(String(200), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Citations(Base):
    __tablename__ = "sigmaaldrich_kit_citations"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    PMID = Column(String(40), nullable=True, comment="")
    Species = Column(String(100), nullable=True, comment="")
    Article_title = Column(String(1000), nullable=True, comment="")
    Sample_type = Column(String(100), nullable=True, comment="")
    Pubmed_url = Column(String(1000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Images(Base):
    __tablename__ = "sigmaaldrich_kit_images"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    Image_url = Column(String(500), nullable=True, comment="")
    Image_description = Column(String(2000), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


class Price(Base):
    __tablename__ = "sigmaaldrich_kit_price"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    Catalog_Number = Column(String(40), nullable=True, comment="")
    sub_Catalog_Number = Column(String(40), nullable=True, comment="")
    Size = Column(String(50), nullable=True, comment="")
    Price = Column(String(50), nullable=True, comment="")
    Kit_Status = Column(String(20), nullable=True, comment="")
    Note = Column(String(500), nullable=True, comment="")
    Crawl_Date = Column(DateTime, server_default=func.now(), nullable=True, comment="")


# Mysql
engine = create_engine(
    "mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/bio_kit?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()
# Redis
pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=3)
r = redis.Redis(connection_pool=pool)


class SigmaAldrich(object):
    headers = {
        "Host": "www.sigmaaldrich.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.sigmaaldrich.com/life-science/cell-biology/antibodies/antibody-products.html?TablePage=13831609",
        "Connection": "keep-alive",
        "Cookie": "GUID=39bb3c4d-ec88-4413-b71e-148655a57693|NULL|1607650428002; TLTSID=C79074C43B4C103B07EDB10E232DB2CF; TLTUID=C79074C43B4C103B07EDB10E232DB2CF; dtCookie=v_4_srv_1_sn_98D1AD964724BB169DCB6CE1F56A71B6_perc_100000_ol_0_mul_1_app-3A1686b6dcc9c2ca9b_1; ak_bmsc=83A023EFD75F9D7A58C20C669D477F89~000000000000000000000000000000~YAAQnYzZF0WbcX51AQAAT4xVTwqSikpSsgYXNkqIj7h2OgiEmi3ck7bgM54nZlyzBLWXhbBmd8oJEB0ngd757DXTg7cf61/0eL1XwfC+FKZsSPHOc8hFxPMXDN5Ot5mS4nhTuym1orYqoCcSYH6ecUfR0ZW8QqGLFLg9eoQ2UkE4yM25dpz1IWT/qwc8mIqsxZN7KrVS28W2Y+yFRMGIw7HdTkuDHF1kuLu2ZmNkfnhqkHEGvcvBl2IcuJj9P0edPI4Ts6B5cZNRiUYWH5bU6tm1n+FFuVlPri10AatopZcvewCAoZ41EcwV/8KQ267W2Ul9X05AptUdf9jvfHJ/I0O1r2fBuH6in/LrRcn1zOzmYMSswp4wyPIi6jql2K7qHGQvME9S+WSliyXl71sZUXX2wGI4IpcsXI8Jf7vWUCaIYaS8+rCczGzgc/z/BtIejsVzwGLmpO/hGSQfk4g7h8U0464/hIQOy9BT/b+asdUAQ7ofbb6u; rxVisitor=1607648766018HS72MIK006LRFG89FQMQJ47VIQG1TKMC; dtPC=1$252257019_666h-vEOHMHVULMCJQCRUUWMPBFMAWMQUHOAFM-0e100; rxvt=1607655878979|1607648766021; dtLatC=203; bm_sv=89D770CCB02849A766BE50BA90F56609~ndHA06uAlqjqDnBG12qquuHenpiQldzwf4GcpWhHA8gKaCT1Ksun8Hrc0c+s3/eZqHWbqmG/e3XMAhoJ68jiWnDgK8qFvzp6vwVRn0TAPyKMptE+zOxiMPbuxfwzyPSZLwxhC8DOCSXiT7H4zC5Q5cErrgQjsraKgO1NJq+AJLY=; SessionPersistence=CLIENTCONTEXT%3A%3DvisitorId%253Danonymous%7CPROFILEDATA%3A%3Davatar%253D%252Fetc%252Fdesigns%252Fdefault%252Fimages%252Fsocial%252Favatar.png%252Cpath%253D%252Fhome%252Fusers%252Fa%252Fanonymous%252Fprofile%252CisLoggedIn%253Dfalse%252CisLoggedIn_xss%253Dfalse%252CauthorizableId%253Danonymous%252CauthorizableId_xss%253Danonymous%252CformattedName%253Danonymous%252CformattedName_xss%253Danonymous; _gcl_au=1.1.1188192576.1607648774; UrCapture=bb6d4d9a-9a22-9572-ac7f-6914f1b2bbf7; _4c_=jVPLbts6EP2VQouuQpvvR4DiIlHSIoCbLhzfLgNSpGwhiiTQctS0Sb79DuVXm6TA3QicM2eOhmeGv7JhFZrslEispOBYU6HZSXYXHtfZ6a%2Bs6NL3IX02sc5Os1Xfd%2BvT6XQYhsm6Wt5bW%2FtYFatJ0d5PC9vbul1Ou9j6TdFPR8I0Wocx0f%2FUtll%2BCs3HGJZV23zKz7KTLDSgnXXRw7lofYCAmAmhEwJA%2FxNCrjEcd4q3%2FWOXOENwH9b%2BDhI%2BPFRFuB0q36%2FGYoqP6CpUy1WfYDyq%2BC6mAE5D1fh2OJZhjo%2FoocxwDmjebpo%2BPkKcn11fJqCuQtNfXQAC0ZfFeGLGOVZwj0KhNeKcMOQUCYhwLYWwQknDnq4Xs9nT1mrMqcaYgsDN7GY%2BSuTKYMVzzs55TjA7x%2Bry4pzgS8roxTnNP2%2B5i%2F%2FL%2FRruXYj7NueVrWdtYetwEcoksL1VDqa%2F5GdP34ObwXxeEHkCcjpu7DJZjZJdi5jbrt%2FEBDgnPffGImMpRUYoimyhSiQN4SVx1LlSQcm%2FNla2SS5eXc8v5%2FOrb9eAutgO65Cm8LmKoWx%2FfNDJghaWLfs%2Bur%2BGEDIhxpEG0brq039fb9suA5v6KpnWJW0VScr1eGWIYMmhIBQ97B6E2qTRnd1u3SSaUaKUFnIyTodrjcnRQg%2BURXPXtEMSuYnVchni19Cv2pS5idZXSdbWaceSegw%2BQEuJ7ZMFEN31bXeAn0%2ByH9s3pzjGmFFBYN17eGBaJgBjYMTK7x5fpm1hhaASmUJIxI0lSJfYISqZZ8aaQGCmO00iOOVKMaMJiDxUew1qHSt5kIiVwiDOLAwPWEgR5gqJiSKWZYe%2BYGehESPEri%2Bi92119U6RHMmSEbiIlnsyP1yie9iz6fHOTHMhFVVv77zdD4q0QaE51MrXpQz%2FvdRo9HP1bqlgjAD9balPL2LrErHUUcsdKoL0iGsrkQ2mRMEwLygxmiqZvWnnnemlPdhK%2FrYMhwFpLKSmHHhV1%2B8dGm2nWICV5E9uQhJ3L2mzd%2FNxOEhtExJGqP6gjsjz8%2FN%2F; country=CANE; SialLocaleDef=CountryCode~CA|WebLang~-1|; _vwo_uuid_v2=D9F14EFA7C14BB789B0D4C646394DE53A|baef96d5c21d9343cbb4b1cb4a28a0c7; fpestid=7X0JNTkKcG7aKrXzTX874ZhZ2hL-DAsjtGmj9C2_y7nUGU9W76ZlgsNzJ4-5mgE4J-ie0g; _vwo_ssm=1; _vis_opt_s=1%7C; _vis_opt_test_cookie=1; _vwo_uuid=D9F14EFA7C14BB789B0D4C646394DE53A; _vwo_ds=3%3Aa_1%2Ct_1%3A0%241607648795%3A95.9259337%3A%3A45_1%2C44_1%2C39_1%2C38_1%2C34_1%2C21_1%2C23_1%2C22_1%3A68_1%2C2_1%3A0; JabmoSP0ses.5e8a=*; JabmoSP0id.5e8a=aa6b58a1-80f2-4870-8afa-2faf1d7031a9.1607648800.1.1607654074.1607648800.208f16eb-5ad9-446e-b0a5-b4f128036777; _ga=GA1.2.1832177856.1607648801; _gid=GA1.2.1253403032.1607648801; _wingify_pc_uuid=d51adac84a284c74b8608bf5a40ab8cd; wingify_donot_track_actions=0; _ju_dm=cookie; _ju_dn=1; _fbp=fb.1.1607648915206.1905997085; _ju_dc=65872b84-3b4d-11eb-9fa5-37e8fc1c5d2c; _ju_pn=18; JSESSIONID=812F5893D8DC9139FE8B8DB07128873A.stltcat12a; visid_incap_1614561=tMNd4XJQRdyu4r5N5CHg0n7M0l8AAAAAQUIPAAAAAAD2OzNSq5nw6f2cTBxRA7nF; incap_ses_1047_1614561=jmUIdnQrwESE/Vd/G7GHDn/M0l8AAAAAriYlgvqd3LyjAKxQqqLmFg==; incap_ses_1204_1614561=DHJBKnO6onMRSTmNxne1EH7M0l8AAAAA7nBX067stkuthxsy3djf9g==; incap_ses_573_1614561=JYsGRWf+Dgjn3g68p7TzB/zQ0l8AAAAAFDiFbQBhb3m9IYkGPe4gqw==; incap_ses_810_1614561=9B5QdgjMaRGP9hlJ37I9C7LT0l8AAAAAonDK5lWSfBCL0CbSNXf3Gw==; incap_ses_577_1614561=y06LN7hk2l78fjGjpOoBCADR0l8AAAAAENwKDfLp2cIAjW+CwTnW1Q==; incap_ses_1044_1614561=qo7LfvE65GvEqlQ7pAh9Dv7Q0l8AAAAAPjIK3nK4nOXvMscgudKEsA==; incap_ses_1206_1614561=sZHwXzO5ZE1TUiTXw5K8EP7Q0l8AAAAAhyVdBoQvUalCyJ7XPNADuw==; incap_ses_795_1614561=V4kiApiGlR/roz6hc2gIC/7Q0l8AAAAASOtgiXDscEZE9D2nx32xTw==; incap_ses_798_1614561=to28AZUTdjKKYPSO7xATC//Q0l8AAAAAr4fkOX+63aKv0ybgbS4geg==; incap_ses_572_1614561=Kt2gQsvWCDDvZI6/JifwBwDR0l8AAAAAV6t4rApdeUGjgup6vc7IhA==; incap_ses_570_1614561=Ppq8VaHxrzh6b451KQzpBwHR0l8AAAAAKs5AkzVigsDNMiG2Padtwg==; incap_ses_1045_1614561=TarKVYhLmRyqoNqKIJaADgDR0l8AAAAAewX4YhWqWxKOxJkmQKRkYg==; incap_ses_574_1614561=Ny3jTBwHo1TlHv5hJkL3BwDR0l8AAAAAlN5ErnGh0aCWoE29RJLDPQ==; incap_ses_265_1614561=R0Q2bdUXcnDIcw+xinitAwDR0l8AAAAAnDkuT137vN+sFEjrbP+3iA==; incap_ses_627_1614561=W2XAA1a/ylI/UP7PYI2zCP/Q0l8AAAAAOdzkS+nckFaRWz0ZbwaWig==; nlbi_1614561_1918600=s3arW1QxkhNFKb4SL3uB3QAAAABghHIkhgZ09Fq5iYBALZx/; incap_ses_576_1614561=5KIuNn8d0WEJUH3+JV3+BwDR0l8AAAAA6uxyAcSKp/Ae/vGkrtVbbg==; _vis_opt_exp_281_combi=2; _vis_opt_exp_285_combi=3; incap_ses_431_1614561=lEq9YmxLK1Y2HQRSaDj7BaLT0l8AAAAAuCBSY+Boyxr/RseinSmm7A==; incap_ses_256_1614561=Sqk5HnkJuFhm9layzn6NA7LT0l8AAAAAX0uCwhSwZVYP49z7pdsgog==; _ju_v=4.1_3.67; _uetsid=105118403b4d11ebbf56690ae27d9a88; _uetvid=105171603b4d11eba5b97740effa56c5; _gali=two_column; _gat_UA-51006100-1=1",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    def format(self, url):
        with requests.Session() as s:
            # resp = s.get(url=url, headers=self.headers, timeout=240)
            resp = s.get(url=url)
            html_lxml = etree.HTML(resp.text)
            content = html_lxml.xpath('//div[@id="contentWrapper"]/div')[
                0
            ]  # ! <xpath lxml>  None
        return content

    def brand(self):
        return "Sigma-Aldrich"

    def kit_type(self):
        return "elisa kit"

    def catalog_number(self, html):
        catnum = html.xpath('.//strong[@itemprop="productID"]/text()')[0].strip()
        return catnum

    def product_name(self, html):
        try:
            name_list = html.xpath('.//h1[@itemprop="name"]//text()')
            name = "".join(i for i in name_list).strip()
        except Exception:
            name = None
        return name

    def detail_url(self, url):
        return url

    def tests(self, html):  # 48T/96T 使用次数 规格
        try:
            tests = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"packaging")]/following-sibling::td[@class="rgt"]/text()'
            )[0].strip()
        except Exception:
            return None
        return tests

    def assay_type(self, html):
        return None

    def detection_method(self, html):
        try:
            detection_method = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"detection method")]/following-sibling::td[@class="rgt"]/text()'
            )[0].strip()
        except Exception:
            return None
        return detection_method

    def sample_type(self, html):
        # Suitable Sample:
        try:
            sample_type = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"input")]/following-sibling::td[@class="rgt"]/text()'
            )[0].strip()
        except Exception:
            return None
        return sample_type

    def assay_length(self, html):
        return None

    def sensitivity(self, html):
        try:
            sensitivity_text = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"assay range")]/following-sibling::td[@class="rgt"]/text()'
            )
            sensitivity = "".join(
                i for i in sensitivity_text if "sensitivity:" in i
            ).strip()
            if len(sensitivity) > 0:
                return sensitivity.split("sensitivity:")[-1]
            else:
                return None
        except Exception:
            return None

    def assay_range(self, html):
        # Calibration Range:
        try:
            assay_range_text = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"assay range")]/following-sibling::td[@class="rgt"]/text()'
            )

            assay_range = "".join(
                i for i in assay_range_text if "standard curve range:" in i
            ).strip()
            if len(assay_range) > 0:
                return assay_range.split("standard curve range:")[-1]
            else:
                return None
        except Exception:
            return None

    def specificity(self, html):
        try:
            specificity = html.xpath(
                './/h4[contains(text(), "Application")]/following-sibling::p[1]/text()'
            )[0].strip()
        except Exception:
            return None
        return specificity

    def target_protein(self):
        return None

    def geneid(self, html):
        try:
            geneid_text = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"Gene Information")]/following-sibling::td[@class="rgt"]//text()'
            )
            results = []
            for item in geneid_text:
                if "(" and ")" in item:
                    gid = item.split("(")[1].split(")")[0]
                    results.append(gid)
            geneid = ",".join(i for i in results)
            if len(geneid) > 0:
                return geneid
            else:
                return None
        except Exception:
            return None

    def swissprot(self, html):
        return None

    def datasheet_url(self, html):
        try:
            datasheet_url = (
                "https://www.sigmaaldrich.com"
                + html.xpath('.//a[contains(text(), "Bulletin (PDF)")]/@href')[
                    0
                ].strip()
            )
        except Exception:
            return None
        return datasheet_url

    def review(self, html):
        try:
            review = int(
                html.xpath(
                    './/h2[@class="greyHead2"][contains(text(), "Reviews for")]/span/text()'
                )[0]
                .strip()
                .split("(")[-1]
                .split(")")[0]
            )
        except Exception:
            review = 0
        return review

    def image_qty(self, html):
        try:
            image_qty = len(
                html.xpath(
                    './/div[@class="image prodImage"]//img[not(contains(@class,"portfolio-brand-placeholder"))]'
                )
            )
        except Exception:
            image_qty = 0
        return image_qty

    def citations(self, html):
        return None

    def synonyms(self, html):
        try:
            synonyms = html.xpath(
                './/b[contains(text(), "Gene Alias:")]/../following-sibling::li[@class="black11_a_underline sub_content"]/text()'
            )[0].strip()
        except Exception:
            synonyms = None
        return synonyms

    def conjugate(self, html):
        try:
            conjugate = html.xpath(
                './/b[contains(text(), "Label:")]/../following-sibling::li[@class="black11_a_underline sub_content"]/text()'
            )[0].strip()
        except Exception:
            conjugate = None
        return conjugate

    def species_reactivity(self, html):
        try:
            species_reactivity = html.xpath(
                './/tr/td[@class="lft"][contains(text(),"species reactivity")]/following-sibling::td[@class="rgt"]/text()'
            )[0].strip()
        except Exception:
            return None
        return species_reactivity

    def note(self, html):
        return None

    # ======================================================================== #
    # Citations表
    def sub_citations(self, citations, html):
        results = []
        if citations == 0:
            return results
        else:
            trs = html.xpath(
                './/table[@id="publications_list_table"]/tbody/tr[@class="lined revfil firstten" or @class="lined revfil hidden"]'
            )
            for tr in trs:
                pm_url = tr.xpath("./td/a/@href")[0].strip()
                if "www.ncbi.nlm.nih.gov/pubmed/" in pm_url:
                    pmid = pm_url.split("gov/pubmed/")[1].strip()
                else:
                    pmid = None
                species = tr.xpath("./@data-species")[0].strip()
                title_list = tr.xpath("./td/a//text()")
                title = "".join(i for i in title_list)
                results.append([pmid, title, pm_url, species])
        return results

    # ======================================================================== #
    # Images表
    def sub_images(self, image_qty, html):
        results = []
        if image_qty == 0:
            return results
        else:
            imgs = html.xpath(
                './/div[@class="image prodImage"]//img[not(contains(@class,"portfolio-brand-placeholder"))]'
            )
            for img in imgs:
                img_des = ",".join(i for i in img.xpath("./@alt")).strip()
                if len(img_des) == 0:
                    img_des = None
                try:
                    img_url = (
                        "https://www.sigmaaldrich.com" + img.xpath("./@src")[0].strip()
                    )
                    results.append([img_url, img_des])
                except Exception:
                    pass
        return results

    # ======================================================================== #
    # Price
    def sub_price(self, html):
        results = []
        try:
            trs = html.xpath('.//li[@class="add-to-cart"]//tbody/tr[@class="odd"]')
        except Exception:
            return results
        for tr in trs:
            sub_cata = tr.xpath('./td/div[@class="atc_catnum"]/text()')[0].strip()
            sub_size_list = tr.xpath('./td/div[@class="atc_size"]//text()')
            sub_size = "".join(i for i in sub_size_list).strip()
            results.append([sub_cata, sub_size])
        return results


if __name__ == "__main__":
    # for i in range(1):
    while r.exists("sigmaaldrich_detail"):
        # single_data = r.rpop("sigmaaldrich_detail")
        # extract = single_data.split(",")[0]
        # catano = single_data.split(",")[1]
        extract = r.rpop("sigmaaldrich_detail")
        # extract = "https://www.sigmaaldrich.com/catalog/product/sigma/rab0710?lang=en&region=CA"
        print(extract)
        try:
            lxml = SigmaAldrich().format(extract)
        except Exception as e:
            print(e)
            r.lpush("sigmaaldrich_detail", extract)
            time.sleep(30)
            print("sleeping...")
            continue
        if lxml is not None:
            brand = SigmaAldrich().brand()
            kit_type = SigmaAldrich().kit_type()
            catalog_number = SigmaAldrich().catalog_number(lxml)
            product_name = SigmaAldrich().product_name(lxml)
            detail_url = SigmaAldrich().detail_url(extract)
            tests = SigmaAldrich().tests(lxml)
            # assay_type = SigmaAldrich().assay_type(lxml)
            detection_method = SigmaAldrich().detection_method(lxml)
            sample_type = SigmaAldrich().sample_type(lxml)
            # assay_length = SigmaAldrich().assay_length(lxml)

            sensitivity = SigmaAldrich().sensitivity(lxml)  # ! 里面包含两个字段，需处理
            assay_range = SigmaAldrich().assay_range(lxml)  # ! 里面包含两个字段，需处理

            specificity = SigmaAldrich().specificity(lxml)
            # target_protein = SigmaAldrich().target_protein(lxml)
            geneid = SigmaAldrich().geneid(lxml)
            # swissprot = SigmaAldrich().swissprot(lxml)
            datasheet_url = SigmaAldrich().datasheet_url(lxml)
            # review = SigmaAldrich().review(lxml)
            image_qty = SigmaAldrich().image_qty(lxml)
            # citations = SigmaAldrich().citations(lxml)
            # synonyms = SigmaAldrich().synonyms(lxml)
            # conjugate = SigmaAldrich().conjugate(lxml)
            species_reactivity = SigmaAldrich().species_reactivity(lxml)
            # note = SigmaAldrich().note(lxml)
            # sub_citations = SigmaAldrich().sub_citations(citations, lxml)
            sub_images = SigmaAldrich().sub_images(image_qty, lxml)
            # sub_price = SigmaAldrich().sub_price(lxml)

        else:
            r.lpush("sigmaaldrich_detail", extract)
            print("html is none")
            continue
        new_detail = Detail(
            Brand=brand,
            Kit_Type=kit_type,
            Catalog_Number=catalog_number,
            Product_Name=product_name,
            Detail_url=detail_url,
            Tests=tests,
            # Assay_type=assay_type,
            Detection_Method=detection_method,
            Sample_type=sample_type,
            # Assay_length=assay_length,
            Sensitivity=sensitivity,
            Assay_range=assay_range,
            Specificity=specificity,
            # target_protein=target_protein,
            GeneId=geneid,
            # SwissProt=swissprot,
            DataSheet_URL=datasheet_url,
            # Review=str(review),
            Image_qty=image_qty,
            # Citations=citations,
            # Synonyms=synonyms,
            # Conjugate=conjugate,
            Species_Reactivity=species_reactivity,
            # Note=note,
        )
        session.add(new_detail)

        # if sub_citations:
        #     objects_sub_citations = []
        #     for sub in sub_citations:
        #         sub_pid = sub[0]
        #         sub_tit = sub[1]
        #         sub_pul = sub[2]
        #         sub_species = sub[3]

        #         new_citations = Citations(
        #             Catalog_Number=catalog_number,
        #             PMID=sub_pid,
        #             Article_title=sub_tit,
        #             Pubmed_url=sub_pul,
        #             Species=sub_species,
        #         )
        #         objects_sub_citations.append(new_citations)
        #     session.bulk_save_objects(objects_sub_citations)

        if sub_images:
            objects_sub_images = []
            for sub in sub_images:
                img = sub[0]
                des = sub[1]

                new_images = Images(
                    Catalog_Number=catalog_number, Image_url=img, Image_description=des
                )
                objects_sub_images.append(new_images)
            session.bulk_save_objects(objects_sub_images)

        # if sub_price:
        #     objects_sub_price = []
        #     for sub in sub_price:
        #         sub_c = sub[0]
        #         suc_s = sub[1]

        #         new_price = Price(
        #             Catalog_Number=catalog_number, sub_Catalog_Number=sub_c, Size=suc_s
        #         )
        #         objects_sub_price.append(new_price)
        #     session.bulk_save_objects(objects_sub_price)

        try:
            session.commit()
            session.close()
            print("done")
        except Exception as e:
            r.lpush("sigmaaldrich_detail", extract)
            session.rollback()
            print(e)
        time.sleep(random.uniform(1.0, 1.5))
