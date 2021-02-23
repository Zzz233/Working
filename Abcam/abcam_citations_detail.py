# Author: HuangWei Time:2020/5/9
# Author: HuangWei Time:2020/1/18

import re
import requests
import threading
from queue import Queue
from Mysql_helper import MYSQL
from utils.Random_UserAgent import get_request_headers
import time
import random
import pymysql
from lxml import etree

# 用来分析文章作者和机构的代码

count = 0


def get_no_none_list(pre_list):
    po_list = [x.strip() for x in pre_list if x.strip() != '']
    #po_list.remove('see all')
    if 'see all' in po_list:
        po_list.remove('see all')
    return po_list


def get_string(strings, x):
    if len(strings) > x:
        first_str = strings[0:x-1]

    else:
        first_str = strings
    return first_str


def get_first_item(list, y):
    if not list or len(list[0].strip()) > y:
        first_item = ''
    else:
        first_item = list[0].strip()
    return first_item


class Producer (threading.Thread):

    def __init__(self, pmid_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.pmid_queue = pmid_queue
        self.mysql = MYSQL('antibodies_info')

    def run(self):
        while True:
            if self.pmid_queue.empty():
                break
            pmid = self.pmid_queue.get()
            self.parse_page(pmid)
            time.sleep(random.uniform(0.5, 2))

    def parse_page(self, pmid):
        global count
        try:
            url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={}&mode=xml&_=1572314980717'.format(pmid)
            r = requests.get(url, headers=get_request_headers(), timeout=10)
            r.raise_for_status()
        except Exception as e:
            self.pmid_queue.put(pmid)
            print('访问抗体列表页失败，将{}放回队列'.format(pmid))
            print(e)
        else:
            html = r.text
            #pmid_list = re.findall('<PMID Version="1">(.*?)</PMID>', html, re.S)
            authors = re.findall('<LastName>(.*?)</LastName>.*?<ForeName>(.*?)</ForeName>', html, re.S)
            tab = []
            if not authors:
                authors = [('', '')]
            for i in range(len(authors)):
                a = authors[i][0] + ' ' + authors[i][1]
                tab.append(a)
            author = ', '.join(tab)
            pubdate = re.findall('<PubDate>.*?<Year>(.*?)</Year>.*?<Month>(.*?)</Month>.*?<Day>(.*?)</Day>',
                                 html, re.S)
            if not pubdate:
                pubdate = [('', '', '')]
            for l in range(len(pubdate)):
                pubdatetime = pubdate[l][0] + '-' + pubdate[l][1] + '-' + pubdate[l][2]
            institution = re.findall('<Affiliation>(.*?)</Affiliation>', html, re.S)
            if not institution:
                institution = ['']
            journal_list = re.findall('<Title>(.*?)</Title>', html, re.S)
            journal = get_first_item(journal_list, 100)
            try:
                insert_sql = 'insert into abcam_citations_details (pmid, journal, pub_date, institution, author) values ("{}","{}","{}","{}","{}");'.format(pmid, journal, pubdatetime, institution[0], author)
                self.mysql.insert_into_table(insert_sql)
            except Exception as es:
                print(es)
                print(url)
            else:
                update_sql = 'update abcam_pmid set pmid_Status = "1" where  pmid = {};'.format(pmid)
                self.mysql.insert_into_table(update_sql)
                count += 1
                print("\r获得citation详情页进度: %d" % count, end="")


def main():
    mysql_pmid = MYSQL('antibodies_info')
    pmid_queue = Queue(200000)
    pmids = mysql_pmid.show_all('select pmid from abcam_pmid WHERE pmid_Status = "0"')
    for pmid in pmids:
        pmid_queue.put(pmid[0])
        #print(pmid[0])
    print(pmid_queue.qsize())
    for x in range(7):
        t = Producer(pmid_queue)
        t.start()


if __name__ == '__main__':
    main()

