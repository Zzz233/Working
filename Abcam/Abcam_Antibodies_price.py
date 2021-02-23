
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
import json


count = 0


def get_first_item(list, y):
    if not list or len(list[0].strip()) > y:
        first_item = ''
    else:
        first_item = list[0].strip()
    return first_item


class Producer (threading.Thread):

    def __init__(self, antibody_url_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.antibody_url_queue = antibody_url_queue
        self.mysql = MYSQL('new_antibodies_info')

    def run(self):
        while True:
            if self.antibody_url_queue.empty():
                break
            url = self.antibody_url_queue.get()
            self.parse_page(url)
            time.sleep(random.uniform(1.5, 2.5))

    def parse_page(self, url):
        global count
        try:
            r = requests.get(url, headers=get_request_headers(), timeout=10)
            r.raise_for_status()
        except Exception as e:
            self.antibody_url_queue.put(url)
            print('访问抗体列表页失败，将url放回队列')
            print(e)
        else:
            html = r.text

            try:
                catanum_list = re.findall('"ProductCode":"(.*?)"', html, re.S)
                catanum = get_first_item(catanum_list, 40)

                size_price_list = re.findall('"Size":"(.*?)".*?"Price":"(.*?)"', html, re.S)
                for size_price in size_price_list:
                    size = size_price[0]
                    price = size_price[1]
                    price_sql ='insert into Abcam_Antibody_price(Catalog_Number,Size,Price) values("{}","{}","{}");'.format(catanum, size, price)
                    self.mysql.insert_into_table(price_sql)

                # data = json.loads(html)
                # for i in data:
                #     title = i['Title']
                #     pmid = i['PubmedID']
                #     application = i['ApplicationsShortName']
                #     species = i['Species']
                #     citation_sql = 'insert into Abcam_Antibody_citations (Catalog_Number, PMID , Application, Species, Article_title) values ("{}","{}","{}","{}","{}");'.format(
                #         catanum, pmid, application, species, pymysql.escape_string(title))
                #     self.mysql.insert_into_table(citation_sql)

            except Exception as es:
                print(es)
                print(url)

            else:
                update_status_sql = 'update Abcam_Antibody_detail set Price_Status = "1" where Price_url = "%s";' % url
                self.mysql.insert_into_table(update_status_sql)
                count += 1
                print("\r获得抗体详情页进度: %d" % count, end="")


def main():
    mysql_antibody_url = MYSQL('new_antibodies_info')
    antibody_url_queue = Queue(200000)
    antibody_urls = mysql_antibody_url.show_all('select Price_url from Abcam_Antibody_detail where Price_url  != "" '
                                                'and Price_Status = "0";')
    for antibody_url in antibody_urls:
        antibody_url_queue.put(antibody_url[0])
        # print(antibody_url[0])
    print(antibody_url_queue.qsize())
    for x in range(4):
        t = Producer(antibody_url_queue)
        t.start()


if __name__ == '__main__':
    main()

