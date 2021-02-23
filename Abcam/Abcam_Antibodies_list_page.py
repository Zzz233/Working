# Author: HuangWei Time:2019/8/22
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

count = 0


def gethtml(url):
    i = 0
    while i < 5:
        try:
            html = requests.get(url, headers=get_request_headers(), timeout=5)
            time.sleep(random.uniform(2, 3))
            return html
        except requests.exceptions.RequestException:
            i += 1


class Producer (threading.Thread):
    headers = {
        'Cookies': 'PP=1; _ga=GA1.2.1635461879.1565096809; _gid=GA1.2.133744421.1565496241; Hm_lvt_30fac56dd55db0f4c94ac3995'
                   '5a1d1f1=1565268961,1565389179,1565496240,1565510325; Qs_lvt_186141=1565096808%2C1565268960%2C1565389178%2C'
                   '1565496240%2C1565510324; _sp_ses.4591=*; mediav=%7B%22eid%22%3A%2295155%22%2C%22ep%22%3A%22%22%2C%22vid%22%'
                   '3A%22-XCl%25T1HEC%3A4I9xGrX(9%22%2C%22ctn%22%3A%22%22%7D; _gat_UA-367099-9=1; _dc_gtm_UA-367099-9=1; C2LC=CN'
                   '; JSESSIONID=C88C68C292A8E0EF6D45F465F0D12E1C.Pub1; Hm_lpvt_30fac56dd55db0f4c94ac39955a1d1f1=1565510331; Qs_p'
                   'v_186141=3753158417549899000%2C4277747906829928000%2C349668107189188000%2C3100574197771999000%2C23251166996415'
                   '81000; _sp_id.4591=0c85e63041a8e8b7.1565096808.6.1565510332.1565501036.a1a460db-b738-431c-9eb1-7f0dc0ed348b',
        'DPR': '1',
        'Host': 'www.abcam.cn',
        'Referer': 'https://www.abcam.cn/products?selected.productType=Primary+antibodies',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 '
                      'Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'close'
    }

    def __init__(self, pageurl_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.pageurl_queue = pageurl_queue
        self.mysql = MYSQL('new_antibodies_info')

    def run(self):
        while True:
            if self.pageurl_queue.empty():
                break
            url = self.pageurl_queue.get()
            self.parse_page(url)
            time.sleep(random.uniform(1, 3))

    def parse_page(self, url):
        global count
        try:
            r = requests.get(url, headers=self.headers)
            #r = gethtml(url)
        except Exception as e:
            self.pageurl_queue.put(url)
            print('访问抗体列表页失败，尝试其他代理')
            print(e)
        else:
            html = r.text
            element = etree.HTML(html)
            # print(html)
            divs = element.xpath('.//div[@class="pws-item-info"]')
            try:
                for div in divs:
                    anti_name = div.xpath('.//h3/a/text()')[0]
                    anti_catanum = div.xpath('.//h3/a/span/text()')[0]
                    href_list = div.xpath('.//h3/a/@href')
                    href = 'https://www.abcam.cn/' + href_list[0]
                    # print(anti_name, anti_catanum, href)
                    sql = 'insert into Abcam_Antibody_list (Catalog_Number, Product_Name, Antibody_detail_URL)' \
                          'values("{}","{}","{}");'.format(anti_catanum, anti_name, href)
                    self.mysql.insert_into_table(sql)
            except Exception as es:
                print(url)
                print(es)
            else:
                update_status_sql = 'update Abcam_Antibody_list_url set Antibody_Status = "1" where Antibody_list_URL  = "%s";' % url
                self.mysql.insert_into_table(update_status_sql)
                count += 1
                print("\r获得抗体详情页进度: %d" % count, end="")



def main():
    mysql_antibody_list_url = MYSQL('new_antibodies_info')
    pageurl_queue = Queue(10000)
    antibody_list_urls = mysql_antibody_list_url.show_all('select Antibody_list_URL from Abcam_Antibody_list_url where Antibody_Status = "0";')
    for antibody_list_url in antibody_list_urls:
        # print(antibody_list_url[0])
        pageurl_queue.put(antibody_list_url[0])
    print(pageurl_queue.qsize())
    for x in range(1):
        t = Producer(pageurl_queue)
        t.start()


if __name__ == '__main__':
    main()

