
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

# 有十几条数据没有取，是不可卖的（取抗体名称时没有，则为不可卖的抗体），取到的数据种中，modify和swisprot字段有脏数据，需要处理


def get_string(strings, x):
    if len(strings) > x:
        first_str = ''

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
            time.sleep(random.uniform(2.5, 4.5))

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
            element = etree.HTML(html)
            try:
                antiname_list = re.findall('产品名称</h3>.*?<div class="value">(.*?)<', html, re.S)
                antiname = antiname_list[0]
                cataNo_list = re.findall('data-product-code="(.*?)">', html, re.S)
                cataNo = cataNo_list[0]
                antibody_type_list = re.findall('克隆</h3>.*?<div class="value">(.*?)</div>', html, re.S)
                antibody_type = get_first_item(antibody_type_list, 30)
                lis = element.xpath('.//li[@class="attribute list"]')
                if lis:
                    li = lis[-1]
                    synonyms_list = li.xpath('.//li/text()')
                    synonyms = ','.join(synonyms_list)
                    synonyms = get_string(synonyms, 3000)
                else:
                    synonyms = ''
                # 取指定文本标签的同级标签
                divs = element.xpath('.//h3[.="经测试应用"]/following-sibling::div')
                if divs:
                    div = divs[0]
                    application_lists = div.xpath('./abbr/text()')
                    application = ','.join(application_lists)
                else:
                    application = ''
                conjugation_list = re.findall('<li>Conjugation: (.*?)</li>', html, re.S)
                conjugation = get_first_item(conjugation_list, 200)
                clone_number_list = re.findall('克隆编号</h3>.*?div class="value">(.*?)</div>', html, re.S)
                clone_number = get_first_item(clone_number_list, 40)
                recombinant_list = re.findall('product-label product-label--recombinant">(.*?)</div>', html, re.S)
                if recombinant_list:  # 判断是否有敲除验证
                    recombinant = "yes"
                else:
                    recombinant = ''
                modify_list = re.findall('(描述</h3>.*?</div>)', html, re.S)
                if modify_list:
                    modify_list = re.findall('描述</h3>.*?\((.*?)\)', modify_list[0], re.S)
                    modify = get_first_item(modify_list, 200)
                else:
                    modify = ''
                host_list = re.findall('宿主</h3>.*?<div class="value">(.*?)</div>', html, re.S)
                host = get_first_item(host_list, 200)
                species_reactivity_list = re.findall('种属反应性</h3>.*?</strong>(.*?)<', html, re.S)
                species_reactivity = get_first_item(species_reactivity_list, 1000)
                abid = re.findall('data-track-value="(.*?)"', html, re.S)
                if abid:
                    price_url = 'https://www.abcam.cn/datasheetproperties/availability?abId=' + abid[0]
                    sellable = 'yes'
                else:
                    price_url = ''
                    sellable = 'no'
                geneid_list = re.findall('Entrez Gene:(.*?)</li>', html, re.S)
                if geneid_list:
                    geneid_list = list(map(lambda geneid: geneid.strip(), geneid_list))
                    geneid = ','.join(geneid_list)
                    if len(geneid) > 499:
                        geneid = geneid[0:480]
                else:
                    geneid = ''
                siRNA_list = re.findall('alt="使用(.*?)细胞株进行验证', html, re.S)
                if siRNA_list:  # 判断是否有敲除验证
                    siRNA = "yes"
                else:
                    siRNA = ''
                swisprot_list = re.findall('SwissProt:(.*?)</li>', html, re.S)
                if swisprot_list:
                    swisprot_list = list(map(lambda swisprot: swisprot.strip(), swisprot_list))
                    swisprot = ','.join(swisprot_list)
                    if len(swisprot) > 499:
                        swisprot = swisprot[0:480]
                else:
                    swisprot = ''
                predicted_mw_list = re.findall('Predicted band size:</b>(.*?)<br>', html, re.S)
                predicted_mw = get_first_item(predicted_mw_list, 200)
                observed_mw_list = re.findall('Observed band size:</b> (.*?)<', html, re.S)
                observed_mw = get_first_item(observed_mw_list, 200)
                isotype_list = re.findall('同种型</h3>.*?div class="value">(.*?)</div>', html, re.S)
                isotype = get_first_item(isotype_list, 100)
                citations_list = re.findall('被引用在 (.*?)文献中', html, re.S)
                citations = get_first_item(citations_list, 100)
                if citations_list:
                    reference_url = 'https://www.abcam.cn/DatasheetProperties/References?productcode=' + cataNo_list[0]
                else:
                    reference_url = ''
                pdf_url_list = re.findall('class="pdf-links">.*?<li><a target="_blank" href="(.*?)"', html, re.S)
                if pdf_url_list:
                    pdf_url = 'https://www.abcam.cn' + get_first_item(pdf_url_list, 300)
                else:
                    pdf_url = ''
                review_list = re.findall('"reviewCount": "(.*?)"', html, re.S)
                review = get_first_item(review_list, 100)
                lis = element.xpath('//*[@id="description_images"]/div[2]/ul/li')
                image_qty = len(lis)
                # 在此处写入检测图片
                if lis:
                    for li in lis:
                        image_url_list = li.xpath('./div/a/@href')
                        image_url = get_first_item(image_url_list, 500)
                        description_list = li.xpath('./div[1]/div/div//text()')
                        description = get_first_item(description_list, 1000)
                        image_sql = 'insert into Abcam_Antibody_images (Catalog_Number, Image_url, Image_description) values ("{}","{}","{}");'.format(
                            cataNo, image_url, description)
                        self.mysql.insert_into_table(image_sql)
                # 在此处写入应用信息
                trs = element.xpath('//*[@id="description_applications"]/div[2]/table/tbody/tr')
                if trs:
                    for tr in trs:
                        application_list = tr.xpath('./td[1]//text()')
                        application2 = get_first_item(application_list, 200)
                        dillution_list = tr.xpath('./td[3]//text()')
                        dillution = get_first_item(dillution_list, 1000)
                        application_sql = 'insert into Abcam_Antibody_application (Catalog_Number, Application, Dilution) values ("{}","{}","{}");'.format(cataNo, application2, dillution)
                        self.mysql.insert_into_table(application_sql)
                detail_sql = 'insert into Abcam_Antibody_detail(Sellable,Catalog_Number, Product_Name, Antibody_Type, Synonyms, Application, Conjugated, Clone_Number, Recombinant_Antibody, Modified, Host_Species, Antibody_detail_URL, GeneId, KO_Validation, Species_Reactivity, SwissProt,Predicted_MW,Observed_MW,Isotype,Citations,Citations_url,DataSheet_URL,Review,Price_url,Image_qty) values ("{}", "{}", "{}","{}", ' \
                             '"{}","{}","{}","{}","{}", "{}", "{}","{}", "{}", "{}","{}", "{}","{}","{}", "{}", "{}","{}", "{}", "{}","{}", "{}");'.format(sellable,cataNo, antiname,antibody_type,synonyms,application,conjugation,clone_number,recombinant,modify,host,url,pymysql.escape_string(geneid),siRNA,species_reactivity,pymysql.escape_string(swisprot),predicted_mw,observed_mw,isotype,citations,reference_url,pdf_url,review,price_url,image_qty)
                self.mysql.insert_into_table(detail_sql)

            except Exception as es:
                print(es)
                print(url)

            else:
                update_status_sql = 'update Abcam_Antibody_list set Antibody_Status = "1" where  Antibody_detail_URL = "%s";' % url
                self.mysql.insert_into_table(update_status_sql)
                count += 1
                print("\r获得抗体详情页进度: %d" % count, end="")


def main():
    mysql_antibody_url = MYSQL('new_antibodies_info')
    antibody_url_queue = Queue(200000)
    antibody_urls = mysql_antibody_url.show_all('select Antibody_detail_URL from Abcam_Antibody_list where Antibody_detail_URL  != "" '
                                                'and Antibody_Status = "0";')
    for antibody_url in antibody_urls:
        antibody_url_queue.put(antibody_url[0])
        # print(antibody_url[0])
    print(antibody_url_queue.qsize())
    for x in range(1):
        t = Producer(antibody_url_queue)
        t.start()


if __name__ == '__main__':
    main()

