import requests
from lxml import etree
from sqlalchemy_sql import List
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'PHPSESSID=tcckccj6644bk5r5am82e3apo1; SY=9hscuhnk416stouvacoxlobyjglz5y7o',
    'Host': 'www.sysy.com',
    'Referer': 'https://www.sysy.com/product/list?antibodyType=primary&type%5B0%5D=Antibody&page=2',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
}

engine = create_engine(
    'mysql+pymysql://root:biopicky!2019@127.0.0.1:3306/biopick?charset=utf8')
DBSession = sessionmaker(bind=engine)
session = DBSession()
brand = 'Synaptic Systems'
for i in range(1, 80):
    url = f'https://www.sysy.com/product/list?antibodyType=primary&type%5B0%5D=Antibody&page={i}'

    with requests.Session() as s:
        resp = s.get(url=url, headers=headers, timeout=120)
        html = etree.HTML(resp.text)
        tr = html.xpath('//tr[@class="even" or @class="odd"]')
        objects = []
        for item in tr:
            catno = item.xpath('.//a[@href]/text()')[0]
            link = item.xpath('.//a[@href]/@href')[0]
            ko_kd = []
            if item.xpath(
                    './/span[@class="info-pill"][contains(text(), "K.O.")]'):
                ko_kd.append('ko')
            if item.xpath(
                    './/span[@class="info-pill"][contains(text(), "K.D.")]'):
                ko_kd.append('kd')
            note = '; '.join(i for i in ko_kd)

            if 'variants' in link:
                with open('diff.txt', 'a') as f:
                    f.write(catno + ',' + link + ',' + note + '\n')
            elif 'redirectHistosure' in link:
                link = 'https://sysy-histosure.com/product/' + link.split('/')[
                    -1]
                new_list = List(Brand=brand,
                                Catalog_Number=catno,
                                Note=note,
                                Antibody_detail_URL=link)
                objects.append(new_list)
            else:
                new_list = List(Brand=brand,
                                Catalog_Number=catno,
                                Note=note,
                                Antibody_detail_URL=link)
                objects.append(new_list)
            print(catno, link, note)
        try:
            session.bulk_save_objects(objects)
            session.commit()
            session.close()
        except Exception as e:
            session.rollback()
            print(e)
        else:
            pass

        print(i, 'done')
