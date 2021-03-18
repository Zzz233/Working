import requests
from lxml import etree
import re

headers = {
    'Host': 'www.absin.cn',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cookie': 'tencentSig=137457664; Hm_lvt_e4fa13237bf225eb26502859bfb17407=1614754215; Hm_lpvt_e4fa13237bf225eb26502859bfb17407=1614754804; UM_distinctid=177f6d9f802133-06e568595461ff-4c3f227c-1fa400-177f6d9f8033e; CNZZDATA1274446530=1897531509-1614750755-%7C1614750755; _ga=GA1.2.1958882062.1614754216; _gid=GA1.2.465428137.1614754216; _qddaz=QD.s71g8d.i22wyy.klt2yznm; _qdda=3-1.1; _qddab=3-rs887c.klt2yzno; _qddamta_800835708=3-0',
    'Upgrade-Insecure-Requests': '1',

}

# r = requests.get(url='https://www.absin.cn/gdf15-antibody/abs114075.html', headers=headers)
r = requests.get('https://www.absin.cn/akap13-antibody/abs130057.html', headers=headers)
r.encoding = 'utf-8'
html = r.text
print(html)
element = etree.HTML(html)
print(element)
trs = element.xpath('.//ul')
print(trs)