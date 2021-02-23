import requests
from utils.Random_UserAgent import get_request_headers
import re
from lxml import etree

from Mysql_helper import MYSQL

mysql = MYSQL('new_antibodies_info')
for x in range(1, 3061):
    url = 'https://www.abcam.cn/products/loadmore?selected.productType=Primary+antibodies&pagenumber=%d' % x
    sql = 'insert into Abcam_Antibody_list_url (Antibody_list_URL) values("{}");'.format(url)
    mysql.insert_into_table(sql)