import re
import requests
import threading
from queue import Queue
from Mysql_helper import MYSQL
from utils.Random_UserAgent import get_request_headers
import time
import random
import pymysql
import csv
from lxml import etree
import json

r = requests.get('https://www.abcam.cn/datasheetproperties/availability?abId=8226', headers=get_request_headers())
html = r.text
print(html)
size_list = re.findall('"Size":"(.*?)".*?"Price":"(.*?)"', html, re.S)
# title = re.findall('"Title":"(.*?)".*?"PubmedID":(.*?),.*?"ApplicationsShortName":(.*?),.*?"Species":"(.*?)"', html , re.S)
print(size_list)
data = json.loads(html)
print(data["size-information"]['Sizes'][0]['Size'])