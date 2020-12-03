import requests
from lxml import etree
import re

r = requests.get(
    "https://www.enzolifesciences.com/BML-SA478/14-3-3-epsilon-nt-polyclonal-antibody/")
html = r.text
# element = etree.HTML(html)
print(html)
cata_list = re.findall(
    '<td style="padding-bottom:2px;" ><b>(.*?)</b>.*?align="right">(.*?)</td>',
    html, re.S)
print(cata_list)
