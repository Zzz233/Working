import requests
from lxml import etree
with requests.session() as s:
    resp = s.get('https://pubmed.ncbi.nlm.nih.gov/30866779/')
    xml = etree.HTML(resp.text)
    a = xml.xpath('//figure[@class="figure-item " or @class="figure-item tail"]')
    print(a)
