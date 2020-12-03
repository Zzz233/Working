import requests
from lxml import etree

url = 'https://www.enzolifesciences.com/ADI-SPA-200-488/ubiquitin-polyclonal-antibody-dylight-488-conjugate/'
resp = requests.get(url)
html_parsed = etree.HTML(resp.content)
# print(resp.text)
html_content = html_parsed.xpath('//div[@class="tx-cs2smdnavi-pi3"]')[0]
for bad in html_content.xpath(
        './/div[@class="tx-cs2smdnavi-pi5"]'):
    bad.getparent().remove(bad)
catano = html_content.xpath(
    './/tr/td[@style="padding-bottom:2px;"]/b/text()')
size = html_content.xpath(
    './/tr/td[@style="padding-bottom:2px;"]/b/../following-sibling::td/text()')

a = html_content.xpath(
    './/tr/td[@style="padding-bottom:2px;"]/b/../..')
# print(a)

# print(catano, size)
for i, cata in enumerate(a):
    a = cata.xpath('.//td/b/text()')[0]
    print(a)
    print(cata.xpath(
        './/td/b/../following-sibling::td[1]/text()')[0].replace('\\xa0', ' '))
