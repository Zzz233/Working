import requests
from lxml import etree

with requests.Session() as s:
    r = s.get(
        'https://www.diagenode.com/cn/p/h3k4me3-polyclonal-antibody-premium-50-ug-50-ul')
    html = etree.HTML(r.text)
    a = html.xpath(
        '//div[@class=" spaced-before row"]')[0]
    # print(a)


class Testing():
    def sub_images(self, html):
        changdu = int(len(html.xpath(
            './/div[@id="info1"][@class="content active"]/div[@class="row"]/div[@class="small-6 columns"]')) / 2)
        for i in range(0, changdu):
            html.xpath(
                './/div[@id="info1"][@class="content active"]/div[@class="row"]/div[@class="small-6 columns"]')
            print(i)


if __name__ == '__main__':
    Testing().sub_images(a)
