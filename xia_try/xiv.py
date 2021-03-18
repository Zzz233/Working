from lxml import etree

html = etree.HTML('asdasd')
img_tags = html.xpath('.//li[@class="app_img_b"]')
for li in img_tags:
    img_url = li.xpath('./img/@src')[0].strip()
    img_desc = li.xpath('./../li[@class="black11_a_underline app_desc"]/text()')[0].strip()