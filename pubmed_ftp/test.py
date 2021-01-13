from lxml import etree

path = "D:/lbs.xml"
xml = etree.parse(path)
articles = xml.xpath("//ArticleTitle//text()")
article_title = "".join(i for i in articles)
print(article_title)