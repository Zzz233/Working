import requests
from lxml import etree


class Template:
    def __init__(self):
        """
        初始化变量
        """
        self.base_url = "https://123123/list/020000,000000,0000,00,9,99,2,{}.html?"
        self.headers = {}

    def get_url_list(self):
        """
        通过base_url获取URL的列表
        :return: 通过base_url获取到的url_list
        """
        # 1. 使用yield返回数据
        for page in range(1, 100):
            url = self.base_url.format(page)
            yield url
        # 2. 使用列表推导式
        return [self.base_url.format(page) for page in range(1, 100)]

    def get_url_html(self, url):
        """
        获取当前url的html
        :param url: 需要抓取的url
        :return: 当前url的html
        """
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            response.encoding = response.apparent_encoding  # 指定编码
            return response.text
        return None

    def parse_html(self, html):
        """
        解析当前html
        :param html: 需要解析的html文本
        :return: 当前html中的数据
        """
        x_html = etree.HTML(html)
        items = x_html.xpath("")  # xpath/
        for item in items:
            yield item

    def parse_item(self, item):
        """
        处理结果：文本，SQL，print...
        :param item: 需要处理的数据
        :return: 当前数据的处理结果
        """
        print(item)

    def run(self):
        for url in self.get_url_list():
            html = self.get_url_html(url)
            if html == None:
                continue
            for item in self.parse_html(html):
                if item == None:
                    continue
                self.parse_item(item)


template = Template()
template.run()