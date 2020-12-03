import requests
from lxml import etree


class Affbiotech():
    brand = "Affinity Biosciences"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel "
                      "Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, "
                      "like Gecko) Chrome/86.0.4240.111 Safari/537.36"
    }

    def parse_html(self, url):
        with requests.Session() as s:
            resp = s.get(url=url, headers=self.headers)
            status_code = resp.status_code
            html = etree.HTML(resp.text)
        return html, status_code

    def get_brand(self):
        return self

    def get_catno_name(self, html):
        title = html.xpath('//title/text()')[0].split(' - ')
        catno = title[1]
        name = title[2]
        return catno, name

    def get_sellable(self, html):
        sellable = 'no'
        if html.xpath('//td[@class="price-td"]'):
            sellable = 'yes'
        return sellable

    def get_alias(self, html):
        alias = None
        if html.xpath('//p[@class="alias-txt"]'):
            alias = html.xpath('//p[@class="alias-txt"]/text()')[0]
        return alias

    def get_application(self, html):
        application = None
        if html.xpath('//h5[@class="s-title"][contains(text(), "应用:")]'):
            application = html.xpath(
                '//h5[@class="s-title"][contains(text(), '
                '"应用:")]/following-sibling::p[1]/text()')[0].strip()
        return application

    def get_host_species(self, html):
        host_species = None
        if html.xpath('//h5[@class="s-title"][contains(text(),"来源:")]'):
            host_species = html.xpath(
                '//h5[@class="s-title"][contains(text(),"来源:")]'
                '/following-sibling::p[1]/text()')[0].strip()
        return host_species

    def get_reactivity_species(self, html):
        reactivity_species = None
        if html.xpath('//h5[@class="s-title"][contains(text(),"反应性:")]'):
            reactivity_species = html.xpath(
                '//h5[@class="s-title"][contains(text(),"反应性:")]'
                '/following-sibling::p[1]/text()')[0].strip()
        return reactivity_species

    def get_geneid(self, html):
        geneid = None
        if html.xpath('//span[@class="flag flag-geneid hidden"]'):
            geneid = html.xpath(
                '//span[@class="flag flag-geneid hidden"]/text()')[0]
        return geneid

    def get_mw(self, html):
        observed = None  # 检测
        predicted = None  # 预测
        if html.xpath(
                '//h5[@class="s-title"][contains(text(),"Molecular Weight:")]'):
            observed = html.xpath(
                '//p[contains(text(),"Observed Mol.Wt.: ")]/text()')[0].replace(
                'Observed Mol.Wt.: ', '')
            predicted = html.xpath(
                '//p[contains(text(),"Observed Mol.Wt.: ")]/text()')[1].replace(
                ' Predicted Mol.Wt.: ', '')
        return observed, predicted

    def get_swissport(self, html):
        swissport = None
        if html.xpath('//span[@class="flag flag-swiss hidden"]'):
            swissport = html.xpath(
                '//span[@class="flag flag-swiss hidden"]/text()')[0]
        return swissport

    def get_immunogen(self, html):
        html.xpath('//h5')


if __name__ == "__main__":
    for i in range(1):
        link = "http://www.affbiotech.cn/goods-1414-AF3242-Phospho-PI3K+p" \
               "85+%28Tyr458%29%5BTyr467%5D_p55+%28Tyr199%29+Antibody.html"
        r_html, r_status_code = Affbiotech().parse_html(link)

        r_catno, r_name = Affbiotech().get_catno_name(r_html)
        r_sellable = Affbiotech().get_sellable(r_html)
        r_alias = Affbiotech().get_alias(r_html)
        r_application = Affbiotech().get_application(r_html)
        r_host_species = Affbiotech().get_host_species(r_html)
        r_reactivity_species = Affbiotech().get_reactivity_species(r_html)
        r_geneid = Affbiotech().get_geneid(r_html)
        r_observed, r_predicted = Affbiotech().get_mw(r_html)
        r_swissport = Affbiotech().get_swissport(r_html)
        print(r_observed, r_predicted)
