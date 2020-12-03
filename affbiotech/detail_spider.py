import requests
from bs4 import BeautifulSoup


class Affbiotech():
    brand = 'Affinity Biosciences'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0)'
                      ' Gecko/20100101 Firefox/82.0'
    }

    # 解析html
    def parse_html(self, url):
        with requests.Session() as session:
            resp = session.get(url=url, headers=self.headers)
            soup = BeautifulSoup(resp.text, 'html.parser')
        return soup, resp.text

    # 品牌
    def get_brand(self):
        return self.brand

    # 货号 & 产品名字
    def get_catno_name(self, soup):
        catno = soup.find(
            'div', class_='page-header').get_text().split(' - ')[0].strip()
        name = soup.find(
            'div', class_='page-header').get_text().split(' - ')[1].strip()
        return catno, name

    # 抗体种类
    def get_antibody_type(self, soup):
        # 好像没有
        return

    # 是否出售
    def get_sellable(self, html):
        sellable = 'no'
        if "price-td" in html:
            sellable = 'yes'
        return sellable

    # 别名
    def get_synonyms(self, soup, html):
        synonyms = None
        if '别名:' in html:
            synonyms = soup.find('div', id='accordion1').get_text().split(
                '别名:Expand▼\n')[1].split('\n')[0].strip()
        return synonyms

    # 应用
    def get_application(self, soup, html):
        application = None
        if '应用:' in html:
            application = soup.find('div', id='accordion1').get_text().split(
                '应用:')[1].strip().split('\n')[0]
        return application

    # 欧联
    def get_conjugated(self):
        # 没看到
        return

    # 克隆号
    def get_clone_number(self):
        # 没看到
        return

    # 是否重组抗体
    def get_recombinant_antibody(self):
        # 没看到
        return

    # 修饰（磷酸化、甲基化、还是乙酰化，能取到修饰位点需要取位点）
    def get_modified(self):
        # 没看到
        return

    # 宿主
    def get_host_species(self, soup, html):
        host_species = None
        if '来源:' in html:
            host_species = \
                soup.find('div', id='accordion1').get_text().split(
                    '来源:')[1].strip().split('\n')[0]
        return host_species

    # 抗原来源物种
    def get_reactivity_species(self, soup, html):
        reactivity_species = None
        if '反应性:' in html:
            reactivity_species = \
                soup.find('div', id='accordion1').get_text().split(
                    '反应性:')[1].strip().split('\n')[0]
        return reactivity_species

    # 基因id
    def get_geneid(self, soup, html):
        geneid = None
        if 'Gene ID:' in html:
            geneid = soup.find('span',
                               class_='flag flag-geneid hidden').get_text()
        return geneid

    # 预测分子量 检测分子量
    def get_mw(self, soup, html):
        observed = None  # 检测
        predicted = None  # 预测
        if 'Observed Mol.Wt.:' in html:
            observed = soup.find(
                'div', id='collapseTwo').get_text().split('Observed Mol.Wt.:')[
                1].split('.')[0].strip()
        if 'Predicted Mol.Wt.:' in html:
            predicted = soup.find(
                'div', id='collapseTwo').get_text().split('Predicted Mol.Wt.:')[
                1].split('.')[0].strip()
        return observed, predicted

    # 可以反应物种
    def get_species_reactivity(self):
        # 疑问
        return

    def get_swissport(self, soup, html):
        swissport = None
        if '蛋白号:' in html:
            swissport = soup.find('span',
                                  class_='flag flag-swiss hidden').get_text()
        return swissport

    # 抗原
    def get_immunogen(self, soup, html):
        immunogen = None
        if 'Immunogen:' in html:
            immunogen = soup.find(
                'div', id='collapseTwo').get_text().split('Immunogen:')[
                1].strip().split('\n')[0]
        return immunogen

    def get_purify(self, soup, html):
        purify = None
        if '纯化:' in html:
            purify = soup.find('div', id='accordion1').get_text().split(
                '纯化:')[1].strip().split('\n')[0]
        return purify

    def get_citations_num(self, soup, html):
        citations_num = None
        if '#tab_citation' in html:
            citations_num = soup.find(
                'a', href='#tab_citation').get_text().split('文献(')[1].split(
                ')')[0]
        return citations_num

    def get_datasheet_url(self, soup, html):
        datasheet_url = None
        if 'MSDS' and '说明书' in html:
            datasheet_url = 'http://www.affbiotech.cn/' + \
                            soup.find('div', class_='col-md-10').find_all('a')[
                                1]['href']
        return datasheet_url

    def get_image_url_qty(self, soup, html):
        image_url = None
        image_qty = '0'
        if '<ul class="slides">' in html:
            image_url = ''
            image_qty = 0
            images = soup.find('ul', class_='slides').find_all('img',
                                                               class_='img-responsive center-block')
            for image in images:
                image_url = image_url + 'http://www.affbiotech.cn/' + \
                            image['src'] + ','
                image_qty += 1
        return image_url, image_qty


if __name__ == '__main__':
    link = 'http://www.affbiotech.cn/goods-1414-AF3242-Phospho-PI3K+p85+%28Tyr458%29%5BTyr467%5D_p55+%28Tyr199%29+Antibody.html'
    r_soup, r_html = Affbiotech().parse_html(link)

    r_catno, r_name = Affbiotech().get_catno_name(r_soup)
    r_sellable = Affbiotech().get_sellable(r_html)
    r_synonyms = Affbiotech().get_synonyms(r_soup, r_html)
    r_application = Affbiotech().get_application(r_soup, r_html)
    r_host_species = Affbiotech().get_host_species(r_soup, r_html)
    r_reactivity_species = Affbiotech().get_reactivity_species(r_soup,
                                                               r_html)
    r_antibody_detail_url = link
    r_antibody_status = '0'
    r_price_status = '0'
    r_citations_status = '0'
    r_get_geneid = Affbiotech().get_geneid(r_soup, r_html)
    r_observed, r_predicted = Affbiotech().get_mw(r_soup, r_html)
    r_swissport = Affbiotech().get_swissport(r_soup, r_html)
    r_immunogen = Affbiotech().get_immunogen(r_soup, r_html)
    r_purify = Affbiotech().get_purify(r_soup, r_html)
    r_citations_num = Affbiotech().get_citations_num(r_soup, r_html)
    r_datasheet_url = Affbiotech().get_datasheet_url(r_soup, r_html)
    r_image_url, r_image__qty = Affbiotech().get_image_url_qty(r_soup, r_html)

    print(r_image_url, r_image__qty)
