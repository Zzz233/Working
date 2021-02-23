import requests
from lxml import etree

url = 'https://www.abcam.com/products/loadmore'
headers = {
    'Host': 'www.abcam_old.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-NewRelic-ID': 'VQIAU1ZQGwsDU1JQBw==',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Referer': 'https://www.abcam.com/products?sortOptions=Relevance',
    # 'Cookie': 'PP=1; _gcl_au=1.1.941902288.1603845240; _sp_id.2495=cb18a3dc0368ba07.1603845240.3.1604368361.1604279248.7970f0c1-a832-4522-9298-314a54e69a44; C2LC=CN; mbox=PC#03113d88cf2343f99905769324b57a32.38_0#1667612946|session#53c638420e684e049350dcbb004088b1#1604369997; _ga=GA1.2.931369227.1603845241; _mibhv=anon-1603845253585-6183173558_7395; _uetvid=5206596018b511eb823961cd48d813c5; _hjid=00a20296-dc0d-4bc4-9b41-ca41ee11bb80; ak_bmsc=50517C2852A38481BBDF170A6B2022EA8CAE1904C605000009B7A05F83FA222D~plY+0d+pCnwtUViI+oeIgHIrm2oTzxGm+JQN+KQFNilyK4awIqYNPYm49N0YjUU6tayCj+0IBUpGdxSCl1Z0YpgaN89ccHbuaG0xzv/4uVb58HoW4/sIQA7hxQ9yjY2D0afnm1pQ5VV1SpeJqfDDZULAdwZE3blFOD9g6zOa071vIpJY/04zsihTEO6DsAIWIqfMyVzg9xT561pm3lfkSPCO3qoUHuLcdsMYuBd/y4FJLxRk+GV7nsIss56R3uyvfi; _sp_ses.2495=*; check=true; JSESSIONID=FFF275997B4D13296FD344166B346F9F.Pub2; bm_sv=C0A805DD6EF48721B87902638DA8DB8D~Ztbu0QzJsgmYTGx4conDoQrsouhiRVapqXdzRyjtVxaez9i23bsLiuBjy24GKomvdhYj1GdZVEqEugxxFKTbpvcRnMeVX//Zt2QmsCh/OT7JnpnlFagAuVI/t9Yy20c5LcxOP5Y7500NbEcApjF5FG3ypvH2Y/JB969LQs/TPVc=; _gid=GA1.2.1796073784.1604368138; mboxEdgeCluster=38; _hjTLDTest=1; _hjAbsoluteSessionInProgress=1; _hjShownFeedbackMessage=true; _gat_UA-367099-1=1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'Trailers',
}

if __name__ == '__main__':
    for i in range(1):
        params = {'sortOptions': 'Relevance', 'pagenumber': 100}
        with requests.Session() as s:
            resp = s.get(url=url, params=params, headers=headers)
            html = etree.HTML(resp.text)

        for item in html.xpath('.//div[@data-productcode]'):
            name = item.xpath('@data-productname')[0]
            print(name)
            catno = item.xpath('@data-productcode')[0]
            print(catno)
            if item.xpath(
                    './/div[@class="clearfix pws_item ProductDescription"]'):
                description = item.xpath(
                    './/div[@class="clearfix pws_item ProductDescription"]'
                    '/div[@class="pws_value"]/text()')[0]
                print(description)
            else:
                description = None
                print(1)

            if item.xpath(
                    './/div[@class="clearfix pws_item Application"]'):
                application = str(item.xpath(
                    './/div[@class="clearfix pws_item Application"]'
                    '/div[@class="pws_value"]/span/text()')).replace(
                    '[', '').replace('\'', '').replace(']', '')
                print(application)
            else:
                application = None
                print(2)
            if item.xpath(
                    './/div[@class="clearfix pws_item Reactivity"]'):
                reactivity = str(item.xpath(
                    './/div[@class="clearfix pws_item Reactivity"]'
                    '/div[@class="pws_value"]/text()')).replace(
                    '[', '').replace('\'', '').replace(']', '')
                print(reactivity)
            else:
                reactivity = None
                print(3)
            if item.xpath(
                    './/div[@class="clearfix pws_item Conjugate"]'):
                conjugate = item.xpath(
                    './/div[@class="clearfix pws_item Conjugate"]'
                    '/div[@class="pws_value"]/text()')[0]
                print(conjugate)
            else:
                conjugate = None
                print(4)
