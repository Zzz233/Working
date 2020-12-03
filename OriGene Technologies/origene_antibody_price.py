import requests
from lxml import etree

url = 'https://www.alomone.com/?wc-ajax=alomone_add_to_cart_block'
payload = {"nonce": "a8825e1c5f",
           "product_id": "10241"}
headers = {
    # 'Cookie': '__cfduid=def680aef4ef14bedc40eb368dc2daec31606118548; wcumcs_user_currency_session=EUR; _gcl_au=1.1.1108748819.1606101728; ac_enable_tracking=1; mailchimp_landing_site=https%3A%2F%2Fwww.alomone.com%2F%3Fwc-ajax%3Dalomone_add_to_cart_block; prism_224139433=66b4981b-b7f3-4772-8123-8c15cd905908; _ga=GA1.2.1036379382.1606101730; _gid=GA1.2.440301381.1606101730; _fbp=fb.1.1606101730724.1271827224; CookieConsent={stamp:%27W3npz9Q3pUZLCOvrHLQxKQmDChhONiU8xHw8wV/jzJ4ak7NYUBYZ8A==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cver:1%2Cutc:1606101731766%2Cregion:%27sg%27}; covid19-notice=closed; _uetsid=11522fd02d3b11eb95a345dacf87b52d; _uetvid=115264e02d3b11ebadae85e1912d1f00',
    'Host': 'www.alomone.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Length': '34',
    'Origin': 'https://www.alomone.com',
    'Connection': 'keep-alive',
    'Referer': 'https://www.alomone.com/p/anti-5ht7-receptor-htr7-extracellular-antibody/ASR-037',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'Trailers',
}
r = requests.post(url=url, data=payload, headers=headers)
# print(r.json()['html'])
html = etree.HTML(r.json()['html'])
print(html.xpath('//option'))
