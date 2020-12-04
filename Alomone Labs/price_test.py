import requests
from lxml import etree
import json
import urllib.parse

payload = {
    "nonce": "cde628e38a",
    "product_id": "37607",
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5"
    "37.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36",
}
url = "https://www.alomone.com/?wc-ajax=alomone_add_to_cart_block"


with requests.Session() as s:
    resp = s.post(url=url, data=payload, headers=headers)
    json_html = resp.json()["html"]
    html = etree.HTML(json_html)
    json_str = html.xpath("//form/@data-product_variations")[0]
    a = json.loads(json_str)
    for attributes in a:
        urlencode_str = attributes["attributes"]["attribute_pa_size"]
        size = urllib.parse.unquote(urlencode_str)
        price = "$ " + str(attributes["display_price"])
        catano = attributes["sku"]
        print(catano, size, price)
# s=urllib.parse.unquote(s)
