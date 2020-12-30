import requests
from lxml import etree

headers = {
    "Host": "adipogen.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    # "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://adipogen.com/elisa-kits.html?p=1",
    "Connection": "keep-alive",
    "Cookie": "mage-translation-storage=%7B%7D; mage-translation-file-version=%7B%7D; PHPSESSID=9bb80934853ffb3babfa13bc7df0ffbc; form_key=iNlLuqb9Ql5ueXfV; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; _ga=GA1.2.658866885.1609297013; _gid=GA1.2.1551139616.1609297013; form_key=iNlLuqb9Ql5ueXfV; store=cn; X-Magento-Vary=0af65036ebb608ce5a0a29a665067cc29db28878; mage-messages=; section_data_ids=%7B%22cart%22%3A1609297037%7D",
    "Upgrade-Insecure-Requests": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "Trailers",
}
with requests.Session() as s:
    url = "https://adipogen.com/yif-lf-ek0200-alpha-enolase-eno1-non-neuronal-enolase-human-elisa-kit.html"

    resp = s.get(url=url, headers=headers)
    print(resp.text)
