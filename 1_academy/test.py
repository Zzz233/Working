import requests

proxy_url = "http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text"
res = requests.get(proxy_url)
proxy_text = res.text.strip()
print(proxy_text)
proxies = {
    "http": "http://" + proxy_text,
    "https": "https://" + proxy_text,
}
print(proxies)
data = {
    "page": "",
    "name": "",
    "person": "",
    "no": "",
    "company": "",
    "addcomment_s1": "B",
    "addcomment_s2": "B02",
    "addcomment_s3": "B0201",
    "addcomment_s4": "",
    "money1": "",
    "money2": "",
    "startTime": "2019",
    "endTime": "2019",
    "subcategory": "",
    "searchsubmit": "true",
}
url = "https://www.letpub.com.cn/nsfcfund_search.php?mode=advanced&datakind=list&currentpage=1"
resp = requests.get(url=url, proxies=proxies)
print(resp.text)
