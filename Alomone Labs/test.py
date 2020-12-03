import requests

url = 'https://www.citeab.com/antibodies/search?q=tdp43'
proxies = {
    "http": "http://1.31.96.247:4226",
    "https": "https://1.31.96.247:4226",
}
r = requests.get(url=url, proxies=proxies)
print(r.status_code)
print(r.text)
