import requests

def get_proxy():
    proxy_url = 'http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text'
    content = requests.get(url=proxy_url).text.strip()
    proxies = {
        "http": "http://" + content,
        "https": "http://" + content,
    }
    print("获取新代理", content)
    return proxies

a = get_proxy()

print(a)
url2 = 'https://www.baidu.com'
resp = requests.get(url=url2, proxies=a)
print(resp.text)
