import requests

proxy_url = "http://httpbapi.dobel.cn/User/getIp&account=ELEEEBY5r1JiN3ep&accountKey=cLSj17Kl5Zr4&num=1&cityId=all&format=text"
resp = requests.get(url=proxy_url).text
print(resp)