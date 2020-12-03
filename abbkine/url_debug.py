import requests
from bs4 import BeautifulSoup
from lxml import etree

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Cookie': 'wp_xh_session_8ce7451aa12a2213a89e614aaf6dd2a4=a45414efda907987395d939633942a22%7C%7C1604806403%7C%7C1604802803%7C%7Cd6f6d4756ee535e0a448034b353a6c38; __51cke__=; tk_ai=woo%3AAc2p8GbqpIck4YzXCmQLNs2F; __tins__19011748=%7B%22sid%22%3A%201604636018632%2C%20%22vd%22%3A%202%2C%20%22expires%22%3A%201604637866080%7D; __51laig__=3',
    'Host': 'www.abbkine.cn',
    'Pragma': 'no-cache',
    'Referer': 'http://www.abbkine.cn/products/2?productcat=75',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36',
}
# for l in range(10):
#     for i in range(1, 9):
#         url = f'http://www.abbkine.cn/products/page/{i}'
#         params = {'productcat': 75}
#         r = requests.get(url=url, params=params, headers=headers)
#         soup = BeautifulSoup(r.text, 'html.parser')
#         for item in soup.find_all('div', class_='productlistleft fl'):
#             url = item.find('a')['href']
#             with open('urls.txt', 'a') as file:
#                 file.write(url + '\n')
#     print(l)

limit = []
for line in open('urls.txt', encoding='utf-8'):
    if line not in limit:
        limit.append(line)

# print(len(limit), limit)
for i in limit:
    print(i.strip('\n'))
