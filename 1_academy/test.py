import requests

headers = {
    "Host": "kd.nsfc.gov.cn",
    "Connection": "keep-alive",
    "Content-Length": "329",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "http://kd.nsfc.gov.cn",
    "Referer": "http://kd.nsfc.gov.cn/baseQuery/supportQuery",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cookie": "JSESSIONID=112E75AB8DD56F5BFD37CF4948B53E12",
}
data = {
    "ratifyNo": "",
    "projectName": "",
    "personInCharge": "蒋秉坤",
    "dependUnit": "蚌埠医学院",
    "code": "",
    "keywords": "",
    "ratifyYear": "1986",
    "conclusionYear": "",
    "beginYear": "",
    "endYear": "",
    "checkDep": "",
    "checkType": "",
    "quickQueryInput": "",
    "adminID": "",
    "complete": "false",
    "tryCode": "83ga",
    "pageNum": 0,
    "pageSize": 10,
    "queryType": "input",
}
url = "http://kd.nsfc.gov.cn/baseQuery/data/supportQueryResultsDataForNew"
resp = requests.post(url=url, data=data, headers=headers)
print(resp.text)
