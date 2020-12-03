import datetime

GMT_FORMAT = '%d %b %Y'
a = datetime.datetime.utcnow().strftime(GMT_FORMAT)
b = datetime.datetime.strptime(a, GMT_FORMAT)
print(a)
print(b)

html = 'size_1'
with open('test.txt', 'a') as f:  # 追加
    f.write('aa')
