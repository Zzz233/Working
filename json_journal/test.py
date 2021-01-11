import json
import redis

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=1)
r = redis.Redis(connection_pool=pool)

filepath = "D:\\Dev\\bio_work\\json_journal\\ifqbt_2020.json"
f_obj = open(filepath)
data = json.load(f_obj)
for item in data:
    keywd = item["issn"]
    if keywd == "NA":
        keywd = item["journal"]
    r.rpush("search_key", keywd)
    print(keywd)
r.close()
