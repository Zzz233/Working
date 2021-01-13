import json
import redis

pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=1)
r = redis.Redis(connection_pool=pool)

filepath = r"D:\Dev\bio_work\kd.nsfc.gov.cn\filecode.json"
f_obj = open(filepath, encoding="utf-8")
json_data = json.load(f_obj)
code_sum = json_data["data"]

for item in code_sum:
    name = item["name"]
    code = item["code"]
    if code.startswith("C") and len(code) == 3:
        # print(code, name)
        for projectType in (218, 220, 339, 579, 630, 631, 649):
            for year in range(2005, 2020):
                str_1 = ",".join((code, name, str(projectType), str(year)))
                r.rpush("search_data", str_1)
    elif code == "B07":
        # print(code, name)
        for projectType in (218, 220, 339, 579, 630, 631, 649):
            for year in range(2005, 2020):
                str_2 = ",".join((code, name, str(projectType), str(year)))
                r.rpush("search_data", str_2)

pool.disconnect()
