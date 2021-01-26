import redis


# Redis
pool = redis.ConnectionPool(host="localhost",
                            port=6379,
                            decode_responses=True,
                            db=0)
r = redis.Redis(connection_pool=pool)
list_a = []
while r.exists('11111'):
    path = r.rpop('11111')
    if path in list_a:
        print('有重复')
    list_a.append(path)

pool.disconnect()