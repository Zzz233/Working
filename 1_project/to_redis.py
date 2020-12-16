import redis


pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True, db=5)
r = redis.Redis(connection_pool=pool)
for i in range(1, 10136):
    r.rpush("keyanzhiku_pagenum", i)

pool.disconnect()
