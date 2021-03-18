import redis


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)

for w_1 in range(0, 255+1):
    for w_2 in range(0, 255+1):
        url = f'http://8.136.{w_1}.{w_2}:5601'
        r.rpush('ip_wait_list', url)
print('done')
