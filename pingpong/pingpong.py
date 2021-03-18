import requests
import threading
from queue import Queue
import redis


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True,
                            db=15)
r = redis.Redis(connection_pool=pool)


class Producer(threading.Thread):
    def __init__(self, ip_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.ip_queue = ip_queue

    def run(self):
        # while True:
        #     if self.ip_queue.empty():
        #         print(1)
        #         break
        #     self.ip_queue.get()
        self.hit()

    def hit(self):
        while r.exists('ip_wait_list'):
            with requests.Session() as s:
                try:
                    url = r.lpop('ip_wait_list')
                    r_code = s.get(url=url, timeout=5).status_code
                except:
                    r_code = 6456
                if r_code == 200:
                    r.rpush('tianna', url)
                    print('hit: ', url)
                else:
                    print('pass')


def main():
    ip_queue = Queue(1000000)
    for x in range(10):
        t = Producer(ip_queue)
        t.start()


if __name__ == '__main__':
    main()