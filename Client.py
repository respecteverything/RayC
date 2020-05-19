import sys
import time
import redis
import cv2


def create_redis_pool(host, port, db):
    pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
    return pool


def dequeue(pool, queue):
    r = redis.Redis(connection_pool=pool)
    return r.lpop(queue)


def enqueue(pool, n, queue,uri, img):
    r = redis.Redis(connection_pool=pool)
    timestamp = []
    for i in range(n):
        # use ms as time stamp
        # nowtime = int(round(time.time() * 1000))
        # timestamp.append(nowtime)
        # dict = {"id": nowTime, "img": img[i].tobytes()}
        # dict = {"id": nowtime, "img": img.tobytes()}
        dict = {"uri": uri, "image": img.tobytes()}
        r.xadd(queue, dict)
    return timestamp


if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    source_queue = sys.argv[3]
    img_path = sys.argv[4]
    pool = create_redis_pool(redis_host, redis_port, '0')
    img = cv2.imread(img_path)
    img.copy()
    img.resize(224, 224, 3)
    uri = "xxxx"
    timestamp = enqueue(pool, 100, source_queue, uri, img)


