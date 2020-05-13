import sys
import time
import redis
import cv2


def timing(func):
    def wrapper(*args):
        begin = int(round(time.time() * 1000))
        print("executing", func.__name__)
        result = func(*args)
        end = int(round(time.time() * 1000))
        print("executed " + func.__name__ + ", time consumed:%d" % (end - begin))
        return result
    return wrapper


@timing
def create_redis_pool(host, port, db):
    pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
    return pool


@timing
def dequeue(pool, queue):
    r = redis.Redis(connection_pool=pool)
    return r.lpop(queue)


def test_enqueue(pool, n, queue, img):
    r = redis.Redis(connection_pool=pool)
    nowtime = []
    for i in range(n):
        # use ms as time stamp
        nowtime.append(int(round(time.time() * 1000)))
        # dict = {"id": nowTime, "img": img[i].tobytes()}
        dict = {"id": nowtime, "img": img.tobytes()}
        r.xadd(queue, dict)
    return nowtime


if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    mode = sys.argv[3]
    img_path = sys.argv[4]
    pool = create_redis_pool(redis_host, redis_port, '0')
    img = cv2.imread(img_path)
    img.copy()
    img.resize(224, 224, 3)
    if mode == 'produce':
        source_queue = sys.argv[5]
        test_enqueue(pool, 100, source_queue, img)
    elif mode == 'consume':
        target_queue = sys.argv[5]
        test_dequeue(pool, 10, target_queue)