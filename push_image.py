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
def enqueue(pool, queue, payload):
    r = redis.Redis(connection_pool=pool)
    r.rpush(queue, payload)


@timing
def dequeue(pool, queue):
    r = redis.Redis(connection_pool=pool)
    return r.lpop(queue)


@timing
def enqueue_as_stream(pool, queue, dict):
    r = redis.Redis(connection_pool=pool)
    r.xadd(queue, dict)


@timing
def dequeue_as_stream(pool, queue):
    r = redis.Redis(connection_pool=pool)
    return r.xread({queue: b"0-0"})


def test_enqueue(pool, n, source_queue, img):
    for i in range(n):
        dict = {"id":i, "img":img.tobytes()}
        enqueue_as_stream(pool, source_queue, dict)


def test_dequeue(pool, n, target_queue):
    for i in range(10):
        result = dequeue(pool, target_queue)
        print(result)


if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    mode = sys.argv[3]
    img_path = sys.argv[4]
    pool = create_redis_pool(redis_host, redis_port, '0')
    img = cv2.imread(img_path)
    if mode == 'produce':
        source_queue = sys.argv[5]
        test_enqueue(pool, 100, source_queue, img)
    elif mode == 'consume':
        target_queue = sys.argv[5]
        test_dequeue(pool, 10, target_queue)