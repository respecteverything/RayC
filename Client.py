import sys
import time
import redis
import cv2
import argparse

class InputQueue(object):
    def __init__(self, redis_ip="localhost", redis_port=6379,
                 queue_name="image_stream", input_size=(224, 224)):
        self.r = redis.Redis(host=redis_ip, port=redis_port, db=0)
        self.queue_name = queue_name
        self.input_size = input_size

    def enqueue_image(self, uri, img):
        import cv2
        import numpy as np
        new = cv2.resize(img, self.input_size, interpolation=cv2.INTER_CUBIC)
        new = new.tostring()
        dict = {"uri": uri, "image": new}
        self.r.xadd(self.queue_name, dict)


class OutputQueue(object):
    def __init__(self, redis_ip="localhost", redis_port=6379,):
        self.r = redis.Redis(host=redis_ip, port=redis_port, db=0)

    def dequeue(self, uri):
        key = "result:"+uri
        return self.r.hget(key)

    def dequeue_stream(self):
        return self.r.hgetall("result:*")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cluster-Serving ray setup")
    parser.add_argument("--redis_host", type=str, default='localhost',
                        help="Redis Ip.")
    parser.add_argument("--redis_port", default=6379, type=int,
                        help="The Redis port.")
    parser.add_argument("--source_queue", default='image_stream', type=str,
                        help="The name of source queue")
    parser.add_argument("--img_path", type=str,
                        help="The path to your img")

    args = parser.parse_args()

    img = cv2.imread(args.img_path)
    input = InputQueue(args.redis_host, args.redis_port, args.source_queue)

    nowtime = int(round(time.time() * 1000))
    input.enqueue_image(str(nowtime), img)

    time.sleep(2)
    output = OutputQueue(args.redis_host, args.redis_port)
    print(output.dequeue(str(nowtime)))

