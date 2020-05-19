import sys
import time
import redis
import cv2


class InputQueue(object):
    def __init__(self, redis_ip="localhost", redis_port=6379,
                 queue_name="image_stream", input_size=(224, 224, 3)):
        self.r = redis.Redis(host=self.ip, port=self.port, db=0)
        self.queue_name = queue_name
        self.input_size = input_size

    def enqueue_image(self, uri, img):
        import cv2
        new = cv2.resize(img, self.input_size, interpolation=cv2.INTER_CUBIC)
        dict = {"uri": uri, "image": new.tobytes()}
        self.r.xadd(self.queue_name, dict)


class OutputQueue(object):
    def __init__(self, redis_ip="localhost", redis_port=6379, queue_name="image_stream"):
        self.r = redis.Redis(host=self.ip, port=self.port, db=0)
        self.queue_name = queue_name

    def dequeue(self, uri):
        key = "result:"+uri
        return self.r.hget(key)

    def dequeue_stream(self):
        return self.r.hgetall("result:*")


if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    source_queue = sys.argv[3]
    img_path = sys.argv[4]

    img = cv2.imread(img_path)


    input = InputQueue()
    for i in range(1000):
        nowtime = int(round(time.time() * 1000))
        input.enqueue_image(nowtime, img)



