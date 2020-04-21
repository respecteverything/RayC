import ray
import redis
import numpy as np


@ray.remote(resources={"worker": 1})
class Worker(object):
    def __init__(self, redis_host, redis_port, model_type, model, model_helper):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.model = model
        self.model_type = model_type
        self.sess = None
        self.helper = model_helper
        self.temp = None

    def ip(self):
        # print(ray.services.get_node_ip_address())
        # r = redis.Redis(connection_pool=pool)
        return ray.services.get_node_ip_address()

    def handle(self):
        info = self.r.xreadgroup("consumer", ray.services.get_node_ip_address(), {"source": '>'}, count=1)
        img = np.frombuffer(info[0][1][0][1][b'img'], dtype=np.uint8)
        temp = img.resize(1, 224, 224, 3)
        self.temp = info[0][1][0][0].decode()
        return img

    def load_model_tf(self):
        import tensorflow as tf
        sess = tf.Session()
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(self.model.file)
        sess.graph.as_default()
        tf.import_graph_def(graph_def, name='')
        sess.run(tf.global_variables_initializer())
        # try...catch need to add
        self.sess = sess

    def predict(self):
        if self.model_type == 'tf':
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            op = self.sess.graph.get_tensor_by_name(self.helper['output_names'][0])
            res = self.sess.run(op, feed_dict={input: self.handle()})
            self.r.lpush("result",np.argmax(res))
            self.r.xack("source","consumer",self.temp)

