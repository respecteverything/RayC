import ray
import redis
import numpy as np
import time
import PreProcessing
import PostProcessing


@ray.remote(resources={"worker": 1})
class Worker(object):
    def __init__(self, redis_host, redis_port, model_type, model, model_helper):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.model = model
        self.model_type = model_type
        self.sess = self.load_model_tf()
        self.helper = model_helper

    def ip(self):
        return ray.services.get_node_ip_address()

    def get_content(self, count=1):
        info = self.r.xreadgroup("consumer", ray.services.get_node_ip_address(), {"source": '0-0'}, count=count)
        img = []
        time_stamp = []
        for i in range(len(info[0][1])):
            img.append(np.frombuffer(info[0][1][i][1][b'img'], dtype=np.uint8))
            time_stamp.append(info[0][1][i][0].decode())
        return img, time_stamp

    def load_model_tf(self):
        import tensorflow as tf
        sess = tf.Session()
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(self.model.file)
        sess.graph.as_default()
        tf.import_graph_def(graph_def, name='')
        sess.run(tf.global_variables_initializer())
        return sess

# considering adding some sort of arg:batch_size?
    def predict(self, count=1):
        if self.model_type == 'tf':
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            op = self.sess.graph.get_tensor_by_name(self.helper['output_names'][0])
            img, timestamp = self.get_content(count)
            res = []
            #for i in range(len(img)):
            #res.append(self.sess.run(op, feed_dict={input: img}))
            res = self.sess.run(op, feed_dict={input: img})
            # considering adding id into result
            self.r.lpush("result", np.argmax(res))
            self.r.xack("source", "consumer", timestamp)