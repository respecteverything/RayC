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
        # add some log info here. Calculate time or something
        info = self.r.xreadgroup("consumer", ray.services.get_node_ip_address(), {"source": '0'}, count=count)
        img = []
        time_stamp = []
        for i in range(len(info[0][1])):
            size = self.get_model_input_size()
            im = PreProcessing.resize(np.frombuffer(info[0][1][i][1][b'img'], dtype=np.float32), size)
            img.append(im)
            time_stamp.append(info[0][1][i][0].decode())
        return img, time_stamp

    def load_model_tf(self):
        import tensorflow as tf
        sess = tf.Session()
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(self.model)
        sess.graph.as_default()
        tf.import_graph_def(graph_def, name='')
        sess.run(tf.global_variables_initializer())
        return sess

# considering adding some sort of arg:batch_size?
    def predict(self, count=1):
        if self.model_type == 'tf':
            start = time.time()
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            op = self.sess.graph.get_tensor_by_name(self.helper['output_names'][0])
            img, timestamp = self.get_content(count)
            for i in range(len(img)):
                res = self.sess.run(op, feed_dict={input: img[i]})
                self.r.sadd(timestamp[i], np.argmax(res))
                self.r.xack("source", "consumer", timestamp[i])
            end = time.time()

        result = "Processing " + str(len(img)) + " images. "
        throughput = "Throughput is " + str(len(img)/(end-start))
        return result + throughput

    def get_model_input_size(self):
        if self.model_type == "tf":
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            size = tuple(input.shape.as_list())
            return size

