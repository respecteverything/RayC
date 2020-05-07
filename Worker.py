import ray
import redis
import numpy as np
import time


@ray.remote(resources={"worker": 1})
class Worker(object):
    def __init__(self, redis_host, redis_port, model_type, model, model_helper):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.model = model
        self.model_type = model_type
        try:
            self.sess = self.load_model()
        except ray.exceptions.RayWorkerError():
            print("Can not init worker.")
        self.helper = model_helper

    def ip(self):
        return ray.services.get_node_ip_address()

    def get_content(self, count=1):
        ray.logger.info("Getting contents now....")
        info = self.r.xreadgroup("consumer", ray.services.get_node_ip_address(), {"source": '>'}, count=count)
        img = []
        time_stamp = []
        ids = []
        ray.logger.info("The length of contents is " + str(info[0][1]))
        for i in range(len(info[0][1])):
            img.append(np.frombuffer(info[0][1][i][1][b'img'], dtype=np.float32))
            time_stamp.append(info[0][1][i][0].decode())
            ids.append(info[0][1][i][1][b'id'].decode())
        return img, time_stamp, id

    def load_model_tf(self):
        import tensorflow as tf
        sess = tf.Session()
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(self.model)
        sess.graph.as_default()
        tf.import_graph_def(graph_def, name='')
        sess.run(tf.global_variables_initializer())
        return sess

    def load_model_pytorch(self):
        fp = open("torch_model.pt", 'wb')
        fp.write(self.model)
        fp.close()

        import torch
        model = torch.jit.load("torch_model.pt")
        model.eval()
        return model

    def load_model_bigdl(self):
        fp = open("bigdl_model.model", 'wb')
        fp.write(self.model)
        fp.close()

        from zoo.pipeline.inference import InferenceModel
        model = InferenceModel()
        model = InferenceModel.load_bigdl("bigdl_model.model")
        return model

    def load_model(self):
        if self.model_type == 'tf':
            return self.load_model_tf()
        elif self.model_type == 'torch':
            return self.load_model_pytorch()
        elif self.model_type == 'bigdl':
            return self.load_model_bigdl()
        else:
            raise ray.exceptions.RayWorkerError("Not support this kind of model.")

    def predict(self, count=1):
        img, timestamp, ids = self.get_content(count)
        img, timestamp = self.pre_processing(img, timestamp)
        if self.model_type == 'tf':
            start = time.time()
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            op = self.sess.graph.get_tensor_by_name(self.helper['output_names'][0])
            res = self.sess.run(op, feed_dict={input: img})
            end = time.time()
        elif self.model_type == 'torch':
            start = time.time()
            res = self.sess(img)
            res = res.detach()
            end = time.time()
        elif self.model_type == 'bigdl':
            start = time.time()
            res = self.sess.predict(img)
            end = time.time()
        else:
            raise ray.exceptions.RayWorkerError("Not support this kind of model.")

        throughput_1 = str(len(img) / (end - start))
        for i in range(len(img)):
            self.r.sadd(ids[i], int(np.argmax(res)))
            self.r.xack("source", "consumer", timestamp[i])
            self.r.xdel("source", timestamp[i])
        end = time.time()
        self.post_processing()
        ray.logger.info("Processing " + str(len(img)) + " images.")
        throughput_2 = str(len(img) / (end - start))
        ray.logger.info("Throughput 1 is " + str(throughput_1) + ".")
        ray.logger.info("Throughput 2 is " + str(throughput_2) + ".")
        return [throughput_1, throughput_2]

    def get_model_input_size(self):
        if self.model_type == "tf":
            input = self.sess.graph.get_tensor_by_name(self.helper['input_names'][0])
            size = input.shape.as_list()
        elif self.model_type == 'torch':
            # print("PyTorch models does't require to resize.") set 3 dims
            size = [None, 3, 224, 224]
        elif self.model_type == 'bigdl':
            # print("Bigdl models does't require to resize.") set 3 dims
            size = [None, 3, 224, 224]
        else:
            raise ray.exceptions.RayWorkerError("Not support this kind of type. Please check or rewrite the functions.")
        return size

    def pre_processing(self, img, stamp):
        new_img_size = self.get_model_input_size()
        new_img = self.resize(img, new_img_size)
        if self.model_type == 'torch':
            new_img = torch.from_numpy(new_img)
        return new_img, stamp

    def post_processing(self):
        print("here is post_processing.")

    def resize(self, img, size):
        size[0] = len(img)
        if self.model_type == 'tf':
            size[1] = 1
        print(size)
        image = np.array(img)
        image = image.copy()
        temp = image.resize(tuple(size))
        print(image.shape)
        return image

    def z_score_normalizing(self, img):
        mean = sum(img) / len(img)  # 先求均值
        variance = (sum([(i - mean) ** 2 for i in img])) / len(img)  # 再求方差
        normal = [(i - mean) / variance ** 0.5 for i in img]  # 按照公式标准化
        return normal

    def max_min_normalizing(self, img, max, min):
        normal = [min + np.ptp(img)*(i - min)/(max -min) for i in img]
        return normal
