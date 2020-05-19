import ray
import redis
import numpy as np
import time


@ray.remote(resources={"worker": 1})
class Worker(object):
    def __init__(self, redis_host, redis_port, model_type, model, tf_helper,
                 source_name="image_stream", group_name="consumer",
                 batch_size=64, input_size=(224, 224, 3)):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.model = model
        self.model_type = model_type
        try:
            self.sess = self.load_model()
        except ray.exceptions.RayWorkerError():
            print("Can not init worker.")
        except BaseException as e:
            print(e)
        self.tf_helper = tf_helper
        self.source_name = source_name
        self.batch = batch_size
        self.input_size = input_size
        self.group_name = group_name

        while True:
            self.inference()

    def ip(self):
        return ray.services.get_node_ip_address()

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
        import psutil
        cores = psutil.cpu_count()
        torch.set_num_threads(cores)
        model = torch.jit.load("torch_model.pt")
        model.eval()
        return model

    def load_model_bigdl(self):
        fp = open("bigdl_model.model", 'wb')
        fp.write(self.model)
        fp.close()

        from zoo.pipeline.inference import InferenceModel
        model = InferenceModel()
        model.load_bigdl("bigdl_model.model")
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

    def inference(self):
        start = time.time()
        imgs, time_stamps, uris = self.source()
        e1 = time.time()
        results = self.predict(imgs)
        e2 = time.time()
        self.sink(time_stamps, uris, results)
        e3 = time.time()

        ray.logger.info("Loading images time: " + str(e1-start) + ". Predict time: "
                        + str(e2-e1) + ". Sink time: " + str(e3-e2))
        ray.logger.info("Process " + str(len(imgs)) + " images. Throughput: " + str((e3-start)/len(imgs)))

    def source(self):
        ray.logger.info("Getting contents now....")
        info = self.r.xreadgroup(self.group_name, ray.services.get_node_ip_address(),
                                 {self.source_name: '>'}, block=10, count=self.batch)
        imgs = []
        time_stamps = []
        uris = []
        ray.logger.info("The length of contents is " + str(len(info[0][1])))
        for i in range(len(info[0][1])):
            imgs.append(np.frombuffer(info[0][1][i][1][b'image'], dtype=np.float32))
            time_stamps.append(info[0][1][i][0].decode())
            uris.append(info[0][1][i][1][b'uri'].decode())
        return imgs, time_stamps, uris

    def predict(self, imgs):
        if self.model_type == 'tf':
            input = self.sess.graph.get_tensor_by_name(self.tf_helper[0])
            op = self.sess.graph.get_tensor_by_name(self.tf_helper[1])
            res = self.sess.run(op, feed_dict={input: imgs})
        elif self.model_type == 'torch':
            res = self.sess(imgs)
            res = res.detach()
        elif self.model_type == 'bigdl':
            res = self.sess.predict(imgs)
        else:
            raise ray.exceptions.RayWorkerError("Not support this kind of model.")

        results = res.tolist()
        return results

    def sink(self, time_stamps, uris, results):
        p = self.r.pipeline(transaction=False)
        for time_stamp, uri, result in time_stamps, uris, results:
            key = "result:" + str(uri)
            p.hset(key, "value", np.argmax(result)).xdel(self.source_name, time_stamp)

        p.execute()

    def pre_processing(self, img):
        new_img = self.resize(img)
        if self.model_type == 'torch':
            import torch
            new_img = torch.from_numpy(new_img)
        return new_img

    def post_processing(self):
        print("here is post_processing.")

    def resize(self, img):
        size = (len(img)) + self.input_size
        image = np.array(img)
        image = image.copy()
        image.resize(size)
        return image

    def z_score_normalizing(self, img):
        mean = sum(img) / len(img)  # 先求均值
        variance = (sum([(i - mean) ** 2 for i in img])) / len(img)  # 再求方差
        normal = [(i - mean) / variance ** 0.5 for i in img]  # 按照公式标准化
        return normal

    def max_min_normalizing(self, img, max, min):
        normal = [min + np.ptp(img)*(i - min)/(max -min) for i in img]
        return normal
