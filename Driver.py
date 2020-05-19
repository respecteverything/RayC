import sys
import redis
import os
import ray
from Worker import Worker
import time


class Driver(object):
    def __init__(self, redis_ip, redis_port, model_type,
                 model_path, source_name="image_stream", group_name="consumer",
                 batch_size=64, input_size=(224, 224, 3), tf_input=None, tf_output=None):
        self.redis_ip = redis_ip
        self.redis_port = redis_port
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.workers = []
        self.workers_ip = []
        self.source_name = source_name
        self.group_name = group_name
        # self.redis = redis.Redis(host=self.ip, port=self.port, db=0)
        self.input_size = input_size
        self.batch = batch_size
        self.tf_helper = [tf_input, tf_output]
        self.redis.xgroup_create(self.source_name, self.group_name, id=0, mkstream=True)

    def load_model(self):
        if self.model_type == "tf":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    try:
                        if ".pb" in file:
                            model_path = maindir + "/" + file
                    except FileNotFoundError as e:
                        print(e + " Model not found")
            file = open(model_path, 'rb')
            self.model = file.read()
        elif self.model_type == "torch":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    try:
                        if ".pt" in file:
                            model_path = maindir + "/" + file
                    except FileNotFoundError as e:
                        print(e + " Model not found")
                file = open(model_path, 'rb')
                self.model = file.read()
        elif self.model_type == "bigdl":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    try:
                        if ".model" in file:
                            model_path = maindir + "/" + file
                    except FileNotFoundError as e:
                        print(e + " Model not found")
                file = open(model_path, 'rb')
                self.model = file.read()
        else:
            print("If you want to use customer model, please rewrite some functions.")
            print("More detail please check the github page.")
            raise ValueError("Currently Not support " + self.model_type)

    def add_worker(self):
        new_worker = Worker.remote(self.redis_ip, self.redis_port, self.model_type, self.model,
                                   self.tf_helper, self.source_name, self.group_name, self.batch)
        self.workers.append(new_worker)
        self.workers_ip.append(new_worker.ip.remote())

    def add_workers(self, number):
        new_workers = [Worker.remote(self.redis_ip, self.redis_port, self.model_type, self.model,
                                     self.tf_helper, self.source_name, self.group_name, self.batch) for _ in range(number)]
        self.workers.extend(new_workers)
        self.workers_ip.extend([new_worker.ip.remote() for new_worker in new_workers])

    def delete_worker(self, ip):
        index = self.workers_ip.index(ip)
        if index.__eq__(None):
            ray.logger.warning("The Worker:" + str(ip) + " is not exist.")
        else:
            ray.actor.exit_actor(ray.get(self.workers[index]))
            self.workers.remove(self.workers[index])
            self.workers_ip.remove(ip)
            ray.logger.info("Delete Worker:" + ip + " success.")

    def delete_workers(self, ips):
        for ip in ips:
            self.delete_worker(ip)


