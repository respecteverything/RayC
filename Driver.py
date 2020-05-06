import sys
import redis
import os
import ray
from Worker import Worker
import time


class Driver(object):
    def __init__(self, ip, port, model_type, model_path):
        self.ip = ip
        self.port = port
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.model_helper = None
        self.workers = []
        self.workers_ip = []
        self.redis = redis.Redis(host=self.ip, port=self.port, db=0)

    def init_redis(self):
        group_name = "consumer"
        self.redis.xgroup_create("source", group_name, id=0, mkstream=True)

    def load_model(self):
        if self.model_type == "tf":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    if ".pb" in file:
                        model_path = maindir + "/" + file
                    if ".json" in file:
                        json_path = maindir + file

            file = open(model_path, 'rb')
            import json
            with open(json_path, 'r') as file_2:
                self.model_helper = json.load(file_2)
            self.model = file.read()
        elif self.model_type == "torch":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    if ".pt" in file:
                        model_path = maindir + "/" + file
                file = open(model_path, 'rb')
                self.model = file.read()
        elif self.model_type == "bigdl":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    if ".model" in file:
                        model_path = maindir + "/" + file
                file = open(model_path, 'rb')
                self.model = file.read()
        else:
            print("Currently Not support " + self.model_type)
            print("Please check on the file page.")

    def add_worker(self):
        new_worker = Worker.remote(self.ip, self.port,self.model_type, self.model, self.model_helper)
        self.workers.append(new_worker)
        self.workers_ip.append(new_worker.ip.remote())

    def add_workers(self, number):
        new_workers = [Worker.remote(self.ip, self.port, self.model_type, self.model, self.model_helper) for _ in range(number)]
        self.workers.extend(new_workers)
        self.workers_ip.extend([new_worker.ip.remote() for new_worker in new_workers])

    def delete_worker(self, ip):
        index = self.workers_ip.index(ip)
        if index.__eq__(None):
            print("wrong")
        else:
            ray.actor.exit_actor(ray.get(self.workers[index]))
            self.workers.remove(self.workers[index])
            self.workers_ip.remove(ip)

    def delete_workers(self, ips):
        for ip in ips:
            self.delete_worker(ip)

    def run(self):
        length = self.redis.xlen("source")
        if length.__eq__(0):
            time.sleep(1)
        else:
            avg = length // len(self.workers) + 1
            info = ray.get([worker.predict.remote(avg) for worker in self.workers])
            print(info)

