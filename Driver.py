import sys
import redis
import os
import ray
import Worker

class Driver(object):
    def __init__(self, ip, port, model_type, model_path):
        self.ip = ip
        self.port = port
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.model_helper = None

    def __init__(self,file):
        file = open(file, 'r')


    def init_redis(self):
        r = redis.Redis(host=self.ip, port=self.port, db=0)
        group_name = "consumer"
        r.xgroup_create("source", group_name, id=0)

    def load_model(self):
        if self.model_type == "tf":
            for maindir, subdir, mainfiles in os.walk(self.model_path):
                for file in mainfiles:
                    if ".pb" in file:
                        model_path = maindir + "/" + file
                    if ".json" in file:
                        json_path = maindir + file

            file_1= open(model_path, 'rb')
            import json
            with open(json_path,'r') as file_2:
                self.model_helper = json.load(file_2)
            self.model = file_1.read()


    # def init_ray(self):
    #     ray.init(address="",)

    def ray_info(self):
        if ray.is_initialized():
            ray.cluster_resource()
        else:
            # throw error

    def init_worker(self):
        workers = [Worker.remote(self.ip, self.port,self.model_type, self.model, self.model_helper) for i in range(2)]
