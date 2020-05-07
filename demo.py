from Driver import Driver
import sys
import ray

if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    model = sys.argv[3]
    ray_head = sys.argv[4]
    model_type = sys.argv[5]
    ray.init(address=ray_head)
    driver = Driver(redis_host, redis_port, model_type, model)
    driver.init_redis()
    driver.load_model()
    driver.add_workers(2)
    while True:
        driver.run()
