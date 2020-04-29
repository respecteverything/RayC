from Worker import Driver
import sys
import ray

if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    model = sys.argv[3]
    ray.init(address="172.16.0.120:8888")
    driver = Driver(redis_host, redis_port, "tf", model)
    driver.init_redis()
    driver.load_model()
    driver.add_workers(2)
    while True:
        driver.run()
