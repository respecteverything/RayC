import Driver
import Worker
import sys

if __name__ == "__main__":
    print(sys.argv)
    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    model = sys.argv[3]
    driver = Driver(redis_host, redis_port, "tf", model)
    driver.init_redis()
    driver.load_model()
    driver.add_workers(2)
    driver.run()
