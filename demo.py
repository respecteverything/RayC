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
    # input_size = sys.argv[6]
    tf_input = None
    tf_output = None
    long = sys.argv[8]
    wide = sys.argv[9]
    chennel = sys.argv[10]
    input_size = (int(chennel), int(long), int(wide))
    if model_type == 'tf':
        tf_input = sys.argv[6]
        tf_output = sys.argv[7]
        input_size = (int(long), int(wide), int(chennel))
    ray.init(address=ray_head)
    driver = Driver(redis_host, redis_port, model_type, model,
                    input_size=input_size, tf_input=tf_input, tf_output=tf_output)
    driver.load_model()
    driver.add_worker()
    while True:
        driver.info()

