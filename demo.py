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
    input_size = sys.argv[6]
    tf_input = None
    tf_output = None
    if model_type == 'tf':
        tf_input = sys.argv[7]
        tf_output = sys.argv[8]
    ray.init(address=ray_head)
    driver = Driver(redis_host, redis_port, model_type, model,
                    input_size=input_size, tf_input=tf_input, tf_output=tf_output)
    driver.load_model()
    driver.add_worker()

