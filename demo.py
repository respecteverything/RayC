from Driver import Driver
import sys
import ray
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Cluster-Serving ray setup")

    parser.add_argument("--redis_host", type=str, default='localhost',
                        help="Redis Ip.")
    parser.add_argument("--redis_port", default=6379, type=int,
                        help="The Redis port.")
    parser.add_argument("--batch_size", default=64, type=int,
                        help="The number of inference to do per batch.")
    parser.add_argument("--model_path", type=str,
                        help="The path to your model")
    parser.add_argument("--ray_head", type=str,
                        help="The IP and port of your ray head, e.g 172.168.0.120:8888")
    parser.add_argument("--model_type", type=str, default='tf',
                        help="The type of your model. Currently we support tf, torch and bigdl.")
    parser.add_argument("--tf_input", type=str, default=None,
                        help="If your model_type isn't tf, you can just ignore this."
                             "If you use tf model, put the input tensor name here")
    parser.add_argument("--tf_output", type=str, default=None,
                        help="If your model_type isn't tf, you can just ignore this."
                             "If you use tf model, put the output tensor name here")
    parser.add_argument("--img_long", type=int, default=224,
                        help="The long of your img.")
    parser.add_argument("--img_wide", type=int, default=224,
                        help="The wide of your img.")
    parser.add_argument("--img_channel", type=int, default=3,
                        help="The number of channels of your img.")

    args = parser.parse_args()

    input_size = (int(args.channel), int(args.long), int(args.wide))
    if args.model_type == 'tf':
        input_size = (int(args.long), int(args.wide), int(args.channel))
    ray.init(address=args.ray_head)
    driver = Driver(args.redis_host, args.redis_port, args.model_type, args.model_path, args.batch_size,
                    args.input_size, args.tf_input, args.tf_output)
    driver.load_model()
    driver.add_worker()
    while True:
        driver.info()

