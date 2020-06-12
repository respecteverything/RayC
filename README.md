# RayC
Using Ray to do cluster serving

## Instruction
Using conda is strongly recommended!
### Environment
* Python 3.6 and above
* Ray 0.8 and above
* Redis 5.0 and above
* TensorFlow 1.14
* PyTorch 1.5
* Analytics-Zoo 0.8

### Run Driver
* Step one: prepare the conda envs in your cluster and make sure each envs have the same version of python and pip-package.
* Step two: choose one machine as driver. Then run `ray start --head --redis-port=8888`.The redis-port is using by ray so it can't be the same with your redis port.
* Step three: go to other machines and run `ray start --address=<address> --resource='{"worker":1}'`. The address is your driver's ip and drivers's redis-port(e.g. step-two's 8888) . And `recources` is the number of workers in each machine which we recommend you use 1 because of the maximum performance. 
* Step four: go back to driver and run `python demo.py --model_path /path/to/your/model --model_type tf`. Then you can see the log in your terminal. 

Note: The demo is focus on image_classification so the args have long, wide and channel.
If you want to use other kind of input, just modify these args.

### Input images

Run `python Client.py --img_path /path/to/your/img`. Then you would see the return.
