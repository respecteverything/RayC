import time
import sys


def timing(func):
    def wrapper(*args):
        begin = int(round(time.time() * 1000))
        print("executing", func.__name__)
        result = func(*args)
        end = int(round(time.time() * 1000))
        print("executed " + func.__name__ + ", time consumed:%d" % (end - begin))
        return result
    return wrapper

