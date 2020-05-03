import time
import sys


def resize(img, size):
    # img
    img = img.copy()
    temp = img.resize(size)
    return img
