import pandas as pd
import os
import sys
from threading import Thread, Lock
import random
import time
import threading

buffSize = int(sys.argv[1])
productores = int(sys.argv[2])
consumerFile = sys.argv[3]
alternancia = sys.argv[4]

#Queque = [buffSize]

queue = []
MAX_SIZE = buffSize # Cambiar esto por lo que se ingresa
cv = threading.Condition()

def producer():
    nums = range(5)
    global queue
    while True:
        num = random.choice(nums)

        cv.acquire()

        while len(queue) >= MAX_SIZE:
                cv.wait()

        queue.append(num)
        print("Produced", num, queue)

        cv.notify()

        cv.release()

        time.sleep(random.randrange(0, 3))

def consumer():
    global queue
    while True:

        cv.acquire()

        while len(queue) < 1:
                cv.wait()

        num = queue.pop(0)
        print("Consumed", num, queue)

        cv.notify()

        cv.release()

        time.sleep(random.randrange(0, 3))

producerThread = threading.Thread(target=producer)
consumerThread = threading.Thread(target=consumer)

producerThread.start()
consumerThread.start()