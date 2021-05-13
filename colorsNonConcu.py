from threading import Thread, Lock, Condition
import time
from random import random, randint
import colored
from colored import stylize

queue = []
CAPACITY = 8

qlock = Lock()
item_ok = Condition(qlock)
space_ok = Condition(qlock)
HoldP = True


class ProducerThread(Thread):
    def run(self):
        global queue
        mycolor = self.name
        while True:
            if not queue:
                qlock.acquire()
                if len(queue) == CAPACITY:
                    print(stylize('queue is full, stop producing', colored.fg(mycolor)))

                    space_ok.wait()
                    item_ok.notify()
                    qlock.release()
                    if len(queue) >= CAPACITY:
                        print(stylize('oops, someone filled the space before me', colored.fg(mycolor)))
                item = chr(ord('A') + randint(0, 25))
                print(stylize('[' + ' '.join(queue) + '] <= ' + item, colored.fg(mycolor)))
                queue.append(item)
                item_ok.notify()
                qlock.release()
                time.sleep((random() + 1) * 1.5)


class ConsumerThread(Thread):
    def run(self):
        global queue
        mycolor = self.name
        while len(queue) <= CAPACITY:
            if queue:
                qlock.acquire()
                if not queue:
                    print(stylize('queue is empty, stop consuming', colored.fg(mycolor)))
                    item_ok.wait()
                    if not queue:
                        print(stylize('oops, someone consumed the food before me', colored.fg(mycolor)))
                item = queue.pop(0)
                print(stylize(item + ' <= [' + ' '.join(queue) + ']', colored.fg(mycolor)))
                space_ok.notify()
                qlock.release()
                time.sleep((random() + 1) * 1.5)


if HoldP:
    ProducerThread(name='red', daemon=True).start()
    ProducerThread(name='green', daemon=True).start()
else:
    ConsumerThread(name='yellow', daemon=True).start()
    ConsumerThread(name='yellow', daemon=True).start()

try:
    while True:
        time.sleep(3)
except KeyboardInterrupt:
    print("Exiting")
