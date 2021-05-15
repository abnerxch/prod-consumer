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
llenando = True


class ProducerThread(Thread):
    def run(self):
        global queue
        global llenando
        mycolor = self.name
        while True:
            while llenando:
                qlock.acquire()
                if len(queue) <= CAPACITY:
                    if len(queue) == CAPACITY:
                        print(stylize('queue is full, stop producing', colored.fg(mycolor)))
                        llenando = False
                        space_ok.wait()
                        qlock.release()
                        if len(queue) >= CAPACITY:
                            print(stylize('oops, someone filled the space before me', colored.fg(mycolor)))
                    if llenando:
                        item = chr(ord('A') + randint(0, 25))
                        print(stylize('[' + ' '.join(queue) + '] <= ' + item, colored.fg(mycolor)))
                        queue.append(item)
                        item_ok.notify()
                        qlock.release()
                        time.sleep((random() + 1) * 1.5)


class ConsumerThread(Thread):
    def run(self):
        global queue
        global llenando
        mycolor = self.name
        while True:
            while not llenando:
                qlock.acquire()
                if not queue:
                    print(stylize('queue is empty, stop consuming', colored.fg(mycolor)))
                    llenando = True
                    item_ok.wait()

                    space_ok.notify()
                    qlock.release()
                    if not queue:
                        print(stylize('oops, someone consumed the food before me', colored.fg(mycolor)))
                if not llenando:
                    item = queue.pop(0)
                    print(stylize(item + ' <= [' + ' '.join(queue) + ']', colored.fg(mycolor)))
                    space_ok.notify()
                    qlock.release()
                    time.sleep((random() + 1) * 1.5)



ProducerThread(name='red', daemon=True).start()
ProducerThread(name='green', daemon=True).start()

# ConsumerThread(name='blue', daemon=True).start()
ConsumerThread(name='yellow', daemon=True).start()

try:
    while True:
        time.sleep(3)
except KeyboardInterrupt:
    print("Exiting")
