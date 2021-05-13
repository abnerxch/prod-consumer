from threading import Thread, Condition
import time
import random
import sys

queue = []
MAX_NUM = int(sys.argv[1])
condition = Condition()
ProducerNumber = 0


# Alternance

class ProducerThreadConcurrent(Thread):
    def run(self):
        nums = range(5)
        global queue
        myname = self.name
        while True:
            condition.acquire()
            if len(queue) == MAX_NUM:
                print("Queue full, producer is waiting")
                condition.wait()
                print("Space in queue, Consumer notified the producer")
            num = random.choice(nums)
            queue.append(num)
            print("Produced", num, myname)
            condition.notify()
            condition.release()
            time.sleep(random.random())


class ConsumerThreadConcurrent(Thread):
    def run(self):
        global queue
        myname = self.name
        while True:
            condition.acquire()
            if not queue:
                print("Nothing in queue, consumer is waiting")
                condition.wait()
                print("Producer added something to queue and notified the consumer")
            num = queue.pop(0)
            print("Consumed", num, myname)
            condition.notify()
            condition.release()
            time.sleep(random.random())


class ProducerThreadNonConcurrent(Thread):
    def run(self):
        nums = range(5)
        global queue
        myname = self.name
        while True:
            condition.acquire()
            if len(queue) == MAX_NUM:
                print("Queue full, producer is waiting")
                condition.notify()
                condition.release()
                condition.wait()
                print("Space in queue, Consumer notified the producer")
            num = random.choice(nums)
            queue.append(num)
            print("Produced", num, myname)
            time.sleep(random.random())


class ConsumerThreadNonConcurrent(Thread):
    def run(self):
        global queue
        while True:
            condition.acquire()
            if not queue:
                print("Nothing in queue, consumer is waiting")
                condition.notify()
                condition.release()
                condition.wait()
                print("Producer added something to queue and notified the consumer")
            num = queue.pop(0)
            print("Consumed", num)

            time.sleep(random.random())


if int(sys.argv[4]) == 1:
    for a in range(int(sys.argv[2])):
        ProducerThreadConcurrent(name=str(a)).start()
        print("thread created")
    ConsumerThreadConcurrent().start()
else:

    ProducerThreadNonConcurrent(name=str(1), daemon=True).start()
    ProducerThreadNonConcurrent(name=str(2), daemon=True).start()
    ConsumerThreadNonConcurrent().start()
    ConsumerThreadNonConcurrent().start()