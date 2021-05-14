
from threading import Thread, Lock, Condition
import time
import random
import colored
from colored import stylize
import pandas as pd
import csv


class Personas:
    def __init__(self, idP, date):
        self.idP = idP
        self.date = date


class Compradores:
    def __init__(self, idC, high, low):
        self.idC = idC
        self.high = high
        self.low = low


class Produced:
    def __init__(self, idP, idC, fecha, bid):
        self.idP = idP
        self.idC = idC
        self.fecha = fecha
        self.bid = bid


df2 = pd.read_csv('personas.csv')
idp = df2.id
date = df2.fecha

df = pd.read_csv('compradores.csv')
col1 = df.id
col2 = df.bid_min
col3 = df.bid_max

personas = []
compradores = []

for a in range(len(col1)):
    compradores.append(Compradores(int(col1[a]), int(col3[a]), int(col2[a])))

for b in range(len(idp)):
    personas.append(Personas(int(idp[b]), date[b]))

queue = []
produced = []
CAPACITY = 100

qlock = Lock()
item_ok = Condition(qlock)
space_ok = Condition(qlock)


class ProducerThread(Thread):
    def run(self):
        global queue
        global personas
        mycolor = self.name

        while True:
            qlock.acquire()

            while len(queue) >= CAPACITY:
                print(stylize('queue is full, stop producing', colored.fg(mycolor)))
                space_ok.wait()

                if len(queue) >= CAPACITY:
                    print(stylize('oops, someone filled the space before me', colored.fg(mycolor)))

            if personas:  # si todavia existen registros, produzco
                person = personas.pop(0)
                print(stylize(str(person.idP) + ' ' + str(person.date), colored.fg(mycolor)))
                queue.append(person)  # insertar a mysql
                print(stylize(len(queue), colored.fg(mycolor)))
            else:
                item_ok.wait()  # Dejar de producir si ya no hay registros
                item_ok.notify()
                qlock.release()

            item_ok.notify()
            qlock.release()
            time.sleep(0)


class ConsumerThread(Thread):
    def __init__(self, myid, myminbid, mymaxbid, mycolor):
        super(ConsumerThread, self).__init__()
        self.myid = myid
        self.myminbid = myminbid
        self.mymaxbid = mymaxbid
        self.name = mycolor

    def run(self):
        global queue
        global produced
        global personas

        mycolor = self.name
        myid = self.myid
        myminbid = self.myminbid
        mymaxbid = self.mymaxbid

        while True:
            if personas:
                qlock.acquire()
                while not queue:
                    print(stylize('queue is empty, stop consuming', colored.fg(mycolor)))
                    item_ok.wait()
                    if not queue:
                        print(stylize('oops, someone consumed the food before me', colored.fg(mycolor)))
                finalbid = random.randrange(myminbid, mymaxbid)  # Crear lead
                person = queue.pop(0)  # Sacar de Mysql

                with open('comprador.csv', 'a+') as final:
                    writer = csv.writer(final)
                    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                    final.close()

                produced.append(Produced(person.idP, myid, person.date, finalbid))  # meter a archivo comprador
                print(stylize("CONSUMED", colored.fg(mycolor)))
                print(len(produced))
                print(len(queue))
                space_ok.notify()
                qlock.release()
                time.sleep(1)

            if not personas:
                qlock.acquire()
                if not queue:  # apagar thread si ya no hay nada en el buffer
                    print(stylize('queue is empty, and producer is stopped, thread shutting down...',
                                  colored.fg(mycolor)))

                    item_ok.wait()
                    space_ok.notify()
                    qlock.release()
                    print("SHUTTING DOWN THREAD")

                else:  # crear lead
                    finalbid = random.randrange(myminbid, mymaxbid)
                    person = queue.pop(0)
                    with open('comprador.csv', 'a+') as final:
                        writer = csv.writer(final)
                        writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                        final.close()

                    produced.append(Produced(person.idP, myid, person.date, finalbid))

                    print(stylize("CONSUMED", colored.fg(mycolor)))
                    print(stylize(len(queue), colored.fg(mycolor)))
                    print(len(produced))
                    space_ok.notify()
                    qlock.release()
                    time.sleep(1)


producerList = []
ColorList = ['green', 'red']
for i in range(2):
    producer = ProducerThread(name=ColorList[i], daemon=True)
    producerList.append(producer)
    producer.start()

consumerList = []
for i in compradores:
    consumer = ConsumerThread(i.idC, i.low, i.high, "blue")
    producerList.append(consumer)
    consumer.start()

[producer.join() for producer in producerList]
[consumer.join() for consumer in consumerList]

print("DONE")
# ConsumerThread(name='yellow', daemon=True).start()

# for a in compradores:
# print(a.idC, a.low, a.high)

# for c in personas:
# print(c.idP, c.date)
