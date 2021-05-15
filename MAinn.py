import sys
from threading import Thread, Lock, Condition
import time
from random import random, randrange
import colored
from colored import stylize
import pandas as pd
import csv
import os


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
personas = []
compradores = []
try:
    df = pd.read_csv(str(sys.argv[3]))
    col1 = df.id
    col2 = df.bid_min
    col3 = df.bid_max
    for a in range(len(col1)):
        compradores.append(Compradores(int(col1[a]), int(col3[a]), int(col2[a])))
except:
    print("File not Found or Error in File Format")

for b in range(len(idp)):
    personas.append(Personas(int(idp[b]), date[b]))

tiempoEjec = []
tiempoTotal = []
producidos = []
consumidos = []

queue = []
produced = []
CAPACITY = int(sys.argv[1])

qlock = Lock()
item_ok = Condition(qlock)
space_ok = Condition(qlock)


class ProducerThread(Thread):
    def __init__(self, color, ProducerID):
        super(ProducerThread, self).__init__()
        self.name = color
        self.ProducerID = ProducerID

    def run(self):
        global queue
        global personas
        mycolor = self.name
        UPid = self.ProducerID  # IDENTIFICADOR UNICO

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
                # ANADI ESTO
                sys.exit()
                # ^ANADI ESTO
                qlock.release()

            item_ok.notify()
            qlock.release()
            time.sleep(1)


class ConsumerThread(Thread):
    def __init__(self, myid, myminbid, mymaxbid, mycolor, ConsumerID):
        super(ConsumerThread, self).__init__()
        self.myid = myid
        self.myminbid = myminbid
        self.mymaxbid = mymaxbid
        self.name = mycolor
        self.ConsumerID = ConsumerID

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
                finalbid = randrange(myminbid, mymaxbid)  # Crear lead
                person = queue.pop(0)  # Sacar de Mysql

                with open('comprador.csv', 'a+') as final:
                    writer = csv.writer(final)
                    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                    final.close()  # Cerrar coneccion a archivo para que otros threads la puedan usar

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

                    os._exit(0)
                    item_ok.wait()
                    space_ok.notify()
                    # ANADI ESTO

                    # ^ANADI ESTO
                    qlock.release()

                    print("SHUTTING DOWN THREAD")

                else:  # crear lead
                    finalbid = randrange(myminbid, mymaxbid)
                    person = queue.pop(0)
                    with open('comprador.csv', 'a+') as final:
                        writer = csv.writer(final)
                        writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                        final.close()  # Cerrar coneccion a archivo para que otros threads la puedan usar

                    produced.append(Produced(person.idP, myid, person.date, finalbid))

                    print(stylize("CONSUMED", colored.fg(mycolor)))
                    print(stylize(len(queue), colored.fg(mycolor)))
                    print(len(produced))
                    space_ok.notify()
                    qlock.release()
                    time.sleep(1)


llenando = True


class ProducerThreadAlternance(Thread):
    def __init__(self, color, ProducerID):
        super(ProducerThreadAlternance, self).__init__()
        self.name = color
        self.ProducerID = ProducerID

    def run(self):
        global queue
        global llenando
        global personas
        mycolor = self.name
        UPid = self.ProducerID

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
                    if llenando and personas:
                        if personas:
                            person = personas.pop(0)
                            print(stylize(str(person.idP) + ' ' + str(person.date), colored.fg(mycolor)))
                            queue.append(person)  # <- insertar a mysql
                            print(stylize(len(queue), colored.fg(mycolor)))
                            item_ok.notify()  # notificar
                            qlock.release()  # soltar

                            time.sleep((random() + 1) * 1.5)
                        else:

                            item_ok.wait()  # Dejar de producir si ya no hay registros
                            item_ok.notify()
                            qlock.release()
                            time.sleep((random() + 1) * 1.5)


class ConsumerThreadAlternance(Thread):
    def __init__(self, myid2, myminbid2, mymaxbid2, mycolor2, ConsumerID2):
        super(ConsumerThreadAlternance, self).__init__()
        self.myid2 = myid2
        self.myminbid2 = myminbid2
        self.mymaxbid2 = mymaxbid2
        self.name2 = mycolor2
        self.ConsumerID2 = ConsumerID2

    def run(self):
        global queue
        global llenando
        global produced
        global personas
        mycolor2 = self.name2
        myid2 = self.myid2
        myminbid2 = self.myminbid2
        mymaxbid2 = self.mymaxbid2

        while True:
            while not llenando:
                qlock.acquire()
                if not queue:
                    print(stylize('queue is empty, stop consuming', colored.fg(mycolor2)))
                    llenando = True
                    item_ok.wait()

                    space_ok.notify()
                    qlock.release()
                    if not queue:
                        print(stylize('oops, someone consumed the food before me', colored.fg(mycolor2)))
                if not llenando:
                    finalbid = randrange(myminbid2, mymaxbid2)  # Crear lead
                    person = queue.pop(0)  # Sacar de Mysql

                    with open('comprador.csv', 'a+') as final:
                        writer = csv.writer(final)
                        writer.writerow([person.idP, myid2, person.date, finalbid, mycolor2])
                        final.close()  # Cerrar coneccion a archivo para que otros threads la puedan usar

                    produced.append(Produced(person.idP, myid2, person.date, finalbid))  # meter a archivo comprador

                    print(stylize("CONSUMED", colored.fg(mycolor2)))
                    print(len(produced))
                    print(len(queue))
                    space_ok.notify()
                    qlock.release()

                    time.sleep((random() + 1) * 1.5)


# buffSize productores consumerFile Alternance
# 1        2           3            4
producerList = []
ColorList = ['green', 'red', 'blue', 'yellow', 'white', 'magenta', 'green', 'red', 'blue', 'yellow', 'white', 'magenta',
             'green', 'red', 'blue', 'yellow', 'white', 'magenta']

alternance = int(sys.argv[4])
print("Runs up to this point")


if alternance == 0:
    print("non alternance")
    for i in range(int(sys.argv[2])):
        producer = ProducerThread(ColorList[i], i)
        producerList.append(producer)
        producer.setDaemon(False)
        producer.start()

    try:
        print("trying")
        consumerList = []
        for i in compradores:
            consumer = ConsumerThread(i.idC, i.low, i.high, "blue", i)
            producerList.append(consumer)
            consumer.start()
    except:
        print("No consumer file given, only producer threads will be generated")

if alternance == 1:
    print("alternance")
    for i in range(int(sys.argv[2])):
        producer = ProducerThreadAlternance(ColorList[i], i)
        producerList.append(producer)
        producer.setDaemon(False)
        producer.start()

    try:
        print("trying")
        consumerList = []
        for i in compradores:
            consumer = ConsumerThreadAlternance(i.idC, i.low, i.high, "blue", i)
            producerList.append(consumer)
            consumer.start()
    except:
        print("No consumer file given, only producer threads will be generated")

# print("DONE")
# ConsumerThread(name='yellow', daemon=True).start()

# for a in compradores:
# print(a.idC, a.low, a.high)

# for c in personas:
# print(c.idP, c.date)
