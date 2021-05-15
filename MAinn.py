import sys
from threading import Thread, Lock, Condition
import time

from random import random, randrange
import colored
from colored import stylize
import pandas as pd
import csv
import os
import threading
import random

# ============= Utilities for MySQL =============================

from datetime import datetime
import pymysql
from DBUtils.PooledDB import PooledDB  # pip3 install DBUtils==1.3

import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test123"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS so")
mycursor.execute("use so")
mycursor.execute(
    "CREATE TABLE IF NOT EXISTS lead(id_file INT AUTO_INCREMENT PRIMARY KEY, lead_id INT, nombre VARCHAR(255), telefono VARCHAR(255), fecha VARCHAR(255), ciudad VARCHAR(255), productor_id INT, fechahora_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
# mycursor.execute("CREATE TABLE IF NOT EXISTS copy_lead(id_file INT AUTO_INCREMENT PRIMARY KEY, lead_id INT, nombre VARCHAR(255), telefono VARCHAR(255), fecha VARCHAR(255), ciudad VARCHAR(255), productor_id INT, fechahora_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
mycursor.execute(
    "CREATE TABLE IF NOT EXISTS comprador(compra_id INT AUTO_INCREMENT PRIMARY KEY, id_file INT, comprador INT, monto INT, fechahora TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")

# CONSTRAINT fk_lead FOREIGN KEY(id_file) REFERENCES lead(id_file)
# Information required to create a connection object
dbServerIP = "0.0.0.0"  # IP address of the MySQL database server
dbUserName = "root"  # User name of the MySQL database server
dbUserPassword = "test123"  # Password for the MySQL database user
databaseToUse = "so"  # Name of the MySQL database to be used
charSet = "utf8mb4"  # Character set
cusrorType = pymysql.cursors.DictCursor
Crawl_Info_Count = int(sys.argv[1])

mySQLConnectionPool = PooledDB(creator=pymysql,
                               # Python function returning a connection or a Python module, both based on DB-API 2
                               host=dbServerIP,
                               user=dbUserName,
                               password=dbUserPassword,
                               database=databaseToUse,
                               autocommit=True,
                               charset=charSet,
                               cursorclass=cusrorType,
                               blocking=False,
                               # maxconnections = Crawl_Info_Count
                               )


# ============= End of utilities for MySQL ============================= 

class Personas:
    def __init__(self, idP, nameP, phoneP, date, cityP):
        self.idP = idP
        self.nameP = nameP
        self.phoneP = phoneP
        self.date = date
        self.cityP = cityP


class Produced:
    def __init__(self, idP, idC, fecha, bid):
        self.idP = idP
        self.idC = idC
        self.fecha = fecha
        self.bid = bid


df2 = pd.read_csv('personas.csv')  # Esto sí va así quemado
idp = df2.id
namep = df2.nombre
phonep = df2.telefono
date = df2.fecha
cityp = df2.ciudad
personas = []


class Compradores:
    def __init__(self, idC, compradorC, low, high):
        self.idC = idC
        self.low = low
        self.high = high
        self.compradorC = compradorC


compradores = []
try:
    df = pd.read_csv(str(sys.argv[3]))
    col1 = df.id
    for a in range(len(col1)):
        print(col1[a])
    col2 = df.comprador
    col3 = df.bid_min
    col4 = df.bid_max
    for a in range(len(col1)):
        compradores.append(Compradores(col1[a], col2[a], col3[a], col4[a]))
except:
    print("File not Found or Error in File Format")

for b in range(len(idp)):
    personas.append(Personas(int(idp[b]), namep[b], phonep[b], date[b], cityp[b]))

queue = []
produced = []
CAPACITY = int(sys.argv[1])

qlock = Lock()
item_ok = Condition(qlock)
space_ok = Condition(qlock)
timeConsumer = []
timeProducer = []
timeAll = []


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
        timeach = []

        while True:
            qlock.acquire()

            while len(queue) >= CAPACITY:
                print(stylize('queue is full, stop producing', colored.fg(mycolor)))
                space_ok.wait()

                if len(queue) >= CAPACITY:
                    print(stylize('oops, someone filled the space before me', colored.fg(mycolor)))

            if personas:  # si todavia existen registros, produzco
                startTimeP = time.time()  # INICIO TIEMPO
                person = personas.pop(0)
                print(stylize(str(person.idP) + ' ' + str(person.date) + ' ' + str(UPid), colored.fg(mycolor)))
                queue.append(person)  # insertar a mysql

                # =========== MySQL Space ==================

                try:
                    now = datetime.now()
                    dbConnection_in = mySQLConnectionPool.connection()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                    sqlInsertLead = "INSERT INTO lead (lead_id, nombre, telefono, fecha, ciudad, productor_id, fechahora_ingesta) values ('{}','{}','{}','{}','{}','{}','{}')".format(
                        int(person.idP), str(person.nameP), str(person.phoneP), str(person.date), str(person.cityP),
                        int(UPid), formatted_date)
                    # Obtain a cursor object
                    mySQLCursor = dbConnection_in.cursor()

                    # Execute the SQL stament
                    mySQLCursor.execute(sqlInsertLead)

                    # Close the cursor and connection objects
                    mySQLCursor.close()
                    dbConnection_in.close()

                except Exception as e:
                    print("Exception: %s" % e)
                    # return

                # ============ End MySQL Space =====================
                endTimeP = time.time()
                timeTakenP = endTimeP - startTimeP
                timeach.append(timeTakenP)
                timeAll.append(timeTakenP)
                print(stylize("ROWS PRODUCED: " + str(len(timeach)) + " " + mycolor, colored.fg(mycolor)))
                AverageTimeP = sum(timeach) / len(timeach)
                print(stylize("AVG TIME TAKEN TO PRODUCE: " + str(AverageTimeP) + " " + mycolor, colored.fg(mycolor)))
                totalTime = sum(timeAll)
                print("TOTAL PROGRAM TIME: " + str(totalTime))

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
    def __init__(self, mycomprador, myminbid, mymaxbid, mycolor):
        super(ConsumerThread, self).__init__()

        self.mycomprador = mycomprador
        self.myminbid = myminbid
        self.mymaxbid = mymaxbid
        self.name = mycolor

    def run(self):
        global queue
        global produced
        global personas
        global timeAll
        timeachC = []
        mycolor = self.name

        mycomprador = self.mycomprador
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
                startTimeC = time.time()
                finalbid = random.randrange(myminbid, mymaxbid)  # Crear lead
                person = queue.pop(0)

                # sqlCopyLead = "INSERT INTO copy_lead(lead_id, nombre, telefono, fecha, ciudad, productor_id, fechahora_ingesta) SELECT lead_id, nombre, telefono, fecha, ciudad, productor_id, fechahora_ingesta FROM lead LIMIT 1"

                # ================ MySQL Space ==================

                # try:
                now = datetime.now()
                dbConnection_comprador = mySQLConnectionPool.connection()
                formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                mycursor.execute("SELECT id_file FROM lead LIMIT 1")
                my_lead = mycursor.fetchone()

                sqlDropLead = "DELETE FROM lead LIMIT 1"

                sqlInsertComprador = "INSERT INTO comprador (compra_id, id_file, comprador, monto, fechahora) values ('{}','{}','{}', '{}','{}')".format(
                    random.sample((1, 9999999), 1), person.idP, mycomprador, finalbid, formatted_date)
                # Obtain a cursor object
                mySQLCursorComprador = dbConnection_comprador.cursor()

                # Execute the SQL stament
                # mySQLCursorComprador.execute(sqlCopyLead)
                mySQLCursorComprador.execute(sqlDropLead)
                mySQLCursorComprador.execute(sqlInsertComprador)

                # Close the cursor and connection objects
                mySQLCursorComprador.close()
                dbConnection_comprador.close()

                # except Exception as e:
                #    print("Exception: %s" % e)
                # return

                # ================ End MySQL Space ==================

                # Sacar de Mysql

                # with open('comprador.csv', 'a+') as final:
                #    writer = csv.writer(final)
                #    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                #    final.close() # Cerrar coneccion a archivo para que otros threads la puedan usar

                produced.append(Produced(person.idP, mycomprador, person.date, finalbid))  # meter a archivo comprador
                print(stylize("CONSUMED", colored.fg(mycolor)))
                endTimeC = time.time()
                timeTakenC = endTimeC - startTimeC
                timeAll.append(timeTakenC)
                timeachC.append(timeTakenC)
                print(stylize("ROWS CONSUMED: "+ str(len(timeachC)) + " " + mycolor, colored.fg(mycolor)))
                totalTime = sum(timeAll)
                print("TOTAL PROGRAM TIME: " + str(totalTime))
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
                    startTimeC = time.time()
                    finalbid = random.randrange(myminbid, mymaxbid)
                    person = queue.pop(0)
                    # with open('comprador.csv', 'a+') as final:
                    #    writer = csv.writer(final)
                    #    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                    #    final.close() # Cerrar coneccion a archivo para que otros threads la puedan usar

                    produced.append(Produced(person.idP, mycomprador, person.date, finalbid))

                    print(stylize("CONSUMED", colored.fg(mycolor)))
                    endTimeC = time.time()
                    timeTakenC = endTimeC - startTimeC
                    timeAll.append(timeTakenC)
                    timeachC.append(timeTakenC)
                    print(stylize("ROWS CONSUMED: " + str(len(timeachC)) + " " + mycolor, colored.fg(mycolor)))
                    totalTime = sum(timeAll)
                    print("TOTAL PROGRAM TIME: " + str(totalTime))
                    print(stylize(len(queue), colored.fg(mycolor)))
                    print(len(produced))
                    space_ok.notify()
                    qlock.release()
                    time.sleep(1)


# COSAS NUEVAS
llenando = True  # IMPORTANTE, AGREGAR
producidos = []
consumidos = []


class ProducerThreadAlternance(Thread):
    def __init__(self, color, ProducerID):
        super(ProducerThreadAlternance, self).__init__()
        self.name = color
        self.ProducerID = ProducerID

    def run(self):
        global queue
        global llenando
        global personas
        timeachP = []
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
                            startTimep = time.time()
                            person = personas.pop(0)

                            print(stylize(str(person.idP) + ' ' + str(person.date), colored.fg(mycolor)))
                            queue.append(person)  # <- insertar a mysql

                            # =========== MySQL Space ==================

                            try:
                                now = datetime.now()
                                dbConnection_in = mySQLConnectionPool.connection()
                                formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                                sqlInsertLead = "INSERT INTO lead (lead_id, nombre, telefono, fecha, ciudad, productor_id, fechahora_ingesta) values ('{}','{}','{}','{}','{}','{}','{}')".format(
                                    int(person.idP), str(person.nameP), str(person.phoneP), str(person.date),
                                    str(person.cityP),
                                    int(UPid), formatted_date)
                                # Obtain a cursor object
                                mySQLCursor = dbConnection_in.cursor()

                                # Execute the SQL stament
                                mySQLCursor.execute(sqlInsertLead)

                                # Close the cursor and connection objects
                                mySQLCursor.close()
                                dbConnection_in.close()

                            except Exception as e:
                                print("Exception: %s" % e)
                                # return

                            # ============ End MySQL Space =====================
                            endTimeP = time.time()
                            timeTakenP = endTimeP - startTimep
                            timeachP.append(timeTakenP)
                            timeAll.append(timeTakenP)

                            AverageTimeTakenP = sum(timeachP) / len(timeachP)
                            TotalTime = sum(timeAll)

                            print(stylize("ROWS PRODUCED: " + str(len(timeachP)) + " " + mycolor, colored.fg(mycolor)))
                            print(stylize("Average Time taken to Produce: " + str(AverageTimeTakenP) + " " + mycolor, colored.fg(mycolor)))
                            print("Total Time: " + str(TotalTime))
                            print(stylize(len(queue), colored.fg(mycolor)))
                            item_ok.notify()  # notificar
                            qlock.release()  # soltar

                            time.sleep(1)
                        else:

                            item_ok.wait()  # Dejar de producir si ya no hay registros
                            item_ok.notify()
                            qlock.release()
                            time.sleep(1)


class ConsumerThreadAlternance(Thread):
    def __init__(self, mycomprador, myminbid, mymaxbid, mycolor):
        super(ConsumerThreadAlternance, self).__init__()
        self.mycomprador = mycomprador
        self.myminbid = myminbid
        self.mymaxbid = mymaxbid
        self.name = mycolor

    def run(self):
        global queue
        global llenando
        global produced
        global personas
        timeachC = []
        mycolor = self.name
        mycomprador = self.mycomprador
        myminbid = self.myminbid
        mymaxbid = self.mymaxbid

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
                    startTimeC = time.time()
                    finalbid = randrange(myminbid, mymaxbid)  # Crear lead
                    person = queue.pop(0)  # Sacar de Mysql

                    # ================ MySQL Space ==================

                    # try:
                    now = datetime.now()
                    dbConnection_comprador = mySQLConnectionPool.connection()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                    mycursor.execute("SELECT id_file FROM lead LIMIT 1")
                    my_lead = mycursor.fetchone()

                    sqlDropLead = "DELETE FROM lead LIMIT 1"

                    sqlInsertComprador = "INSERT INTO comprador (compra_id, id_file, comprador, monto, fechahora) values ('{}','{}','{}', '{}','{}')".format(
                        random.sample((1, 9999999), 1), person.idP, mycomprador, finalbid, formatted_date)
                    # Obtain a cursor object
                    mySQLCursorComprador = dbConnection_comprador.cursor()

                    # Execute the SQL stament
                    # mySQLCursorComprador.execute(sqlCopyLead)
                    mySQLCursorComprador.execute(sqlDropLead)
                    mySQLCursorComprador.execute(sqlInsertComprador)

                    # Close the cursor and connection objects
                    mySQLCursorComprador.close()
                    dbConnection_comprador.close()

                    # except Exception as e:
                    #    print("Exception: %s" % e)
                    # return

                    # ================ End MySQL Space ==================

                    # with open('comprador.csv', 'a+') as final:
                    #    writer = csv.writer(final)
                    #    writer.writerow([person.idP, mycomprador, person.date, finalbid, mycolor])
                    #    final.close()  # Cerrar coneccion a archivo para que otros threads la puedan usar

                    produced.append(
                        Produced(person.idP, mycomprador, person.date, finalbid))  # meter a archivo comprador

                    print(stylize("CONSUMED", colored.fg(mycolor)))
                    endTimeC = time.time()
                    timeTakenC = endTimeC - startTimeC
                    timeachC.append(timeTakenC)
                    timeAll.append(timeTakenC)
                    TotalTime = sum(timeAll)

                    print(stylize("ROWS PRODUCED: " + str(len(timeachC)) + " " + mycolor, colored.fg(mycolor)))
                    print("Total Time: " + str(TotalTime))
                    print(len(produced))
                    print(len(queue))
                    space_ok.notify()
                    qlock.release()

                    time.sleep(1)


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
            print("A")
            consumer = ConsumerThread(i.idC, i.low, i.high, "blue")
            print("B")
            consumerList.append(consumer)
            print("C")
            consumer.start()
            print("D")
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
            consumer = ConsumerThreadAlternance(i.idC, i.low, i.high, "blue")
            consumerList.append(consumer)
            consumer.start()
    except:
        print("No consumer file given, only producer threads will be generated")

# print("DONE")
# ConsumerThread(name='yellow', daemon=True).start()

# for a in compradores:
# print(a.idC, a.low, a.high)

# for c in personas:
# print(c.idP, c.date)
