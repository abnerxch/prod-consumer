import sys
from threading import Thread, Lock, Condition
import time
import random
import colored
from colored import stylize
import pandas as pd
import csv
import os
import threading

# ============= Utilities for MySQL =============================

from datetime import datetime
import pymysql
from DBUtils.PooledDB import PooledDB # pip3 install DBUtils==1.3

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="test123"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS so")
mycursor.execute("use so")
mycursor.execute("CREATE TABLE IF NOT EXISTS lead(lead_id INT AUTO_INCREMENT PRIMARY KEY, id_file INT, nombre VARCHAR(255), telefono VARCHAR(255), fecha VARCHAR(255), ciudad VARCHAR(255), productor_id INT, fechahora_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
mycursor.execute("CREATE TABLE IF NOT EXISTS comprador(compra_id INT AUTO_INCREMENT PRIMARY KEY, lead_id INT, FOREIGN KEY(lead_id) REFERENCES lead(lead_id), comprador VARCHAR(255), monto INT, fechahora TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")

# Information required to create a connection object
dbServerIP              = "0.0.0.0"       # IP address of the MySQL database server
dbUserName              = "root"            # User name of the MySQL database server
dbUserPassword          = "test123"                # Password for the MySQL database user
databaseToUse           = "so"    # Name of the MySQL database to be used
charSet                 = "utf8mb4"         # Character set
cusrorType              = pymysql.cursors.DictCursor
Crawl_Info_Count        = int(sys.argv[1])

mySQLConnectionPool = PooledDB(creator   = pymysql,     # Python function returning a connection or a Python module, both based on DB-API 2
                               host      = dbServerIP,
                               user      = dbUserName,
                               password  = dbUserPassword,
                               database  = databaseToUse,
                               autocommit    = True,
                               charset       = charSet,
                               cursorclass   = cusrorType,
                               blocking      = False,
                               #maxconnections = Crawl_Info_Count
                               ) 

# ============= End of utilities for MySQL ============================= 

class Personas:
    def __init__(self, idP, nameP, phoneP, date, cityP):
        self.idP = idP
        self.nameP = nameP
        self.phoneP = phoneP
        self.date = date
        self.cityP = cityP


class Compradores:
    def __init__(self, idC, high, low):
        self.idC = idC
        self.high = high
        self.low = low
        #self.compradorC = compradorC


class Produced:
    def __init__(self, idP, idC, fecha, bid):
        self.idP = idP
        self.idC = idC
        self.fecha = fecha
        self.bid = bid


df2 = pd.read_csv('personas.csv')
idp = df2.id
namep = df2.nombre
phonep = df2.telefono
date = df2.fecha
cityp = df2.ciudad
personas = []
compradores = []
try:
    df = pd.read_csv('compradores.csv')
    print("hhh")
    col1 = df.id
    for a in range(len(col1)):
        print(col1[a])
    print("hhhhhhhhhh")
    col3 = df.bid_max
    for a in range(len(col3)):
        print(col3[a])
    col2 = df.bid_min
    for a in range(len(col2)):
        print(col2[a])
    print("hhhhhhhhhhhhhhhhhhhhhhh")
    
    print("hhhhhhhhhh")
    """col4 = df.comprador
    for a in range(len(col4)):
        print(col4[a])"""
    print("hhhhhhhhhhhhhhhhhhhhhhh")
    for a in range(len(col1)):
        print("aaaaa")
        compradores.append(Compradores(col1[a], col3[a], col2[a]))
        print("==")
    print("hhhhh")
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
                print(stylize(str(person.idP) + ' ' + str(person.date) + ' ' + str(UPid), colored.fg(mycolor)))
                queue.append(person)  # insertar a mysql

                # =========== MySQL Space ==================

                try:
                    now = datetime.now()
                    dbConnection_in = mySQLConnectionPool.connection()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                    sqlInsertLead   = "INSERT INTO lead (id_file, nombre, telefono, fecha, ciudad, productor_id, fechahora_ingesta) values ('{}','{}','{}','{}','{}','{}','{}')".format(int(person.idP), str(person.nameP), str(person.phoneP), str(person.date), str(person.cityP), int(UPid), formatted_date) 
                    # Obtain a cursor object
                    mySQLCursor = dbConnection_in.cursor()      

                    # Execute the SQL stament
                    mySQLCursor.execute(sqlInsertLead)       

                    # Close the cursor and connection objects
                    mySQLCursor.close()
                    dbConnection_in.close() 
                    
                except Exception as e:
                    print("Exception: %s" % e)
                    #return 

                # ============ End MySQL Space =====================

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
    def __init__(self, myid, mycomprador, myminbid, mymaxbid, mycolor, ConsumerID):
        super(ConsumerThread, self).__init__()
        self.myid = myid
        self.mycomprador = mycomprador
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
                finalbid = random.randrange(myminbid, mymaxbid)  # Crear lead
                

                # ================ MySQL Space ==================
                
                #try:
                now = datetime.now()
                dbConnection_comprador = mySQLConnectionPool.connection()
                formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                sqlReadLeadID = "SELECT lead_id FROM lead LIMIT 1"
                sqlDropLead = "DELETE FROM lead LIMIT 1"
                sqlInsertComprador   = "INSERT INTO comprador (lead_id, comprador, monto, fecha_hora) values ('{}','{}','{}', '{}')".format(int(starting_index), str(mycomprador), int(finalbid),formatted_date) 
                    # Obtain a cursor object
                mySQLCursor = dbConnection_comprador.cursor()      

                    # Execute the SQL stament
                mySQLCursor.execute(sqlReadLeadID)
                starting_index = mySQLCursor.fetchone()[0]
                mySQLCursor.execute(sqlDropLead)
                mySQLCursor.execute(sqlInsertComprador)       

                    # Close the cursor and connection objects
                mySQLCursor.close()
                dbConnection_comprador.close() 
                    
                #except Exception as e:
                #    print("Exception: %s" % e)
                #return 

                # ================ End MySQL Space ==================

                person = queue.pop(0)  # Sacar de Mysql

                #with open('comprador.csv', 'a+') as final:
                #    writer = csv.writer(final)
                #    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                #    final.close() # Cerrar coneccion a archivo para que otros threads la puedan usar

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
                    finalbid = random.randrange(myminbid, mymaxbid)
                    person = queue.pop(0)
                    #with open('comprador.csv', 'a+') as final:
                    #    writer = csv.writer(final)
                    #    writer.writerow([person.idP, myid, person.date, finalbid, mycolor])
                    #    final.close() # Cerrar coneccion a archivo para que otros threads la puedan usar

                    produced.append(Produced(person.idP, myid, person.date, finalbid))

                    print(stylize("CONSUMED", colored.fg(mycolor)))
                    print(stylize(len(queue), colored.fg(mycolor)))
                    print(len(produced))
                    space_ok.notify()
                    qlock.release()
                    time.sleep(1)


# buffSize productores consumerFile Alternance
# 1        2           3            4
producerList = []
ColorList = ['green', 'red', 'blue', 'yellow', 'white', 'purple']
for i in range(int(sys.argv[2])):
    producer = ProducerThread(ColorList[i], i)
    producerList.append(producer)
    producer.setDaemon(False)
    producer.start()

[thread.join() for thread in producerList]


try:
    print("trying")
    consumerList = []
    for i in compradores:
        consumer = ConsumerThread(i.idC, i.low, i.high, "blue", i)
        producerList.append(consumer)
        consumer.start()
        [thread.join() for thread in consumerList]
except:
    print("No consumer file given, only producer threads will be generated")
# print("DONE")
# ConsumerThread(name='yellow', daemon=True).start()

# for a in compradores:
# print(a.idC, a.low, a.high)

# for c in personas:
# print(c.idP, c.date)