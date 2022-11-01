import time
import psycopg2
import threading

def lost_update(cur, conn):
    for _ in range(10000):
        cur.execute("SELECT Counter FROM User_Counter WHERE USER_ID = 1")
        counter = cur.fetchone()[0] + 1
        cur.execute("UPDATE User_Counter SET Counter = %s WHERE USER_ID = %s", (counter, 1))
        conn.commit()
        
def in_place_update(cur, conn):
    for _ in range(10000):
        cur.execute("UPDATE User_Counter SET Counter = Counter + 1 WHERE USER_ID = %s", (1, ))
        conn.commit()

def row_level_locking(cur, conn):
    for _ in range(10000):
        cur.execute("SELECT Counter FROM User_Counter WHERE USER_ID = 1 FOR UPDATE")
        counter = cur.fetchone()[0] + 1
        cur.execute("UPDATE User_Counter SET Counter = %s WHERE USER_ID = %s", (counter, 1))
        conn.commit()

def optimistic_concur_control(cur, conn):
    for _ in range(10000):
        while True:
            cur.execute("SELECT Counter, Version FROM User_Counter WHERE USER_ID = 1")
            counter, version = cur.fetchone()
            counter += 1
            cur.execute("UPDATE User_Counter SET Counter = %s, Version = %s WHERE USER_ID = %s and Version = %s", (counter, version + 1, 1, version))
            conn.commit()
            row_count = cur.rowcount
            if row_count > 0: break 

def get_threading(thread_func, data):
    threads = []
    dbname, user, password = data

    for i in range(10):
        conn = psycopg2.connect(host='localhost', database=dbname, user=user, password=password)
        cur = conn.cursor()
        threads.append(threading.Thread(target=thread_func, args=(cur, conn)))
        threads[i].start()

    for i in range(10):
        threads[i].join()

def count(name, data):
    if name == 'lost_update': func = lost_update
    elif name == 'in_place_update': func = in_place_update
    elif name == 'row_level_locking': func = row_level_locking
    elif name == 'optimistic_concur_control': func = optimistic_concur_control
    
    start = time.time()
    get_threading(func, data)
    end = time.time() - start

    return end