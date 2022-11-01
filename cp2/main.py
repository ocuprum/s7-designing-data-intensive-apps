import psycopg2
from counters import count

dbname, user, password = 'Counter', 'admin', 'password'
conn = psycopg2.connect(host='localhost', database=dbname, user=user, password=password)

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS User_Counter;")
cur.execute("CREATE TABLE User_Counter (USER_ID serial PRIMARY KEY, Counter INTEGER NOT NULL, Version INTEGER);")
cur.execute("INSERT INTO User_Counter (Counter, Version) VALUES (%s, %s)", (1, 0))
conn.commit()

for func in ['lost_update', 'in_place_update', 
             'row_level_locking', 'optimistic_concur_control']: 
    cur.execute("UPDATE User_Counter SET Counter = %s WHERE USER_ID = %s;", (0, 1))
    conn.commit()
    end = count(func, (dbname, user, password))
    cur.execute("SELECT Counter FROM User_Counter WHERE USER_ID = 1")
    counter = cur.fetchone()[0]
    print("{}: time = {}, count = {}".format(func, end, counter))

cur.close()
conn.close()