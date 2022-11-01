import time
import threading 

def racy_update_thread_func(client, map_name, key):
    counting_map = client.get_map(map_name).blocking()
    counting_map.put_if_absent(key, 1)
    for _ in range(10000):
        value = counting_map.get(key)
        counting_map.put(key, value + 1)

def pessimistic_locking_thread_func(client, map_name, key):
    counting_map = client.get_map(map_name).blocking()
    counting_map.put_if_absent(key, 1)
    for _ in range(10000):
        counting_map.lock(key)
        try:
            value = counting_map.get(key)
            counting_map.put(key, value + 1)
        finally:
            counting_map.unlock(key)

def optimistic_locking_thread_func(client, map_name, key):
    counting_map = client.get_map(map_name).blocking()
    counting_map.put_if_absent(key, 1)
    for _ in range(10000):
        while True:
            value = counting_map.get(key)
            if counting_map.replace_if_same(key, value, value + 1):
                break

def atomic_long_counter_thread_func(client, name='counter'):
    counter = client.cp_subsystem.get_atomic_long(name).blocking()
    for _ in range(10000):
        counter.increment_and_get()

def get_threading(thread_func, clients, name='counting-map', key='count'):
    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=thread_func, args=(clients[i], name, key)))
        threads[i].start()

    for i in range(10):
        threads[i].join()

def locking_map(type, clients, map_name='counting-map', key='count'):
    if type == 'racy_update':
        start = time.time()
        get_threading(racy_update_thread_func, clients, map_name, key)
        res_time = time.time() - start
    elif type == 'pessimistic_locking':
        start = time.time()
        get_threading(pessimistic_locking_thread_func, clients, map_name, key)
        res_time = time.time() - start
    elif type == 'optimistic_locking':
        start = time.time()
        get_threading(optimistic_locking_thread_func, clients, map_name, key)
        res_time = time.time() - start
    return res_time

def atomic_long_counter(clients):
    start = time.time()
    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=atomic_long_counter_thread_func, args=(clients[i], 'counter',)))
        threads[i].start()

    for i in range(10):
        threads[i].join()
    res_time = time.time() - start
    return res_time