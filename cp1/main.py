import hazelcast
from counters import locking_map as lm
from counters import atomic_long_counter as alc 

clients = [hazelcast.HazelcastClient() for _ in range(10)]

map_name = 'counting-map'
counting_map = clients[0].get_map(map_name).blocking()

atomic_name = 'counter'
counter = clients[0].cp_subsystem.get_atomic_long(atomic_name).blocking()
counter.set(0)

for type in ['racy_update', 'optimistic_locking', 'pessimistic_locking', 'atomic_long_counter']:
    if type == 'atomic_long_counter':
        time = alc(clients)
        print('{}: time = {:.3f}, count = {}'.format(type, time, counter.get()))
        continue
    counting_map.put('count', 0)
    time = lm(type, clients)
    print('{}: time = {:.3f}, count = {}'.format(type, time, counting_map.get('count')))

for client in clients:
    client.shutdown()