#!/usr/bin/env python

import dataclasses
import multiprocessing
import multiprocessing.shared_memory
import numpy
import pandas
import psycopg
import typing
import time


type DatabaseConnection = psycopg.connection.Connection
conninfo = 'dbname=schedules'


def b(value: int, bits: int) -> str:
    return bin(value)[2:].rjust(bits, '0')


@dataclasses.dataclass
class User:
    id: int
    slots: int

    def t(self, bits: int) -> str:
        return f'{self.id:<10}: {b(self.slots, bits)} (u)'


@dataclasses.dataclass
class Event:
    id: int
    slots: int

    def t(self, bits: int) -> str:
        return f'{self.id:<10}: {b(self.slots, bits)} (e)'


def _parse_slots(slots):
    # If the database uses type `bit(336)`, do this:
    #return int(slots, 2)

    # For `bytea`, use:
    return int.from_bytes(slots)

    # For `numeric`, do:
    return int(slots)


def load_users(connection: DatabaseConnection) -> typing.Generator[User, typing.Any, None]:
    with connection.cursor() as cursor:
        cursor.execute('select id, slots from users order by id')
        for id, slots in cursor:
            yield User(id, _parse_slots(slots))


def load_users_slots(connection: DatabaseConnection) -> typing.Generator[int, typing.Any, None]:
    with connection.cursor() as cursor:
        cursor.execute('select slots from users order by id')
        for row in cursor:
            slots = row[0]
            yield _parse_slots(slots)


def load_events(connection: DatabaseConnection) -> typing.Generator[Event, typing.Any, None]:
    with connection.cursor() as cursor:
        cursor.execute('select id, slots from events order by id')
        for id, slots in cursor:
            yield Event(id, _parse_slots(slots))


def load_events_page(connection: DatabaseConnection, skip: int, take: int) -> typing.Generator[Event, typing.Any, None]:
    with connection.cursor() as cursor:
        cursor.execute('select id, slots from events order by id offset %s limit %s', (skip, take))
        for id, slots in cursor:
            yield Event(id, _parse_slots(slots))


def count_plain(connection: DatabaseConnection) -> dict[int, int]:
    users_slots = list(load_users_slots(connection))
    return {
        e.id: sum(1 for u in users_slots if u & e.slots == e.slots)
        for e
        in load_events(connection)}


def count_pandas(connection: DatabaseConnection) -> dict[int, int]:
    users_slots = numpy.fromiter(load_users_slots(connection), dtype=object)
    events = pandas.DataFrame(load_events(connection))
    events.set_index('id', inplace=True)

    def count(event_slot):
        return numpy.sum(event_slot & users_slots == event_slot)

    events['matches'] = events['slots'].apply(count)
    return events.to_dict()['matches']


def _count_map_reduce_one(skip: int, take: int) -> dict[int, int]:
    conninfo = 'dbname=schedules'
    with psycopg.connect(conninfo, connect_timeout=2) as connection:
        users_slots = numpy.fromiter(load_users_slots(connection), dtype=object)
        events_counters = {}
        for e in load_events_page(connection, skip, take):
            event_slot = e.slots
            events_counters[e.id] = numpy.sum(event_slot & users_slots == event_slot)

    return events_counters


def count_map_reduce(connection: DatabaseConnection) -> dict[int, int]:
    arguments = [(page * 1000, 1000) for page in range(5)]

    with multiprocessing.Pool() as pool:
        results = pool.starmap(_count_map_reduce_one, arguments)

    combined = {}
    for result in results:
        combined |= result

    return combined


def _count_map_reduce_shared_memory_one(shm_name: str, skip: int, take: int) -> dict[int, int]:
    start_time = time.time()
    #with cProfile.Profile() as profile:
    slot_length = 42  # 42 bytes matches 336 bits.
    shm = multiprocessing.shared_memory.SharedMemory(name=shm_name)
    buffer = bytes(shm.buf)
    shm.close()

    with psycopg.connect(conninfo, connect_timeout=2) as connection:
        events_counters = {}
        for e in load_events_page(connection, skip, take):
            event_slot = e.slots.to_bytes(slot_length)
            matches = 0
            for user_number in range(15000):
                buffer_start_index = user_number * slot_length
                position = 0
                is_match = True
                for event_byte in event_slot:
                    user_byte = buffer[buffer_start_index + position]
                    if event_byte & user_byte != event_byte:
                        is_match = False
                        break
                    position += 1

                if is_match:
                    matches += 1

            events_counters[e.id] = matches

    print(f'One: {(time.time() - start_time):.3f} seconds.')
    #pstats.Stats(profile).strip_dirs().sort_stats(pstats.SortKey.CUMULATIVE).print_stats()
    return events_counters


def count_map_reduce_shared_memory(connection: DatabaseConnection) -> dict[int, int]:
    slot_length = 42  # 42 bytes matches 336 bits.
    shm = multiprocessing.shared_memory.SharedMemory(create=True, size=slot_length * 15000)
    position = 0
    for user_slot in load_users_slots(connection):
        shm.buf[position:position + slot_length] = user_slot.to_bytes(slot_length)
        position += slot_length

    arguments = [(shm.name, page * 1000, 1000) for page in range(5)]

    with multiprocessing.Pool() as pool:
        results = pool.starmap(_count_map_reduce_shared_memory_one, arguments)

    combined = {}
    for result in results:
        combined |= result

    shm.close()
    shm.unlink()
    return combined


def _count_map_reduce_file_one(file_path: str, skip: int, take: int) -> dict[int, int]:
    t1 = time.time()
    slot_length = 42  # 42 bytes matches 336 bits.
    with open(file_path, 'rb') as f:
        buffer = f.read()

    t2 = time.time()

    with psycopg.connect(conninfo, connect_timeout=2) as connection:
        t3 = time.time()
        events_counters = {}
        for e in load_events_page(connection, skip, take):
            event_slot = e.slots.to_bytes(slot_length)
            matches = 0
            for user_number in range(15000):
                buffer_start_index = user_number * slot_length
                position = 0
                is_match = True
                for event_byte in event_slot:
                    user_byte = buffer[buffer_start_index + position]
                    if event_byte & user_byte != event_byte:
                        is_match = False
                        break
                    position += 1

                if is_match:
                    matches += 1

            events_counters[e.id] = matches

    t4 = time.time()
    print(f'File: loading: {(t2 - t1):.3f}; connecting: {(t3 - t2):.3f}; matching: {(t4 - t3):.3f}.')
    return events_counters


def count_map_reduce_file(connection: DatabaseConnection) -> dict[int, int]:
    start_time = time.time()
    slot_length = 42  # 42 bytes matches 336 bits.
    file_path = '/tmp/schedules_users'
    with open(file_path, 'wb') as f:
        for user_slot in load_users_slots(connection):
            f.write(user_slot.to_bytes(slot_length))

    arguments = [(file_path, page * 1000, 1000) for page in range(5)]
    print(f'Preparation: {(time.time() - start_time):.3f} seconds.')

    with multiprocessing.Pool() as pool:
        results = pool.starmap(_count_map_reduce_file_one, arguments)

    combined = {}
    for result in results:
        combined |= result

    return combined


def count_in_database(connection: DatabaseConnection):
    query = '''
select events.id, count(users.slots) as count
from events
    left join users on (users.slots & events.slots) = events.slots
group by events.id
'''
    with connection.cursor() as cursor:
        cursor.execute(query)
        return {row[0]: row[1] for row in cursor.fetchall()}

