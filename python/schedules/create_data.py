#!/usr/bin/env python

#import concurrent.futures
#import functools
import random
import psycopg
import typing


type DatabaseConnection = psycopg.connection.Connection


def reset_database(connection: DatabaseConnection):
    with open('reset_tables.sql', 'r') as f:
        query = f.read()

    with connection.cursor() as cursor:
        cursor.execute(query)

    connection.commit()


def _slots_to_db_type(slots: int) -> typing.Any:
    # If the database uses type `bit(336)`, do this:
    # return f'{slots:b}'.rjust(336, '0')

    # For `bytea`, use:
    return slots.to_bytes(42)

    # For `numeric`, do:
    #return slots


def create_users(connection: DatabaseConnection, count_slots: int, total_users: int) -> None:
    maximum = 1 << count_slots - 1
    with connection.cursor() as cursor:
        for _ in range(total_users):
            slots = random.randint(0, maximum)
            cursor.execute('insert into users (slots) values (%s)', (_slots_to_db_type(slots),))

    connection.commit()


def create_u(connection: DatabaseConnection, slots: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute('insert into users (slots) values (%s)', (_slots_to_db_type(slots),))

    connection.commit()


def create_events(connection: DatabaseConnection, count_slots: int, total_events: int) -> None:
    with connection.cursor() as cursor:
        for _ in range(total_events):
            duration = random.randint(1, 6)  # 30 minutes to 3 hours.
            pattern = {
                1: 0b1,
                2: 0b11,
                3: 0b111,
                4: 0b1111,
                5: 0b11111,
                6: 0b111111,
            }[duration]
            start_time = random.randint(0, count_slots - duration)
            slots = pattern << start_time
            cursor.execute('insert into events (slots) values (%s)', (_slots_to_db_type(slots),))

    connection.commit()


def create_e(connection: DatabaseConnection, slots: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute('insert into events (slots) values (%s)', (_slots_to_db_type(slots),))

    connection.commit()
