#!/usr/bin/env python

import psycopg
import schedules


if __name__ == '__main__':
    count_slots = 7 * 24 * 2  # 336 bits. If going higher, replicate the change in SQL scripts and source code.
    assert count_slots % 8 == 0
    total_users = 15000
    total_events = 5000

    #count_slots = 24 * 2
    #total_users = 15
    #total_events = 5

    conninfo = 'dbname=schedules'

    with psycopg.connect(conninfo, connect_timeout=2) as connection:
        schedules.reset_database(connection)
        schedules.create_users(connection, count_slots, total_users)
        schedules.create_events(connection, count_slots, total_events)
