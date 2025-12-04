#!/usr/bin/env python

import psycopg
import schedules
import sys


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <type>")
        sys.exit(1)

    run_type = sys.argv[1]
    conninfo = 'dbname=schedules'

    alternative = {
        'plain': schedules.count_plain,
        'pandas': schedules.count_pandas,
        'mapreduce': schedules.count_map_reduce,
        'mapreduce-shared': schedules.count_map_reduce_shared_memory,
        'mapreduce-file': schedules.count_map_reduce_file,
        'indb': schedules.count_in_database,
    }[run_type]

    with psycopg.connect(conninfo, connect_timeout=2) as connection:
        for event_id, count in alternative(connection).items():
            print(f'.{event_id}:{count}')
