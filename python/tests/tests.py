import schedules
import unittest
import psycopg


conninfo = 'dbname=schedules'


class Tests(unittest.TestCase):
    def setUp(self):
        with psycopg.connect(conninfo, connect_timeout=2) as connection:
            schedules.reset_database(connection)

    def test_large_slots(self):
        large_value = 1 << 335
        with psycopg.connect(conninfo, connect_timeout=2) as connection:
            schedules.create_u(connection, large_value)
            actual = list(schedules.load_users(connection))
            self.assertEqual(large_value, actual[0].slots)

    def test_count_plain_when_event_matches_user(self):
        with psycopg.connect(conninfo, connect_timeout=2) as connection:
            schedules.create_u(connection, 0b00101101)
            schedules.create_e(connection, 0b00001100)
            actual = schedules.count_plain(connection)

        expected = {1: 1}
        self.assertEqual(expected, actual)

    def test_count_plain_when_event_does_not_match_user(self):
        with psycopg.connect(conninfo, connect_timeout=2) as connection:
            schedules.create_u(connection, 0b00101101)
            schedules.create_e(connection, 0b00011000)
            actual = schedules.count_plain(connection)

        expected = {1: 0}
        self.assertEqual(expected, actual)
