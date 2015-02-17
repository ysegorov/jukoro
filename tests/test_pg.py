# -*- coding: utf-8 -*-

import datetime
import logging
import os
import unittest
import warnings

import psycopg2

from jukoro import pg

logger = logging.getLogger(__name__)


URI = os.environ.get('PG_URI', 'postgresql://localhost/jukoro_test')
TEST_URI = 'postgresql://test:test@localhost:5467/jukoro_test'
BAD_URI = 'postgresq://localhost:5432/jukoro_test.a1'

IS_ONLINE = False

SQL_SETUP = """
CREATE SCHEMA IF NOT EXISTS {schema};

-- global id sequence
DO $$
BEGIN
    CREATE SEQUENCE {schema}.global_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
EXCEPTION WHEN duplicate_table THEN
    -- do nothing, it's already there
END $$;


-- basic entity table (for inheritance)
CREATE TABLE IF NOT EXISTS {schema}."entity" (
    "id" serial PRIMARY KEY,
    "entity_id" bigint NOT NULL DEFAULT nextval('{schema}.global_id_seq'),
    "entity_start" timestamp with time zone DEFAULT current_timestamp,
    "entity_end" timestamp with time zone
            DEFAULT '2999-12-31 23:59:59.999+0'::timestamp with time zone,
    "data" jsonb NOT NULL
);
"""
SQL_TEARDOWN = """
DROP SCHEMA {schema} CASCADE;
"""


def setUp():
    global IS_ONLINE
    global URI
    kwargs = pg.pg_uri_to_kwargs(URI)
    try:
        conn = psycopg2.connect(
            host=kwargs['host'], port=kwargs['port'],
            user=kwargs['user'], dbname=kwargs['dbname'])
    except psycopg2.OperationalError:
        warnings.warn(
            'Test db does not exist or unaccessible - have to skip pg tests',
            RuntimeWarning)
    else:
        IS_ONLINE = True
        if kwargs['schema'] == 'public':

            kwargs['schema'] = (
                'ju_%s' % datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            URI += '.%s' % kwargs['schema']
        # create test table
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(SQL_SETUP.format(schema=kwargs['schema']))
        cursor.close()
        conn.close()


def tearDown():
    if not IS_ONLINE:
        return
    kwargs = pg.pg_uri_to_kwargs(URI)
    schema = kwargs['schema']
    try:
        conn = psycopg2.connect(
            host=kwargs['host'], port=kwargs['port'],
            user=kwargs['user'], dbname=kwargs['dbname'])
    except psycopg2.OperationalError:
        warnings.warn(
            'Oops, unable to clean test schema "%s", do it manually' % schema,
            RuntimeWarning)
    else:
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(SQL_TEARDOWN.format(schema=schema))
        cursor.close()
        conn.close()


class TestUri(unittest.TestCase):

    def setUp(self):
        if not IS_ONLINE:
            self.skipTest('no db available')

    def test_uri_to_kwargs_full(self):
        kwargs = pg.pg_uri_to_kwargs(TEST_URI)
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5467)
        self.assertEqual(kwargs['user'], 'test')
        self.assertEqual(kwargs['password'], 'test')
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_partial(self):
        kwargs = pg.pg_uri_to_kwargs(URI)
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5432)
        self.assertIsNot(kwargs['user'], None)
        self.assertIs(kwargs['password'], None)
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertNotEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_bad(self):
        self.assertRaises(pg.PgUriError, lambda: pg.pg_uri_to_kwargs(BAD_URI))


class Base(unittest.TestCase):
    pool = None
    pool_size = 3
    uri_kwargs = None

    @classmethod
    def setUpClass(cls):
        cls.pool = pg.PgDbPool(URI, cls.pool_size)
        cls.uri_kwargs = pg.pg_uri_to_kwargs(URI)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()
        cls.pool = cls.uri_kwargs = None


class TestWarmUp(Base):

    def test_uri(self):
        self.assertEqual(self.pool.uri, URI)

    def test_warm_up(self):
        self.assertEqual(len(self.pool), 0)

        with self.pool.transaction() as cursor:

            self.assertTrue(self.pool._pool.is_locked(cursor._pg_conn))

        self.assertIsNone(cursor._pg_conn)

        self.assertEqual(len(self.pool), self.pool_size)


class TestConnectionParams(Base):

    def test_connection_params(self):
        with self.pool.transaction() as cursor:

            res = cursor.execute_and_get('SHOW search_path;')
            self.assertIsInstance(res, dict)
            self.assertTrue('search_path' in res)
            self.assertTrue(self.uri_kwargs['schema'] in res['search_path'])

            res = cursor.execute_and_get('SHOW TIME ZONE;')
            self.assertEqual(res['TimeZone'], 'UTC')


@unittest.skip('TODO')
class TestIntrospect(Base):

    def test(self):
        pass


@unittest.skip('TODO')
class TestAutoCommit(Base):

    def test_a(self):
        pass

    def test_b(self):
        pass


@unittest.skip('TODO')
class TestManualCommit(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestRollback(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestNamedCursor(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestFetch(Base):

    def test_fetch_one(self):
        pass

    def test_fetch_all(self):
        pass

    def test_fetch_many(self):
        pass

    def test_scroll(self):
        pass
