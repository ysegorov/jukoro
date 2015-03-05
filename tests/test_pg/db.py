# -*- coding: utf-8 -*-

import unittest

import psycopg2.extensions

from jukoro import pg

from .base import Base, BaseWithPool


__all__ = ['TestPgPool', 'TestPgConnection', 'TestAutoCommit',
           'TestManualCommit', 'TestRollback', 'TestNamedCursor', 'TestFetch']


class TestPgPool(BaseWithPool):

    def test_uri(self):
        self.assertEqual(self.pool.uri, self.uri())

    def test_warm_up(self):
        # FIXME do not test private members
        self.assertEqual(len(self.pool), 0)

        with self.pool.transaction() as cursor:

            self.assertTrue(self.pool._pool.is_locked(cursor._pg_conn))

        self.assertIsNone(cursor._pg_conn)

        self.assertEqual(len(self.pool), self.pool_size)

    def test_pool_closed(self):
        sz = 3
        pool = pg.PgDbPool(self.uri(), pool_size=sz)

        # warm up
        with pool.transaction():
            pass

        self.assertEqual(len(pool), sz)

        pool.close()

        self.assertEqual(len(pool), 0)

        with self.assertRaises(pg.PgPoolClosedError):
            pool.transaction()


class TestPgConnection(Base):

    def test_connection_params(self):
        uri, schema = self.uri(), self.schema()

        with pg.PgConnection(uri, autoclose=True).transaction() as cursor:

            res = cursor.execute_and_get('SHOW search_path;')
            self.assertIsInstance(res, dict)
            self.assertTrue('search_path' in res)
            self.assertTrue(schema in res['search_path'])

            res = cursor.execute_and_get('SHOW TIME ZONE;')
            self.assertEqual(res['TimeZone'], 'UTC')

    def test_connection_close(self):
        uri = self.uri()

        conn = pg.PgConnection(uri, autoclose=True)
        with conn.transaction():
            pass
        self.assertTrue(conn.is_closed)

        conn = pg.PgConnection(uri, autoclose=False)
        with conn.transaction():
            pass
        self.assertFalse(conn.is_closed)
        conn.close()
        self.assertTrue(conn.is_closed)

    def test_connection_schema(self):
        uri, schema = self.uri(), self.schema()

        conn = pg.PgConnection(uri)
        self.assertEqual(schema, conn.schema)

    def test_connection_is_closed(self):
        uri, schema = self.uri(), self.schema()

        conn = pg.PgConnection(uri, autoclose=True)
        with conn.transaction():
            pass

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.commit()

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.rollback()

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.close()

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.transaction()

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.cursor()

        with self.assertRaises(pg.PgConnectionClosedError):
            conn.reattach()

    def test_connection_psycopg2_cursor(self):
        uri, schema = self.uri(), self.schema()

        conn = pg.PgConnection(uri)

        cursor = conn.cursor(named=False)
        self.assertIsInstance(cursor, psycopg2.extensions.cursor)
        self.assertIs(cursor.name, None)
        cursor.close()

        cursor = conn.cursor(named=True)
        self.assertIsInstance(cursor, psycopg2.extensions.cursor)
        self.assertIsNot(cursor.name, None)
        self.assertTrue(cursor.scrollable)
        self.assertTrue(cursor.withhold)
        cursor.execute('SELECT 1;')  # needed to close named cursor
        cursor.close()

        conn.close()

    def test_connection_transaction(self):
        uri, schema = self.uri(), self.schema()

        conn = pg.PgConnection(uri)

        cursor = conn.transaction()
        self.assertIsInstance(cursor, pg.PgTransaction)
        cursor.close()
        conn.close()


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
