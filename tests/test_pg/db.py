# -*- coding: utf-8 -*-

import logging
import time
import unittest

import psycopg2.extensions

from jukoro import pg

from .base import Base, BaseWithPool


__all__ = ['TestPgPool', 'TestPgConnection', 'TestHistory', 'TestAutoCommit',
           'TestManualCommit', 'TestRollback', 'TestNamedCursor', 'TestFetch']


logger = logging.getLogger(__name__)


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


class TestHistory(BaseWithPool):

    def test_history_count(self):

        eid = self.eid
        cnt = 3

        with self.pool.transaction() as cursor:
            doc = self._get(cursor, eid)[0]
            for __ in xrange(cnt):
                time.sleep(0.125)
                doc['attr6'] = int(time.time())
                self._save(cursor, eid, doc)

        with self.pool.transaction() as cursor:
            count = self._count(cursor, eid)
            raw_count = self._count_raw(cursor, eid)
            self.assertEqual(count, 1)
            self.assertEqual(raw_count, cnt + 1)


class TestAutoCommit(BaseWithPool):

    def test_a(self):

        eid = self.eid

        with self.pool.transaction() as cur1, self.pool.transaction() as cur2:
            doc1, queries1 = self._get(cur1, eid)

            self.assertIsInstance(doc1, dict)
            self.assertEqual(len(queries1), 1)

            doc1['attr6'] = int(time.time())
            queries1 = self._save(cur1, eid, doc1)
            self.assertEqual(len(queries1), 2)

            doc2 = self._get(cur2, eid)[0]

            self.assertEqual(doc1.get('attr6'), doc2.get('attr6'))
            self.assertEqual(doc1, doc2)

    def test_b(self):

        eid = self.eid

        with self.pool.transaction(autocommit=False) as cur1:
            with self.pool.transaction() as cur2:

                doc1, queries1 = self._get(cur1, eid)

                doc1['attr6'] = int(time.time())
                queries1 = self._save(cur1, eid, doc1)
                self.assertEqual(len(queries1), 2)

                doc2 = self._get(cur2, eid)[0]

                self.assertNotEqual(doc1.get('attr6'), doc2.get('attr6'))
                self.assertNotEqual(doc1, doc2)

        with self.pool.transaction() as cur3:

            doc3 = self._get(cur3, eid)[0]
            self.assertEqual(doc1.get('attr6'), doc3.get('attr6'))
            self.assertEqual(doc1, doc3)


class TestManualCommit(BaseWithPool):

    def test_a(self):

        uri, schema = self.uri(), self.schema()
        eid = self.eid

        conn = pg.PgConnection(uri)

        cur1 = conn.transaction(autocommit=False)
        doc1 = self._get(cur1, eid)[0]

        doc1['attr6'] = int(time.time())
        self._save(cur1, eid, doc1)

        with self.pool.transaction() as cur2:
            doc2 = self._get(cur2, eid)[0]

        self.assertNotEqual(doc1, doc2)

        conn.commit()

        with self.pool.transaction() as cur3:
            doc3 = self._get(cur3, eid)[0]

        self.assertEqual(doc1, doc3)

        cur1.close()
        conn.close()


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
