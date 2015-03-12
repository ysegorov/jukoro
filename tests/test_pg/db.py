# -*- coding: utf-8 -*-

from __future__ import division

import logging
import random
import time
import unittest

import psycopg2.extensions

from jukoro import pg

from .base import Base, BaseWithPool


__all__ = ['TestPgPool', 'TestPgConnection', 'TestHistory', 'TestAutoCommit',
           'TestManualCommit', 'TestRollback', 'TestNamedCursor', 'TestFetch',
           'TestCallProc']


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
            conn.autocommit = True

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


class TestRollback(BaseWithPool):

    def test_a(self):

        eid = self.eid

        with self.pool.transaction(autocommit=False) as cursor:
            doc = self._get(cursor, eid)[0]
            doc['attr5'] = -1

            with self.assertRaises(pg.IntegrityError):
                self._save(cursor, eid, doc)

    def test_b(self):

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

        conn.rollback()

        with self.pool.transaction() as cur3:
            doc3 = self._get(cur3, eid)[0]

        self.assertNotEqual(doc1, doc3)
        self.assertEqual(doc2, doc3)

        cur1.close()
        conn.close()


class TestNamedCursor(BaseWithPool):

    def test_a(self):
        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        with self.pool.transaction(autocommit=False, named=True) as cursor:
            cursor.execute(q)
            queries = ''.join(cursor.queries)
            self.assertTrue('DECLARE' in queries)

    def test_b(self):

        # test proper cursor close
        with self.pool.transaction(autocommit=False, named=True):
            pass

    def test_c(self):
        uri, schema = self.uri(), self.schema()

        conn = pg.PgConnection(uri)

        cur1 = conn.transaction(autocommit=False, named=True)

        try:
            cur1.execute('SELECT 1/0;')
        except pg.DataError:
            conn.rollback()

        # query not in transaction queries as it failed
        self.assertFalse('DECLARE' in ''.join(cur1.queries))

        cur1.close()
        conn.close()

    def test_d(self):
        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        with self.pool.transaction(autocommit=False, named=True) as cursor:
            cursor.execute(q)
            self.assertTrue('DECLARE' in ''.join(cursor.queries))

            # second execute on named cursor raises ProgrammingError
            with self.assertRaises(pg.ProgrammingError):
                cursor.execute('SELECT 1/0;')

    def test_e(self):
        # test for named cursor fetched data slicing
        # check postgresql logs for actual queries
        # TODO real test
        random.seed()

        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        cnt = 0
        with self.pool.transaction(autocommit=False,
                                   named=True, block_size=100) as cursor:
            res = cursor.execute(q)

            self.assertEqual(res.rowcount, -1)

            for row in res:
                cnt += 1

        half = int(cnt / 2)
        p1, p2 = random.randint(10, half), random.randint(half, cnt)
        p3, p4 = random.randint(10, half), random.randint(half, cnt)
        if p3 > p1:
            (p1, p2), (p3, p4) = (p3, p4), (p1, p2)
        elif p3 == p1:
            p3 -= 5

        self.assertTrue(p1 > p3)

        with self.pool.transaction(autocommit=False,
                                   named=True, block_size=100) as cursor:
            res = cursor.execute(q)
            res[p1:p2]
            res[p3:p4]

            with self.assertRaises(ValueError):
                res[cnt + 2:]

            res[cnt + 1000:cnt + 1200]
            res[p1:p2]


class TestFetch(BaseWithPool):

    def test_fetch_one(self):
        eid = self.eid

        q = 'SELECT "entity_id", "doc" from "test_pg__live" ' \
            'WHERE "entity_id" = %s;'

        with self.pool.transaction() as cursor:
            res = cursor.execute(q, (eid, ))

            self.assertIsInstance(res, pg.PgResult)
            self.assertTrue(len(res) == 1)

            r1 = res.get()
            r2 = res.get()

        self.assertEqual(r1, r2)
        self.assertEqual(r1['entity_id'], eid)
        self.assertTrue(res.is_closed)

        with self.assertRaises(pg.PgCursorClosedError):
            res.get()

        with self.pool.transaction() as cursor:
            res = cursor.execute(q, (-1, ))

            self.assertTrue(len(res) == 0)

            with self.assertRaises(pg.PgDoesNotExistError):
                res.get()

    def test_fetch_all(self):
        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        with self.pool.transaction() as cursor:
            res = cursor.execute(q)

            self.assertIsInstance(res, pg.PgResult)

            rows1 = res.all()
            rows2 = res.all()
            self.assertEqual(len(res), len(rows1))
            self.assertEqual(len(rows2), 0)

        with self.pool.transaction() as cursor:
            res = cursor.execute(q)

        with self.assertRaises(pg.PgCursorClosedError):
            res.all()

    def test_fetch_many(self):
        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        bs = 75
        with self.pool.transaction(block_size=bs) as cursor:
            res = cursor.execute(q)

            cnt, data = 0, []
            block = res.block()
            while block:
                cnt += len(block)
                data.extend(block)
                self.assertTrue(len(block) <= bs)
                block = res.block()

            rows = res.all()
            self.assertTrue(len(rows) == 0)

            half = int(cnt / 2)
            res.scroll(0)
            rows = set(x['entity_id'] for x in res[:half])
            data = set(x['entity_id'] for x in data)
            self.assertTrue(len(rows) == half)
            self.assertTrue(set(data).issuperset(set(rows)))

    def test_scroll(self):

        q = 'SELECT "entity_id", "doc" from "test_pg__live";'

        cnt, bs = 0, 75
        with self.pool.transaction(block_size=bs) as cursor:
            res = cursor.execute(q)

            for __ in res:
                cnt += 1

            res.scroll(0)
            res.scroll(int(cnt / 2))
            with self.assertRaises(pg.PgDoesNotExistError):
                res.scroll(-1)
            with self.assertRaises(pg.PgDoesNotExistError):
                res.scroll(cnt)


@unittest.skip('TODO')
class TestCallProc(Base):

    def test_a(self):
        pass

    def test_b(self):
        pass

    def test_c(self):
        pass
