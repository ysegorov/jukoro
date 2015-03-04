# -*- coding: utf-8 -*-

import unittest

from .base import Base, BaseWithPool


__all__ = ['TestPgPoolWarmUp', 'TestPgConnectionParams', 'TestAutoCommit',
           'TestManualCommit', 'TestRollback', 'TestNamedCursor', 'TestFetch']


class TestPgPoolWarmUp(BaseWithPool):

    def test_uri(self):
        self.assertEqual(self.pool.uri, self.uri())

    def test_warm_up(self):
        # FIXME do not test private members
        self.assertEqual(len(self.pool), 0)

        with self.pool.transaction() as cursor:

            self.assertTrue(self.pool._pool.is_locked(cursor._pg_conn))

        self.assertIsNone(cursor._pg_conn)

        self.assertEqual(len(self.pool), self.pool_size)


class TestPgConnectionParams(BaseWithPool):

    def test_connection_params(self):
        with self.pool.transaction() as cursor:

            res = cursor.execute_and_get('SHOW search_path;')
            self.assertIsInstance(res, dict)
            self.assertTrue('search_path' in res)
            self.assertTrue(self.uri_kwargs['schema'] in res['search_path'])

            res = cursor.execute_and_get('SHOW TIME ZONE;')
            self.assertEqual(res['TimeZone'], 'UTC')


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
