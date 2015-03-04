# -*- coding: utf-8 -*-

from .base import Base

from jukoro import pg


__all__ = ['TestPgUriToKwargs']


class TestPgUriToKwargs(Base):

    def test_uri_to_kwargs_full(self):
        kwargs = pg.pg_uri_to_kwargs(self.tst_uri())
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5467)
        self.assertEqual(kwargs['user'], 'test')
        self.assertEqual(kwargs['password'], 'test')
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_partial(self):
        kwargs = pg.pg_uri_to_kwargs(self.uri())
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5432)
        self.assertIsNot(kwargs['user'], None)
        self.assertIs(kwargs['password'], None)
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertNotEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_bad(self):
        self.assertRaises(pg.PgUriError,
                          lambda: pg.pg_uri_to_kwargs(self.bad_uri()))
