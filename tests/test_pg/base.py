# -*- coding: utf-8 -*-

import unittest

from jukoro import pg


class TestEntity(pg.BaseEntity):
    db_table = 'test_pg'

    attr1 = pg.Attr(title='Attr 1',
                    db_index=True, db_type='text', db_not_null=True, minlen=4)
    attr2 = pg.Attr(title='Attr 2',
                    db_index=True, db_type='text', db_not_null=True, minlen=6)
    attr3 = pg.Attr(title='Attr 3',
                    db_type='text', db_not_null=True)
    attr4 = pg.Attr(title='Attr 4',
                    db_index=True, db_type='int', db_not_null=True)
    attr5 = pg.Attr(title='Attr 5',
                    db_type='int', db_not_null=True)
    attr6 = pg.Attr(title='Attr 6',
                    db_type='int', db_not_null=False)


class Base(unittest.TestCase):
    online_required = True

    @staticmethod
    def uri():
        from . import URI
        return URI

    @staticmethod
    def bad_uri():
        from . import BAD_URI
        return BAD_URI

    @staticmethod
    def tst_uri():
        from . import TEST_URI
        return TEST_URI

    @staticmethod
    def is_online():
        from . import IS_ONLINE
        return IS_ONLINE

    @staticmethod
    def schema():
        from . import SCHEMA
        return SCHEMA

    def setUp(self):
        if self.online_required and not self.is_online():
            self.skipTest('no db available')


class BaseWithPool(Base):
    pool = None
    pool_size = 3
    uri_kwargs = None

    @classmethod
    def setUpClass(cls):
        uri = cls.uri()
        cls.pool = pg.PgDbPool(uri, cls.pool_size)
        cls.uri_kwargs = pg.pg_uri_to_kwargs(uri)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()
        cls.pool = cls.uri_kwargs = None
