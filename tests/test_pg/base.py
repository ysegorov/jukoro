# -*- coding: utf-8 -*-

import random
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

    @staticmethod
    def last_id():
        from . import LAST_ID
        return LAST_ID

    @staticmethod
    def first_id():
        from . import FIRST_ID
        return FIRST_ID

    def setUp(self):
        if self.online_required and not self.is_online():
            self.skipTest('no db available')


class BaseWithPool(Base):
    pool = None
    pool_size = 3
    uri_kwargs = None

    def __init__(self, *args, **kwargs):
        super(BaseWithPool, self).__init__(*args, **kwargs)
        self._eid = None

    @classmethod
    def setUpClass(cls):
        uri = cls.uri()
        cls.pool = pg.PgDbPool(uri, cls.pool_size)
        cls.uri_kwargs = pg.pg_uri_to_kwargs(uri)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()
        cls.pool = cls.uri_kwargs = None

    @property
    def eid(self):
        if self._eid is None:
            first_id, last_id = self.first_id(), self.last_id()
            self._eid = random.randint(first_id, last_id)
        return self._eid

    def _get(self, cursor, eid):
        q = 'SELECT "doc" FROM "test_pg__live" WHERE "entity_id" = %s;'
        res = cursor.execute_and_get(q, (eid, ))
        return res['doc'], list(cursor.queries)

    def _cnt(self, cursor, eid, raw=False):
        nm = 'test_pg'
        if not raw:
            nm += '__live'
        q = 'SELECT COUNT("id") as cnt FROM "{}" ' \
            'WHERE "entity_id" = %s;'.format(nm)
        res = cursor.execute_and_get(q, (eid, ))
        return res['cnt']

    def _count(self, cursor, eid):
        return self._cnt(cursor, eid)

    def _count_raw(self, cursor, eid):
        return self._cnt(cursor, eid, raw=True)

    def _save(self, cursor, eid, doc):
        cursor.execute(
            'INSERT INTO "test_pg__live" ("entity_id", "doc") '
            'VALUES (%s, %s);', (eid, doc))
        return list(cursor.queries)
