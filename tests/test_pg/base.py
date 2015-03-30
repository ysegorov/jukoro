# -*- coding: utf-8 -*-

import random
import unittest

from jukoro import arrow
from jukoro import pg


class TestEntity(pg.BaseEntity):
    db_table = 'test_pg'

    attr1 = pg.Attr(title='Attr 1',
                    db_index=True, db_not_null=True, minlen=4)
    attr2 = pg.Attr(title='Attr 2',
                    db_index=True, db_not_null=True, minlen=6)
    attr3 = pg.Attr(title='Attr 3',
                    db_not_null=True)
    attr4 = pg.Attr(title='Attr 4',
                    db_index=True, value_type=int, db_not_null=True)
    attr5 = pg.Attr(title='Attr 5',
                    value_type=int, db_not_null=True)
    attr6 = pg.Attr(title='Attr 6',
                    value_type=int, db_not_null=False)
    attr7 = pg.Attr(title='Attr 7',
                    db_index=True, value_type=arrow.JuArrow, db_not_null=True)


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
        self._entity_id = None

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
    def entity_id(self):
        if self._entity_id is None:
            first_id, last_id = self.first_id(), self.last_id()
            # last 100 of entities to test deletes
            self._entity_id = random.randint(first_id + 1, last_id - 101)
        return self._entity_id

    def _get(self, cursor, entity_id):
        q = 'SELECT "doc" FROM "test_pg__live" WHERE "entity_id" = %s;'
        res = cursor.execute_and_get(q, (entity_id, ))
        return res['doc'], list(cursor.queries)

    def _cnt(self, cursor, entity_id, raw=False):
        nm = 'test_pg'
        if not raw:
            nm += '__live'
        q = 'SELECT COUNT("id") as cnt FROM "{}" ' \
            'WHERE "entity_id" = %s;'.format(nm)
        res = cursor.execute_and_get(q, (entity_id, ))
        return res['cnt']

    def _count(self, cursor, entity_id):
        return self._cnt(cursor, entity_id)

    def _count_raw(self, cursor, entity_id):
        return self._cnt(cursor, entity_id, raw=True)

    def _create(self, cursor, doc):
        res = cursor.execute_and_get(
            'INSERT INTO "test_pg__live" ("entity_id", "doc") '
            'VALUES (DEFAULT, %s) RETURNING "entity_id";', (doc, ))
        return res['entity_id'], list(cursor.queries)

    def _update(self, cursor, entity_id, doc):
        cursor.execute(
            'UPDATE "test_pg__live" SET "doc" = %s '
            'WHERE "entity_id" = %s;', (doc, entity_id))
        return list(cursor.queries)

    def _delete(self, cursor, entity_id):
        cursor.execute('DELETE FROM "test_pg__live" WHERE "entity_id" = %s;',
                       (entity_id, ))
        return list(cursor.queries)

    def assertDocEqual(self, doc1, doc2):
        self.assertIsInstance(doc1, dict)
        self.assertIsInstance(doc2, dict)

        to_skip = ('_created', '_updated', '_deleted')
        for k in doc1:
            if k in to_skip:
                continue
            self.assertEqual(doc1[k], doc2.get(k))
