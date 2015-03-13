# -*- coding: utf-8 -*-

import logging

from .base import TestEntity, Base, BaseWithPool

from jukoro import pg


__all__ = ['TestQueryDescr', 'TestQueryEntity', 'TestQueryEntityClass']


logger = logging.getLogger(__name__)


class TestQueryDescr(Base):

    def test_a(self):
        q = TestEntity.sql
        self.assertIsInstance(q, pg.QueryEntityClass)

        with self.assertRaises(AttributeError):
            pg.BaseEntity.sql

        q = TestEntity().sql
        self.assertIsInstance(q, pg.QueryEntity)


class TestQueryEntity(BaseWithPool):

    def test_save(self):
        last_id = self.last_id()

        a = TestEntity()
        q, params = a.sql.save()

        with self.pool.transaction() as cursor:
            with self.assertRaises(pg.IntegrityError):
                cursor.execute(q, params)

            a.update(attr1='miracle', attr2='musician',
                     attr3='boundary', attr4=5, attr5=26)
            q, params = a.sql.save()
            self.assertTrue(len(params) == 1)

            res = cursor.execute(q, params)
            b = TestEntity(**res.get())

            self.assertIsNot(b.id, None)
            self.assertTrue(b.id > last_id)
            self.assertEqual(a.doc, b.doc)
            self.assertEqual(b.attr1, 'miracle')
            self.assertEqual(b.attr2, 'musician')
            self.assertEqual(b.attr3, 'boundary')
            self.assertEqual(b.attr4, 5)
            self.assertEqual(b.attr5, 26)

            b.update(attr4=45)
            c = b.save(cursor)
            self.assertIsNot(b, c)
            self.assertEqual(c.attr4, 45)
            self.assertEqual(b.id, c.id)
            self.assertEqual(b.attr1, c.attr1)


class TestQueryEntityClass(BaseWithPool):

    def test_by_id(self):
        first_id, last_id = self.first_id(), self.last_id()

        with self.assertRaises(ValueError):
            TestEntity.sql.by_id(None)

        with self.pool.transaction() as cursor:
            q, params = TestEntity.sql.by_id(last_id)
            res = cursor.execute_and_get(q, params)
            a = TestEntity(**res)

            self.assertEqual(a.id, last_id)

            b = TestEntity.by_id(first_id, cursor)
            self.assertIsInstance(b, TestEntity)
            self.assertEqual(b.id, first_id)
