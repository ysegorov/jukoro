# -*- coding: utf-8 -*-

import logging

from .base import TestEntity, Base, BaseWithPool

from jukoro import pg


__all__ = ['TestQueryBuilderDescr', 'TestQueryBuilder']


logger = logging.getLogger(__name__)


class TestQueryBuilderDescr(Base):

    def test_a(self):
        q = TestEntity.qbuilder
        self.assertIsInstance(q, pg.QueryBuilder)

        with self.assertRaises(AttributeError):
            pg.BaseEntity.qbuilder.by_id

        a = TestEntity()
        with self.assertRaises(AttributeError):
            a.qbuilder


class TestQueryBuilder(BaseWithPool):

    def test_save(self):
        last_id = self.last_id()

        a = TestEntity()
        q, params = TestEntity.qbuilder.save(a)

        with self.pool.transaction() as cursor:
            with self.assertRaises(pg.IntegrityError):
                cursor.execute(q, params)

            a.update(attr1='miracle', attr2='musician',
                     attr3='boundary', attr4=5, attr5=26)
            q, params = TestEntity.qbuilder.save(a)
            self.assertTrue(len(params) == 1)

            res = cursor.execute(q, params)
            b = TestEntity(**res.get())

            self.assertIsNot(b.entity_id, None)
            self.assertTrue(b.entity_id > last_id)
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
            self.assertEqual(b.entity_id, c.entity_id)
            self.assertEqual(b.attr1, c.attr1)

    def test_by_id(self):
        first_id, last_id = self.first_id(), self.last_id()

        with self.assertRaises(ValueError):
            TestEntity.qbuilder.by_id(None)

        with self.pool.transaction() as cursor:
            q, params = TestEntity.qbuilder.by_id(last_id)
            res = cursor.execute_and_get(q, params)
            a = TestEntity(**res)

            self.assertEqual(a.entity_id, last_id)

            b = TestEntity.by_id(cursor, first_id)
            self.assertIsInstance(b, TestEntity)
            self.assertEqual(b.entity_id, first_id)
