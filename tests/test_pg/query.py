# -*- coding: utf-8 -*-

from collections import defaultdict
import logging
import random

from .base import TestEntity, Base, BaseWithPool

from jukoro import arrow
from jukoro import pg


__all__ = ['TestQueryBuilderDescr', 'TestQueryViewBuilder']


logger = logging.getLogger(__name__)


class TestQueryBuilderDescr(Base):

    def test_a(self):
        q = TestEntity.qbuilder
        self.assertIsInstance(q, pg.QueryViewBuilder)

        with self.assertRaises(AttributeError):
            pg.BaseEntity.qbuilder.by_id

        a = TestEntity()
        with self.assertRaises(AttributeError):
            a.qbuilder


class TestQueryViewBuilder(BaseWithPool):

    def test_by_id(self):
        first_id, last_id = self.first_id(), self.last_id()
        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)

        q, params = qb.by_id(12)
        self.assertTrue(len(params) == 1)
        self.assertEqual(params, (12, ))
        self.assertTrue(vn in q)
        self.assertTrue('WHERE "entity_id" = %s' in q)

        ids = tuple(range(5, 10))
        q, params = qb.by_id(*ids)
        self.assertTrue(len(params) == 1)
        self.assertEqual(params, (ids, ))
        self.assertTrue('WHERE "entity_id" IN %s' in q)

        with self.assertRaises(ValueError):
            TestEntity.qbuilder.by_id(None)

        with self.assertRaises(ValueError):
            TestEntity.qbuilder.by_id()

        with self.pool.transaction() as cursor:
            q, params = TestEntity.qbuilder.by_id(last_id)
            res = cursor.execute_and_get(q, params)
            a = TestEntity(**res)

            self.assertEqual(a.entity_id, last_id)

            b = TestEntity.by_id(cursor, first_id)
            self.assertIsInstance(b, TestEntity)
            self.assertEqual(b.entity_id, first_id)

        with self.pool.transaction() as cursor:
            q, params = TestEntity.qbuilder.by_id(first_id, last_id)
            res = cursor.execute(q, params)
            res = res.all()
            self.assertTrue(len(res) == 2)
            a, b = TestEntity(**res[0]), TestEntity(**res[1])
            self.assertEqual(a.entity_id, first_id)
            self.assertEqual(b.entity_id, last_id)

    def test_create(self):
        a, b, c = TestEntity(), TestEntity(), TestEntity()

        for idx, entity in enumerate((a, b, c)):
            entity.update(attr1='miracle-%s' % idx,
                          attr2='musician-%s' % idx,
                          attr3='boundary-%s' % idx,
                          attr4=idx,
                          attr5=idx * 100,
                          attr7=arrow.utcnow())

        with self.pool.transaction() as cursor:
            q, params = TestEntity.qbuilder.create(a, b, c)

            res = cursor.execute(q, params)
            res = res.all()
            res = sorted(res, key=lambda x: x['doc']['attr4'])
            aa, bb, cc = res
            aa, bb, cc = TestEntity(**aa), TestEntity(**bb), TestEntity(**cc)

            aa.attr7 = arrow.from_db_val(aa.attr7)
            bb.attr7 = arrow.from_db_val(bb.attr7)
            cc.attr7 = arrow.from_db_val(cc.attr7)

            self.assertEqual(a.attr1, aa.attr1)
            self.assertEqual(a.attr4, aa.attr4)
            self.assertEqual(a.doc, aa.doc)
            self.assertEqual(b.attr2, bb.attr2)
            self.assertEqual(b.attr5, bb.attr5)
            self.assertEqual(b.doc, bb.doc)
            self.assertEqual(c.attr3, cc.attr3)
            self.assertEqual(c.attr5, cc.attr5)
            self.assertEqual(c.doc, cc.doc)

    def test_update(self):
        last_id = self.last_id()

        a = TestEntity()
        q, params = TestEntity.qbuilder.create(a)

        with self.pool.transaction() as cursor:
            with self.assertRaises(pg.IntegrityError):
                cursor.execute(q, params)

            a.update(attr1='miracle', attr2='musician',
                     attr3='boundary', attr4=5, attr5=26,
                     attr7=arrow.utcnow())
            q, params = TestEntity.qbuilder.create(a)
            self.assertTrue(len(params) == 1)

            res = cursor.execute(q, params)
            b = TestEntity(**res.get())

            b.attr7 = arrow.from_db_val(b.attr7)

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

    def test_delete(self):
        random.seed()

        first_id, last_id = self.first_id(), self.last_id()
        eid1 = random.randint(first_id + 1, last_id - 1)
        eid2 = random.randint(first_id + 1, last_id - 1)
        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)

        with self.assertRaises(ValueError):
            TestEntity.qbuilder.delete(None)

        with self.assertRaises(ValueError):
            TestEntity.qbuilder.delete()

        q, params = qb.delete(12)
        self.assertTrue(len(params) == 1)
        self.assertEqual(params, (12, ))
        self.assertTrue(vn in q)
        self.assertTrue('DELETE FROM' in q)
        self.assertTrue('WHERE "entity_id" = %s' in q)

        ids = (eid1, eid2)
        q, params = qb.delete(*ids)
        self.assertTrue(len(params) == 1)
        self.assertEqual(params, (ids, ))
        self.assertTrue('WHERE "entity_id" IN %s' in q)

        with self.pool.transaction() as cursor:
            cursor.execute(q, params)

        with self.pool.transaction() as cursor:
            q, params = TestEntity.qbuilder.by_id(*ids)
            res = cursor.execute(q, params)
            res = res.all()

            self.assertTrue(len(res) == 0)

    def test_select_order_by_asc(self):

        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)

        q, params = qb.select(order_by='attr4')
        self.assertEqual(
            'SELECT "entity_id","doc" FROM "test_pg__live" '
            'ORDER BY ("doc"->>\'attr4\')::BIGINT ASC;', q)

        with self.pool.transaction() as cursor:
            res = cursor.execute(q, params)
            res = res.all()

            self.assertTrue(len(res) > 0)

            prev = None
            for row in res:
                entity = TestEntity(**row)
                if prev is not None:
                    self.assertLessEqual(prev.attr4, entity.attr4)
                prev = entity

    def test_select_order_by_desc(self):

        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)

        q, params = qb.select(order_by=(('attr7', 'DESC'), ))
        self.assertEqual(
            'SELECT "entity_id","doc" FROM "test_pg__live" '
            'ORDER BY ("doc"->>\'attr7\')::BIGINT DESC;', q)

        with self.pool.transaction() as cursor:
            res = cursor.execute(q, params)
            res = res.all()

            self.assertTrue(len(res) > 0)

            prev = None
            for row in res:
                entity = TestEntity(**row)
                if prev is not None:
                    self.assertLessEqual(arrow.from_db_val(entity.attr7),
                                         arrow.from_db_val(prev.attr7))
                prev = entity

    def test_select_where(self):

        # TODO rewrite for separate tests

        random.seed()

        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)
        raw = 'SELECT ("doc"->>\'attr4\')::BIGINT AS attr4, ' \
            'COUNT("id") as cnt ' \
            'FROM test_pg__live GROUP BY attr4;'

        with self.pool.transaction() as cursor:
            res = cursor.execute(raw)
            values = [(x['attr4'], x['cnt']) for x in res]

            attr3_values = defaultdict(int)

            attr4, cnt = random.choice(values)
            attr4_2, cnt_2 = random.choice(values)
            if attr4_2 == attr4:
                while attr4_2 != attr4:
                    attr4_2, cnt_2 = random.choice(values)
            attr4_lte_cnt = sum(x[1] for x in values if x[0] <= attr4)
            attr4_ne_cnt = sum(x[1] for x in values if x[0] != attr4)
            attr4_attr4_2_lte_cnt = sum(
                x[1] for x in values if x[0] <= attr4 or x[0] <= attr4_2)

            q, params = qb.select({'attr4': attr4})

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), cnt)

            for row in res:
                self.assertEqual(attr4, row['doc']['attr4'])

            q, params = qb.select(
                (('attr4', 'eq', attr4), )
            )

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), cnt)

            for row in res:
                self.assertEqual(attr4, row['doc']['attr4'])
                attr3_values[row['doc']['attr3']] += 1

            q, params = qb.select(
                (('attr4', 'ne', attr4), )
            )

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), attr4_ne_cnt)

            q, params = qb.select(
                (('attr4', 'lte', attr4), )
            )

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), attr4_lte_cnt)

            for row in res:
                self.assertLessEqual(row['doc']['attr4'], attr4)

            attr3 = random.choice(attr3_values.keys())
            attr3_cnt = attr3_values[attr3]

            q, params = qb.select(
                (('attr4', 'eq', attr4),
                 ('attr3', 'eq', attr3))
            )

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), attr3_cnt)

            for row in res:
                self.assertEqual(row['doc']['attr4'], attr4)
                self.assertEqual(row['doc']['attr3'], attr3)

            q, params = qb.select(
                (('attr4', 'eq', attr4), ),
                (('attr4', 'eq', attr4_2), )
            )
            self.assertTrue('OR' in q)

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), cnt + cnt_2)

            q, params = qb.select(
                {'attr4': attr4},
                {'attr4': attr4_2}
            )
            self.assertTrue('OR' in q)

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), cnt + cnt_2)

            q, params = qb.select(
                (('attr4', 'lte', attr4), ),
                (('attr4', 'lte', attr4_2), )
            )
            self.assertTrue('OR' in q)

            res = cursor.execute(q, params).all()
            self.assertEqual(len(res), attr4_attr4_2_lte_cnt)

    def test_select_offset_limit(self):

        vn = 'test_pg__live'
        qb = pg.QueryViewBuilder(vn, TestEntity)
        limit, offset = 30, 40

        q, params = qb.select(limit=limit)
        self.assertTrue('LIMIT' in q)

        q1, params1 = qb.select({'attr2': 'mistery'}, limit=limit)
        self.assertTrue('LIMIT' in q1)
        self.assertFalse('OFFSET' in q1)

        q2, params2 = qb.select({'attr2': 'mistery'},
                                limit=limit, offset=offset)
        self.assertTrue('LIMIT %s' in q2)
        self.assertTrue('OFFSET %s' in q2)

        with self.pool.transaction() as cursor:

            res = cursor.execute(q, params)
            res = res.all()
            self.assertEqual(len(res), limit)

            res = cursor.execute(q1, params1)
            res = res.all()
            self.assertEqual(len(res), limit)

            res = cursor.execute(q2, params2)
            res = res.all()
            self.assertEqual(len(res), limit)
