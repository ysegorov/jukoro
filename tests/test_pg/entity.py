# -*- coding: utf-8 -*-

from jukoro import arrow

from jukoro import pg
from jukoro.pg import storage

from .base import Base, BaseWithPool, TestEntity


__all__ = ['TestAbstractEntity', 'TestEntityMeta']


class TestAbstractEntity(Base):

    @classmethod
    def setUpClass(cls):

        class User(pg.AbstractUser):
            db_table = 'test_user1'

            first_name = pg.Attr(title='First name')
            last_name = pg.Attr(title='Last name')

        cls.User = User

    @classmethod
    def tearDownClass(cls):
        storage.unregister(cls.User)
        cls.User = None

    def test_db_table_attr(self):
        self.assertFalse(hasattr(pg.AbstractUser, 'db_table'))

        self.assertTrue(hasattr(self.User, 'db_table'))
        self.assertEqual(self.User.db_table.name, 'test_user1')

    def test_db_view_attr(self):
        self.assertFalse(hasattr(pg.AbstractUser, 'db_view'))

        self.assertTrue(hasattr(self.User, 'db_view'))
        self.assertEqual(self.User.db_view.name, 'test_user1__live')

    def test_compare(self):
        a = self.User(123, {'first_name': 'A', 'last_name': 'B'})
        b = self.User(123, {'first_name': 'A', 'last_name': 'B',
                            'username': 'u'})
        c = self.User(123, {'first_name': 'A', 'last_name': 'B'})
        d = self.User(122, {'first_name': 'A', 'last_name': 'B'})
        self.assertEqual(a, c)
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, d)
        self.assertNotEqual(b, c)
        self.assertNotEqual(b, d)

        with self.assertRaises(RuntimeError):
            a == 1
        with self.assertRaises(RuntimeError):
            a != 1

    def test_serialize_deserialize(self):
        a = self.User(123, {'first_name': 'A', 'last_name': 'B'})
        jsoned = a.serialize()
        self.assertIsInstance(jsoned, basestring)

        b = self.User.deserialize(jsoned)
        self.assertIsInstance(b, self.User)
        self.assertEqual(a, b)


class TestEntityMeta(BaseWithPool):

    def test_created(self):

        with self.assertRaises(AttributeError):

            class A(pg.AbstractEntity):
                _created = pg.Attr(title='created')

        a = TestEntity()
        self.assertTrue(hasattr(a, 'created'))
        self.assertIsNone(a.created)

        a.update(attr1='miracle-a',
                 attr2='musician-a',
                 attr3='boundary-a',
                 attr4=1,
                 attr5=101,
                 attr7=arrow.utcnow())

        with self.pool.transaction() as cursor:
            a = a.save(cursor)

            self.assertIsNotNone(a.created)

    def test_updated(self):

        with self.assertRaises(AttributeError):

            class B(pg.AbstractEntity):
                _updated = pg.Attr(title='updated')

        b = TestEntity()
        self.assertTrue(hasattr(b, 'updated'))
        self.assertIsNone(b.updated)

        b.update(attr1='miracle-b',
                 attr2='musician-b',
                 attr3='boundary-b',
                 attr4=2,
                 attr5=102,
                 attr7=arrow.utcnow())

        with self.pool.transaction() as cursor:
            b = b.save(cursor)

            self.assertIsNotNone(b.entity_id)
            self.assertIsNotNone(b.created)
            self.assertIsNotNone(b.updated)
            self.assertEqual(b.created, b.updated)

        with self.pool.transaction() as cursor:
            b.update(attr5=1022)
            bb = b.save(cursor)
            self.assertIsNotNone(bb.updated)
            self.assertEqual(b.entity_id, bb.entity_id)
            self.assertEqual(b.created, bb.created)
            self.assertNotEqual(b.updated, bb.updated)

    def test_deleted(self):

        with self.assertRaises(AttributeError):

            class C(pg.AbstractEntity):
                _deleted = pg.Attr(title='deleted')

        c = TestEntity()
        self.assertTrue(hasattr(c, 'deleted'))
        self.assertIsNone(c.deleted)

        c.update(attr1='miracle-c',
                 attr2='musician-c',
                 attr3='boundary-c',
                 attr4=3,
                 attr5=103,
                 attr7=arrow.utcnow())

        with self.pool.transaction() as cursor:
            c = c.save(cursor)
            cid = c.entity_id

            self.assertEqual(c.created, c.updated)

        with self.pool.transaction() as cursor:
            c.delete(cursor)

        with self.pool.transaction() as cursor:

            with self.assertRaises(pg.DoesNotExist):
                c = TestEntity.by_id(cursor, cid)

            q = 'SELECT * FROM "test_pg" WHERE "entity_id" = %s ' \
                'ORDER BY "entity_end" DESC'
            res = cursor.execute(q, (cid, )).all()
            self.assertTrue(len(res) == 1)

            doc = res[0]['doc']
            self.assertTrue('_deleted' in doc)
