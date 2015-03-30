# -*- coding: utf-8 -*-

from jukoro import pg
from jukoro.pg import storage

from .base import Base


__all__ = ['TestBaseEntity']


class TestBaseEntity(Base):

    @classmethod
    def setUpClass(cls):

        class User(pg.BaseUser):
            db_table = 'test_user1'

            first_name = pg.Attr(title='First name')
            last_name = pg.Attr(title='Last name')

        cls.User = User

    @classmethod
    def tearDownClass(cls):
        storage.unregister(cls.User)
        cls.User = None

    def test_db_table_attr(self):
        self.assertFalse(hasattr(pg.BaseUser, 'db_table'))

        self.assertTrue(hasattr(self.User, 'db_table'))
        self.assertEqual(self.User.db_table.name, 'test_user1')

    def test_db_view_attr(self):
        self.assertFalse(hasattr(pg.BaseUser, 'db_view'))

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
