# -*- coding: utf-8 -*-

from .base import Base

from jukoro import pg
from jukoro.pg import storage


__all__ = ['TestAttrs']


class TestAttrs(Base):

    @classmethod
    def setUpClass(cls):

        class User(pg.BaseUser):
            db_table = 'test_user'

            first_name = pg.Attr(title='First name')
            last_name = pg.Attr(title='Last name')

        cls.User = User

    @classmethod
    def tearDownClass(cls):
        storage.unregister(cls.User)
        cls.User = None

    def test_attrs_inheritance(self):
        BU = pg.BaseUser

        self.assertTrue(hasattr(BU, 'username'))
        self.assertTrue(hasattr(BU, 'email'))
        self.assertTrue(hasattr(BU, 'password'))
        self.assertFalse(hasattr(BU, 'first_name'))
        self.assertFalse(hasattr(BU, 'last_name'))

        self.assertTrue(hasattr(self.User, 'username'))
        self.assertTrue(hasattr(self.User, 'email'))
        self.assertTrue(hasattr(self.User, 'password'))
        self.assertTrue(hasattr(self.User, 'first_name'))
        self.assertTrue(hasattr(self.User, 'last_name'))

        for attr in BU.attrs:
            self.assertIsInstance(attr, pg.AttrDescr)

        for attr in self.User.attrs:
            self.assertIsInstance(attr, pg.AttrDescr)

        bu_attrs = set(BU.attrs)
        u_attrs = set(self.User.attrs)
        self.assertTrue(u_attrs.issuperset(bu_attrs))

    def test_db_table_attr(self):
        self.assertFalse(hasattr(pg.BaseUser, 'db_table'))
        self.assertFalse(hasattr(pg.BaseUser, 'db_view'))

        self.assertTrue(hasattr(self.User, 'db_table'))
        self.assertEqual(self.User.db_table, 'test_user')
        self.assertTrue(hasattr(self.User, 'db_view'))
        self.assertEqual(self.User.db_view, 'test_user__live')
