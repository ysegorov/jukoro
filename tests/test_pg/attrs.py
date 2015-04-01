# -*- coding: utf-8 -*-

import logging

from .base import Base, TestEntity

from jukoro import arrow
from jukoro import pg
from jukoro.pg import storage


__all__ = ['TestAttr', 'TestAttrs']

logger = logging.getLogger(__name__)


class TestAttr(Base):

    def test_a(self):
        a = pg.Attr(title='a')
        self.assertIs(a.value_type, unicode)
        self.assertFalse(a.is_int)
        self.assertTrue(a.is_text)
        self.assertFalse(a.db_index)
        self.assertTrue(a.db_not_null)
        self.assertEqual(a.db_cast(), 'TEXT')
        self.assertEqual(a.title, 'a')
        self.assertGreater(a.idx, 0)

    def test_b(self):
        b = pg.Attr(title='b', value_type=int)
        self.assertIs(b.value_type, int)
        self.assertTrue(b.is_int)
        self.assertFalse(b.is_text)
        self.assertEqual(b.db_cast(), 'BIGINT')

    def test_c(self):
        c = pg.Attr(title='c', value_type=arrow.JuArrow)
        self.assertIs(c.value_type, arrow.JuArrow)
        self.assertTrue(c.is_int)
        self.assertFalse(c.is_text)
        self.assertEqual(c.db_cast(), 'BIGINT')

    def test_d(self):
        c = pg.Attr(title='c', value_type=arrow.JuArrow)
        d = pg.Attr(title='c', value_type=int)
        self.assertTrue(c.idx < d.idx)
        self.assertTrue(c < d)
        self.assertFalse(c == d)

        with self.assertRaises(RuntimeError):
            c < 4


class TestAttrs(Base):

    @classmethod
    def setUpClass(cls):

        class User(pg.AbstractUser):
            db_table = 'test_user'

            first_name = pg.Attr(title='First name')
            last_name = pg.Attr(title='Last name')

        cls.User = User

    @classmethod
    def tearDownClass(cls):
        storage.unregister(cls.User)
        cls.User = None

    def test_attrs_inheritance(self):
        BU = pg.AbstractUser

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

    def test_attr_cmp(self):
        self.assertTrue(self.User.first_name.idx < self.User.last_name.idx)
        self.assertTrue(TestEntity.attr1.idx < TestEntity.attr4.idx)
        self.assertTrue(TestEntity.attr1 < TestEntity.attr4)
        self.assertTrue(TestEntity.attr6.idx > TestEntity.attr3.idx)
        self.assertTrue(TestEntity.attr6 > TestEntity.attr3)

    def test_user_attr_ordering(self):
        prev = None
        for attr in self.User.attrs:
            if prev is not None:
                self.assertTrue(prev.idx < attr.idx)
            prev = attr

    def test_testentity_attr_ordering(self):
        prev = None
        for attr in self.User.attrs:
            if prev is not None:
                self.assertTrue(prev.idx < attr.idx)
            prev = attr
