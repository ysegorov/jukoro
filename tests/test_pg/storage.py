# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .base import TestEntity, Base

from jukoro import pg
from jukoro.pg import storage


__all__ = ['TestDBTableName', 'TestDBViewName', 'TestSyncDBEmptySchema',
           'TestSyncDB', 'TestSqlDescr', 'TestBaseSql', 'TestRegistry']


class TestDBTableName(Base):

    def test(self):
        tn = storage.DBTableName('test_pg')
        self.assertEqual(tn.name, 'test_pg')


class TestDBViewName(Base):

    def test(self):
        vn = storage.DBViewName('test_pg')
        self.assertEqual(vn.name, 'test_pg__live')


class TestSyncDBEmptySchema(Base):

    @classmethod
    def setUpClass(cls):
        storage.unregister(TestEntity)

    @classmethod
    def tearDownClass(cls):
        storage.register(TestEntity)

    @property
    def uri(self):
        u = super(TestSyncDBEmptySchema, self).uri()
        return u + '__test_syncdb'

    @property
    def schema(self):
        s = super(TestSyncDBEmptySchema, self).schema()
        return s + '__test_syncdb'

    def _syncdb_sql(self):
        return storage.syncdb(self.uri)

    def test(self):
        create_sql, drop_sql = self._syncdb_sql()
        # logger.debug(create_sql)
        # logger.debug(drop_sql)

        self.assertEqual(create_sql.count('CREATE SCHEMA'), 1)
        self.assertEqual(create_sql.count('CREATE SEQUENCE'), 1)
        self.assertEqual(create_sql.count('CREATE TABLE'), 1)
        self.assertEqual(create_sql.count('CREATE OR REPLACE VIEW'), 0)
        self.assertEqual(create_sql.count('CREATE TRIGGER'), 0)
        self.assertEqual(create_sql.count('CREATE INDEX'), 0)
        self.assertEqual(create_sql.count('ADD CONSTRAINT'), 0)

        self.assertEqual(drop_sql.count('DROP TABLE'), 0)
        self.assertEqual(drop_sql.count('DROP VIEW'), 0)
        self.assertEqual(drop_sql.count('DROP INDEX'), 0)
        self.assertEqual(drop_sql.count('DROP CONSTRAINT'), 0)


class TestSyncDB(TestSyncDBEmptySchema):

    def test(self):
        storage.register(TestEntity)

        create_sql, drop_sql = self._syncdb_sql()

        # logger.debug(create_sql)
        self.assertEqual(create_sql.count('CREATE TABLE'), 2)
        self.assertEqual(create_sql.count('CREATE OR REPLACE VIEW'), 1)
        self.assertEqual(create_sql.count('CREATE TRIGGER'), 2)

        self.assertEqual(drop_sql.count('DROP TABLE'), 0)
        self.assertEqual(drop_sql.count('DROP VIEW'), 0)

        uri = self.uri
        conn = pg.PgConnection(uri)

        with conn.transaction() as cursor:
            cursor.execute(create_sql)

        storage.unregister(TestEntity)

        create_sql, drop_sql = self._syncdb_sql()

        self.assertEqual(create_sql.count('CREATE TABLE'), 0)
        self.assertEqual(create_sql.count('CREATE OR REPLACE VIEW'), 0)
        self.assertEqual(create_sql.count('CREATE TRIGGER'), 0)

        self.assertEqual(drop_sql.count('DROP TABLE'), 1)
        self.assertEqual(drop_sql.count('DROP VIEW'), 1)

        self.assertFalse(create_sql.strip())

        with conn.transaction() as cursor:
            cursor.execute(drop_sql)

        create_sql, drop_sql = self._syncdb_sql()

        self.assertFalse(drop_sql.strip())

        self.assertEqual(create_sql.count('CREATE TABLE'), 0)
        self.assertEqual(create_sql.count('CREATE OR REPLACE VIEW'), 0)
        self.assertEqual(create_sql.count('CREATE TRIGGER'), 0)

        self.assertEqual(drop_sql.count('DROP TABLE'), 0)
        self.assertEqual(drop_sql.count('DROP VIEW'), 0)

        with conn.transaction() as cursor:
            cursor.execute(
                'DROP SCHEMA {schema} CASCADE;'.format(schema=self.schema))

        create_sql, drop_sql = self._syncdb_sql()

        self.assertEqual(create_sql.count('CREATE SCHEMA'), 1)
        self.assertEqual(create_sql.count('CREATE TABLE'), 1)
        self.assertEqual(create_sql.count('CREATE OR REPLACE VIEW'), 0)
        self.assertEqual(create_sql.count('CREATE TRIGGER'), 0)

        self.assertEqual(drop_sql.count('DROP TABLE'), 0)
        self.assertEqual(drop_sql.count('DROP VIEW'), 0)

        conn.close()


class TestSqlDescr(Base):

    def test(self):

        class A(object):

            def __init__(self, instance):
                self.instance = instance

        class B(object):
            a = storage.SqlDescr(A)

        self.assertIs(B.a.target, A)
        self.assertIsInstance(B.a, storage.SqlDescr)
        self.assertIsInstance(B().a, A)
        with self.assertRaises(AttributeError):
            B().a = True


class TestBaseSql(Base):

    def test(self):

        class Name(object):

            def __init__(self, name):
                self.name = name

        class S1(storage.BaseSql):
            query = 'hello'

        class S2(storage.BaseSql):
            query = 'hello {db_table}'

        class S3(storage.BaseSql):
            query = 'hello {db_view}'

        class S4(storage.BaseSql):
            query = 'hello {db_table} {db_view}'

        class A(object):
            asql = storage.SqlDescr(storage.BaseSql)
            s1 = storage.SqlDescr(S1)

        class B(object):
            s2 = storage.SqlDescr(S2)

            @property
            def db_table(self):
                return Name('b')

        class C(object):
            s3 = storage.SqlDescr(S3)

            @property
            def db_view(self):
                return Name('cv')

        class D(object):
            s4 = storage.SqlDescr(S4)

            @property
            def db_table(self):
                return Name('d')

            @property
            def db_view(self):
                return Name('dv')

        a, b, c, d = A(), B(), C(), D()
        self.assertIsInstance(a.asql, storage.BaseSql)
        self.assertIsInstance(a.s1, S1)
        self.assertIsInstance(b.s2, S2)
        self.assertIsInstance(c.s3, S3)
        self.assertIsInstance(d.s4, S4)
        with self.assertRaises(AttributeError):
            sql = a.asql.sql

        self.assertDictEqual(a.asql.sql_vars(), {})
        self.assertDictEqual(a.s1.sql_vars(), {})
        self.assertDictEqual(b.s2.sql_vars(), {'db_table': 'b'})
        self.assertDictEqual(c.s3.sql_vars(), {'db_view': 'cv'})
        self.assertDictEqual(d.s4.sql_vars(),
                             {'db_view': 'dv', 'db_table': 'd'})
        self.assertEqual(a.s1.sql, 'hello')
        self.assertEqual(b.s2.sql, 'hello b')
        self.assertEqual(c.s3.sql, 'hello cv')
        self.assertEqual(d.s4.sql, 'hello d dv')


class TestRegistry(Base):

    def test(self):

        class A(pg.BaseEntity):
            pass

        class B(pg.BaseEntity):
            db_table = None

        class C(pg.BaseEntity):
            db_table = 'table_c'
            skip_registry = True

        class D(pg.BaseEntity):
            db_table = 'table_d'

        self.assertFalse(storage.is_registered(A))
        self.assertFalse(storage.is_registered(B))
        self.assertFalse(storage.is_registered(C))
        self.assertTrue(storage.is_registered(D))

        with self.assertRaises(AttributeError):
            storage.register(A)

        with self.assertRaises(AttributeError):
            storage.register(B)

        eclasses = self._entity_classes()
        self.assertNotIn(C, eclasses)
        self.assertIn(D, eclasses)

        storage.register(C)

        with self.assertRaises(pg.PgAlreadyRegisteredError):
            storage.register(C)

        with self.assertRaises(pg.PgAlreadyRegisteredError):
            storage.register(D)

        eclasses = self._entity_classes()
        self.assertIn(C, eclasses)
        self.assertTrue(storage.is_registered(C))

        storage.unregister(C)
        storage.unregister(D)

        eclasses = self._entity_classes()
        self.assertNotIn(C, eclasses)

        self.assertFalse(storage.is_registered(C))
        self.assertFalse(storage.is_registered(D))

    def _entity_classes(self):
        return [x.eclass for x in storage.tables()]
