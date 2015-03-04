# -*- coding: utf-8 -*-

import datetime
import logging
import os
import unittest
import warnings

import psycopg2

from jukoro import pg
from jukoro.pg import storage


logger = logging.getLogger(__name__)


URI = os.environ.get('PG_URI', 'postgresql://localhost/jukoro_test')
TEST_URI = 'postgresql://test:test@localhost:5467/jukoro_test'
BAD_URI = 'postgresq://localhost:5432/jukoro_test.a1'

IS_ONLINE = False
SCHEMA = 'public'

SQL_TEARDOWN = """
DROP SCHEMA {schema} CASCADE;
"""


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


def setUp():
    global IS_ONLINE
    global URI
    global SCHEMA
    kwargs = pg.pg_uri_to_kwargs(URI)
    try:
        conn = psycopg2.connect(
            host=kwargs['host'], port=kwargs['port'],
            user=kwargs['user'], dbname=kwargs['dbname'])
    except psycopg2.OperationalError:
        warnings.warn(
            'Test db does not exist or unaccessible - have to skip pg tests',
            RuntimeWarning)
    else:
        IS_ONLINE = True
        if kwargs['schema'] == 'public':

            kwargs['schema'] = (
                'ju_%s' % datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            URI += '.%s' % kwargs['schema']
            SCHEMA = kwargs['schema']
        # create test schema and tables
        sql_create, sql_drop = storage.syncdb(URI)
        assert not sql_drop.strip()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_create)
        cursor.close()
        conn.close()


def tearDown():
    if not IS_ONLINE:
        return
    kwargs = pg.pg_uri_to_kwargs(URI)
    schema = kwargs['schema']
    try:
        conn = psycopg2.connect(
            host=kwargs['host'], port=kwargs['port'],
            user=kwargs['user'], dbname=kwargs['dbname'])
    except psycopg2.OperationalError:
        warnings.warn(
            'Oops, unable to clean test schema "%s", do it manually' % schema,
            RuntimeWarning)
    else:
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(SQL_TEARDOWN.format(schema=schema))
        cursor.close()
        conn.close()


class TestUri(unittest.TestCase):

    def setUp(self):
        if not IS_ONLINE:
            self.skipTest('no db available')

    def test_uri_to_kwargs_full(self):
        kwargs = pg.pg_uri_to_kwargs(TEST_URI)
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5467)
        self.assertEqual(kwargs['user'], 'test')
        self.assertEqual(kwargs['password'], 'test')
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_partial(self):
        kwargs = pg.pg_uri_to_kwargs(URI)
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 5432)
        self.assertIsNot(kwargs['user'], None)
        self.assertIs(kwargs['password'], None)
        self.assertEqual(kwargs['dbname'], 'jukoro_test')
        self.assertNotEqual(kwargs['schema'], 'public')

    def test_uri_to_kwargs_bad(self):
        self.assertRaises(pg.PgUriError, lambda: pg.pg_uri_to_kwargs(BAD_URI))


class Base(unittest.TestCase):
    pass


class BaseWithPool(unittest.TestCase):
    pool = None
    pool_size = 3
    uri_kwargs = None

    @classmethod
    def setUpClass(cls):
        cls.pool = pg.PgDbPool(URI, cls.pool_size)
        cls.uri_kwargs = pg.pg_uri_to_kwargs(URI)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()
        cls.pool = cls.uri_kwargs = None

    def setUp(self):
        if not IS_ONLINE:
            self.skipTest('no db available')


class TestWarmUp(BaseWithPool):

    def test_uri(self):
        self.assertEqual(self.pool.uri, URI)

    def test_warm_up(self):
        # FIXME do not test private members
        self.assertEqual(len(self.pool), 0)

        with self.pool.transaction() as cursor:

            self.assertTrue(self.pool._pool.is_locked(cursor._pg_conn))

        self.assertIsNone(cursor._pg_conn)

        self.assertEqual(len(self.pool), self.pool_size)


class TestConnectionParams(BaseWithPool):

    def test_connection_params(self):
        with self.pool.transaction() as cursor:

            res = cursor.execute_and_get('SHOW search_path;')
            self.assertIsInstance(res, dict)
            self.assertTrue('search_path' in res)
            self.assertTrue(self.uri_kwargs['schema'] in res['search_path'])

            res = cursor.execute_and_get('SHOW TIME ZONE;')
            self.assertEqual(res['TimeZone'], 'UTC')


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


class TestIntrospect(Base):

    def test(self):
        schema, current = pg.inspect(URI)
        self.assertTrue(schema in current.schemas)
        self.assertTrue('test_pg' in current.tables)
        self.assertTrue('test_pg__live' in current.views)
        self.assertTrue('ju_before__test_pg__insert' in current.triggers)
        self.assertTrue('ju_before__test_pg__delete' in current.triggers)
        self.assertTrue('ju_idx__test_pg__attr1_entity_start_entity_end'
                        in current.indices)
        self.assertTrue('ju_idx__test_pg__attr2_entity_start_entity_end'
                        in current.indices)
        for idx in xrange(1, 6):
            self.assertTrue('ju_validate__test_pg__attr%s' % idx
                            in current.constraints)
        self.assertFalse('ju_validate__test_pg__attr6' in current.constraints)


class TestSyncDBEmptySchema(Base):

    @classmethod
    def setUpClass(cls):
        storage.unregister(TestEntity)

    @classmethod
    def tearDownClass(cls):
        storage.register(TestEntity)

    @property
    def uri(self):
        return URI + '__test_syncdb'

    @property
    def schema(self):
        return SCHEMA + '__test_syncdb'

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
        #raise RuntimeError


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
                return 'b'

        class C(object):
            s3 = storage.SqlDescr(S3)

            @property
            def db_view(self):
                return 'cv'

        class D(object):
            s4 = storage.SqlDescr(S4)

            @property
            def db_table(self):
                return 'd'

            @property
            def db_view(self):
                return 'dv'

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


class TestStorageRegistry(Base):

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


@unittest.skip('TODO')
class TestAutoCommit(Base):

    def test_a(self):
        pass

    def test_b(self):
        pass


@unittest.skip('TODO')
class TestManualCommit(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestRollback(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestNamedCursor(Base):

    def test_a(self):
        pass


@unittest.skip('TODO')
class TestFetch(Base):

    def test_fetch_one(self):
        pass

    def test_fetch_all(self):
        pass

    def test_fetch_many(self):
        pass

    def test_scroll(self):
        pass
