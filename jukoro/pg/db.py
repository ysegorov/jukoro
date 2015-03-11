# -*- coding: utf-8 -*-

import logging
import threading
import uuid
import warnings

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from jukoro.structures import LockRing

from jukoro.pg.exceptions import PgPoolClosedError, PgConnectionClosedError
from jukoro.pg.utils import pg_uri_to_kwargs


logger = logging.getLogger(__name__)
sql_logger = logging.getLogger('jukoro.pg.sql')


class PgResult(object):

    __slots__ = ('_cursor', '_block_size')

    def __init__(self, cursor, block_size=2048):
        self._cursor = cursor
        self._block_size = block_size

    def get(self):
        return self[0]

    def all(self):
        return self._cursor.fetchall()

    def _block(self):
        return self._cursor.fetchmany(self._block_size)

    def __iter__(self):
        block = self._block()
        while block:
            for it in block:
                yield it
            block = self._block()

    def __len__(self):
        return self._cursor.rowcount

    def __getitem__(self, value):
        if isinstance(value, int):
            self.scroll(value)
            resp = self._cursor.fetchone()
            if resp:
                return resp
            return
        if isinstance(value, slice):
            start = value.start
            stop = value.stop
            step = value.step
            # TODO raise for negative positions
            # TODO respect step
            self.scroll(start)
            return self._cursor.fetchmany(stop - start)

    def close(self):
        self._cursor = None

    def scroll(self, pos):
        # TODO raise for out-of-bounds?
        if pos >= len(self):
            pos = len(self) - 1
        if pos < 0:
            pos = 0
        self._cursor.scroll(pos, 'absolute')


class PgTransaction(object):

    __slots__ = ('_pg_conn', '_autocommit', '_named', '_cursor', '_failed',
                 '_result', '_closed', '_queries')

    def __init__(self, conn, autocommit=True, named=False):
        if autocommit and named:
            warnings.warn(
                'incompatible parameters "autocommit = named = True"')
        self._pg_conn = conn  # instance of PgConnection
        self._autocommit = autocommit
        self._named = named
        self._cursor = None
        self._failed = False
        self._result = None
        self._closed = False
        self._queries = []

    def __enter__(self):
        if self._named or not self._autocommit:
            if self._pg_conn.autocommit:
                self._pg_conn.autocommit = False
        else:
            if not self._pg_conn.autocommit:
                self._pg_conn.autocommit = True
        self._cursor = self._pg_conn.cursor(self._named)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            logger.debug('rollback!', exc_info=True)
            self._pg_conn.rollback()
            self._failed = True
        else:
            if self._named or not self._autocommit:
                self._pg_conn.commit()
        self.close()

    @property
    def is_closed(self):
        return self._closed

    @property
    def queries(self):
        for q in self._queries:
            yield q

    def close(self):
        self._close_result()
        if not (self._failed and self._named):
            if self._cursor is not None:
                self._cursor.close()
        self._cursor = self._queries = None
        self._pg_conn.reattach()
        self._pg_conn = None
        self._closed = True

    def _close_result(self):
        if self._result is not None:
            self._result.close()
            self._result = None

    def execute(self, query, params=None):
        sql_logger.debug('executing query "%s"', query)
        self._cursor.execute(query, params)
        self._queries.append(self._cursor.query)
        self._close_result()
        self._result = PgResult(self._cursor)
        return self._result

    def execute_and_get(self, query, params=None):
        return self.execute(query, params).get()

    def callproc(self, procname, params=None):
        self._cursor.callproc(procname, params)
        self._queries.append(self._cursor.query)
        self._close_result()
        self._result = PgResult(self._cursor)
        return self._result


class PgConnection(object):

    __slots__ = ('_uri', '_schema', '_pg_pool', '_conn_kwargs', '_conn',
                 '_autoclose', '_closed')

    def __init__(self, uri, pool=None, autoclose=False):
        self._uri = uri
        self._pg_pool = pool
        kwargs = pg_uri_to_kwargs(uri)
        self._schema = kwargs.pop('schema')
        self._conn_kwargs = kwargs
        self._conn = None  # psycopg2.connection
        self._autoclose = autoclose
        self._closed = False

        # logger.debug('connection created %s', repr(self))

    def __repr__(self):
        return '<PgConnection(uri="{}")> at {}'.format(self._uri,
                                                       hex(id(self)))

    @property
    def conn(self):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        if self._conn is None:
            self._conn = _connect(**self._conn_kwargs)
            self._conn.set_session(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
                autocommit=True)
            self._conn.set_client_encoding('UTF8')
            self._conn.autocommit = True
            cur = self._conn.cursor()
            cur.execute('SET TIME ZONE "UTC";')
            cur.execute('SET search_path TO %s;', (self.schema, ))
            cur.close()
        return self._conn

    @property
    def autocommit(self):
        return self.conn.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.conn.autocommit = value

    @property
    def is_closed(self):
        return self._closed

    @property
    def schema(self):
        return self._schema

    def commit(self):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        self.conn.commit()

    def rollback(self):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        self.conn.rollback()

    def close(self):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._pg_pool = None
        self._closed = True

    def cursor(self, named=False):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        if named:
            return self.conn.cursor(
                name=str(uuid.uuid4()), scrollable=True, withhold=True)
        return self.conn.cursor()

    def reattach(self):
        if self._pg_pool is not None:
            self._pg_pool.unlock(self)
        elif self._autoclose:
            self.close()

    def transaction(self, **kwargs):
        if self.is_closed:
            raise PgConnectionClosedError('connection closed')
        return PgTransaction(self, **kwargs)


class PgDbPool(object):

    __slots__ = ('_uri', '_pool_size', '_pool',
                 '_warmed_up', '_closed', '_lock')

    def __init__(self, uri, pool_size=5, **kwargs):
        self._uri = uri
        self._pool_size = pool_size
        self._pool = LockRing()
        self._warmed_up = False
        self._closed = False
        self._lock = threading.Lock()

    @property
    def is_closed(self):
        return self._closed

    @property
    def uri(self):
        return self._uri

    def __repr__(self):
        return '<PgDbPool("{}")> at {}'.format(self._uri, hex(id(self)))

    def __len__(self):
        return len(self._pool)

    def close(self):
        with self._lock:
            for __ in xrange(len(self._pool)):
                conn = self._pool.pop()
                conn.close()
            self._closed = True
            self._pool.reset()

    def transaction(self, **kwargs):
        if self.is_closed:
            raise PgPoolClosedError('pool closed')
        with self._lock:
            conn = self._get_conn()
        return conn.transaction(**kwargs)

    def _get_conn(self):
        if not self._warmed_up:
            self._warm_up()
        try:
            conn = self._pool.next()
        except (IndexError, StopIteration):
            logger.error('pool exhausted, making new connection')
            conn = self._new_conn(autoclose=True)
        return conn

    def _new_conn(self, **kwargs):
        return PgConnection(self._uri, **kwargs)

    def unlock(self, conn):
        self._pool.push(conn)

    def _warm_up(self):
        self._warmed_up = True
        for __ in xrange(self._pool_size):
            self._pool.push(self._new_conn(pool=self))
        logger.info(
            'warmed up pool "%s" length %s', repr(self), self._pool_size)


def _connect(**kwargs):
    kwargs.setdefault('cursor_factory', psycopg2.extras.RealDictCursor)
    return psycopg2.connect(**kwargs)
