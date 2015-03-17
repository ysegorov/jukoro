# -*- coding: utf-8 -*-

import logging
import threading
import uuid

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from jukoro.decorators import raise_if
from jukoro.structures import LockRing

from jukoro.pg.exceptions import (
    PgPoolClosedError, PgConnectionClosedError, PgCursorClosedError,
    PgDoesNotExistError)
from jukoro.pg.utils import pg_uri_to_kwargs


BLOCK_SIZE = 2048

logger = logging.getLogger(__name__)
sql_logger = logging.getLogger('jukoro.pg.sql')


def is_closed(inst, *args, **kwargs):
    return getattr(inst, 'is_closed', False)


raise_if_cursor_closed = raise_if(PgCursorClosedError,
                                  'cursor closed', is_closed)
raise_if_connection_closed = raise_if(PgConnectionClosedError,
                                      'connection closed', is_closed)
raise_if_pool_closed = raise_if(PgPoolClosedError,
                                'pool closed', is_closed)


class PgResult(object):

    __slots__ = ('_cursor', )

    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def is_closed(self):
        return self._cursor is None

    @raise_if_cursor_closed
    def get(self):
        try:
            return self[0]
        except psycopg2.ProgrammingError:
            raise PgDoesNotExistError

    @raise_if_cursor_closed
    def all(self):
        return self._cursor.fetchall()

    @raise_if_cursor_closed
    def block(self):
        return self._cursor.fetchmany()

    def __iter__(self):
        # named cursor will transparently work the same way the client-side
        # cursor does
        block = self.block()
        while block:
            for it in block:
                yield it
            block = self.block()

    @property
    @raise_if_cursor_closed
    def rowcount(self):
        return self._cursor.rowcount

    @raise_if_cursor_closed
    def __len__(self):
        if self._cursor.name is not None:
            return 0
        return self._cursor.rowcount

    def __getitem__(self, value):
        if isinstance(value, int):
            self.scroll(value)
            resp = self._cursor.fetchone()
            if resp:
                return resp
            return
        if isinstance(value, slice):
            # TODO raise for negative positions
            start = value.start or 0
            stop = value.stop
            if stop is None:
                raise ValueError('undefined value for upper boundary')
            # TODO respect step
            # step = value.step
            self.scroll(start)
            return self._cursor.fetchmany(stop - start)

    def close(self):
        self._cursor = None

    @raise_if_cursor_closed
    def scroll(self, pos):
        try:
            self._cursor.scroll(pos, 'absolute')
        except (psycopg2.ProgrammingError, IndexError):
            raise PgDoesNotExistError


class PgTransaction(object):

    __slots__ = ('_pg_conn', '_autocommit', '_named', '_cursor', '_failed',
                 '_result', '_closed', '_queries', '_block_size')

    def __init__(self, conn, autocommit=True, named=False, **kwargs):
        if named and autocommit:
            logger.warn(
                'incompatible parameters "autocommit = named = True"')
            autocommit = False
        self._pg_conn = conn  # instance of PgConnection
        self._autocommit = autocommit
        self._named = named
        self._cursor = None
        self._failed = False
        self._result = None
        self._closed = False
        self._queries = []
        self._block_size = kwargs.get('block_size', BLOCK_SIZE)

    def _ensure_cursor(self):
        if self._cursor is not None:
            return
        if self._named or not self._autocommit:
            if self._pg_conn.autocommit:
                self._pg_conn.autocommit = False
        else:
            if not self._pg_conn.autocommit:
                self._pg_conn.autocommit = True
        self._cursor = self._pg_conn.cursor(self._named)
        self._cursor.arraysize = self._block_size
        if self._named:
            self._cursor.itersize = self._block_size

    def __enter__(self):
        self._ensure_cursor()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            logger.exception('exception executing query')
            if not self._autocommit:
                self._pg_conn.rollback()
            self._failed = True
        else:
            if self._named or not self._autocommit:
                self._pg_conn.commit()
        self.close()

    @property
    def block_size(self):
        return self._block_size

    @property
    def arraysize(self):
        self._ensure_cursor()
        return self._cursor.arraysize

    @property
    def itersize(self):
        self._ensure_cursor()
        return self._cursor.itersize

    @property
    def is_closed(self):
        return self._closed

    @property
    def queries(self):
        for q in self._queries:
            yield q

    def _should_close_cursor(self):
        if (not self._named and self._cursor is not None) \
                or (self._named and not self._failed and self._queries):
            return True
        return False

    def close(self):
        self._close_result()
        if self._should_close_cursor():
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
        return self._exec(query, params=params)

    def execute_and_get(self, query, params=None):
        return self.execute(query, params).get()

    def callproc(self, procname, params=None):
        return self._exec(procname, params=params, proc=True)

    def _exec(self, q_or_proc, params=None, proc=False):
        self._ensure_cursor()
        fn = self._cursor.callproc if proc else self._cursor.execute
        sql_logger.debug(
            'executing query "%s" with "%s"', q_or_proc, params or [])
        fn(q_or_proc, params)
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
    @raise_if_connection_closed
    def conn(self):
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

    @raise_if_connection_closed
    def commit(self):
        self.conn.commit()

    @raise_if_connection_closed
    def rollback(self):
        self.conn.rollback()

    @raise_if_connection_closed
    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._pg_pool = None
        self._closed = True

    @raise_if_connection_closed
    def cursor(self, named=False):
        if named:
            return self.conn.cursor(
                name=str(uuid.uuid4()), scrollable=True, withhold=True)
        return self.conn.cursor()

    @raise_if_connection_closed
    def reattach(self):
        if self._pg_pool is not None:
            self._pg_pool.unlock(self)
        elif self._autoclose:
            self.close()

    @raise_if_connection_closed
    def transaction(self, **kwargs):
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

    @raise_if_pool_closed
    def transaction(self, **kwargs):
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

    @raise_if_pool_closed
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
