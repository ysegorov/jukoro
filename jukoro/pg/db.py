# -*- coding: utf-8 -*-
"""
Module to provide abstractions to work with PostgreSQL built on top of
``psycopg2.extensions.connection`` and ``psycopg2.extensions.cursor``.

- :class:`PgDbPool <jukoro.pg.db.PgDbPool>` - abstraction for a pool of
  connections
- :class:`PgConnection <jukoro.pg.db.PgConnection>` - abstraction for a
  connection
- :class:`PgTransaction <jukoro.pg.db.PgTransaction>` - abstraction for a
  transaction
- :class:`PgResult <jukoro.pg.db.PgResult>` - abstraction for results


"""

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


# block size for PostgreSQL fetched rows iteration
BLOCK_SIZE = 2048

# module level logger
logger = logging.getLogger(__name__)
# sql queries logger
sql_logger = logging.getLogger('jukoro.pg.sql')


def is_closed(instance, *args, **kwargs):
    """
    Helper function to test if some instance is closed (to be used with
    ``raise_if`` decorator)

    :param instance: some instance with ``is_closed`` property accessible
    :returns:        value of ``instance.is_closed`` or False

    """
    return getattr(instance, 'is_closed', False)


# decorator to test if cursor closed
raise_if_cursor_closed = raise_if(PgCursorClosedError,
                                  'cursor closed', is_closed)
# decorator to test if connection closed
raise_if_connection_closed = raise_if(PgConnectionClosedError,
                                      'connection closed', is_closed)
# decorator to test if pool closed
raise_if_pool_closed = raise_if(PgPoolClosedError,
                                'pool closed', is_closed)


class PgResult(object):
    """
    Provides methods to work with query results

    :param cursor: instance of `psycopg2.extensions.cursor`

    """

    __slots__ = ('_cursor', )

    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def is_closed(self):
        """
        Returns current status of the instance

        """
        return self._cursor is None

    @raise_if_cursor_closed
    def get(self):
        """
        Method to get first row from query result

        :returns: fetched from db row
        :rtype:   dict (`psycopg2.extras.RealDictCursor`)
        :raises PgDoesNotExistError: if query returned no results

        """
        try:
            return self[0]
        except psycopg2.ProgrammingError:
            raise PgDoesNotExistError

    @raise_if_cursor_closed
    def all(self):
        """
        Method to fetch all rows from cursor

        :returns: all rows fetched from db
        :rtype:   list

        """
        return self._cursor.fetchall()

    @raise_if_cursor_closed
    def block(self):
        """
        Method to fetch block of rows from cursor

        :returns: block of rows fetched from db
        :rtype:   list

        """
        return self._cursor.fetchmany()

    def __iter__(self):
        """
        Generator function to iterate over the rows from db
        (iterates using blocks to keep memory usage low if possible)

        """
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
        """
        Returns `psycopg2.extensions.cursor.rowcount` read-only attribute value

        """
        return self._cursor.rowcount

    @raise_if_cursor_closed
    def __len__(self):
        """
        Magic method to get length of the instance

        Returns number of rows fetched or affected by query
        In case cursor has a name (ie it's a named cursor) return 0

        """
        if self._cursor.name is not None:
            return 0
        return self._cursor.rowcount

    def __getitem__(self, value):
        """
        Magic method to provide access to specific row in results or
        to slice query results

        :param value: index (integer) or slice
        :returns:     single row or list of rows

        NB. doesn't support step value for slicing

        """
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
        """
        Method to close results after processing (free cursor instance)

        """
        self._cursor = None

    @raise_if_cursor_closed
    def scroll(self, pos):
        """
        Method to scroll to specific position within fetched results

        :param pos: (int) position to scroll to
        :raises PgDoesNotExistError: if position is unavailable

        """
        try:
            self._cursor.scroll(pos, 'absolute')
        except (psycopg2.ProgrammingError, IndexError):
            raise PgDoesNotExistError


class PgTransaction(object):
    """
    Provides a way to work with ``psycopg2.extensions.cursor``

    :param conn:       instance of PgConnection
    :param autocommit: set autocommit cursor mode on (True) or off (False)
    :param named:      set named cursor mode on (True) or off (False)
    :param block_size: set size of block to iterate over the results
                       (defaults to BLOCK_SIZE)

    NB. initializing with ``autocommit=True`` and ``named=True`` is wrong
    because there is no autocommit mode for named cursor. ``named`` parameter
    considered to have a higher priority.

    """

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
        self._queries = []  # list of queries performed using this instance
        self._block_size = kwargs.get('block_size', BLOCK_SIZE)

    def _ensure_cursor(self):
        """
        Ensures instance have a properly initialized cursor

        Cursor is created using ``PgConnection.cursor`` method

        """
        if self._cursor is not None:
            return
        # FIXME check if _pg_conn is available
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
        """
        Magic method for instance to act as a context manager

        """
        self._ensure_cursor()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """
        Magic method to cleanup on exit from context manager

        Calls ``PgConnection.rollback`` method on case of exception or
        ``PgConnection.commit`` method otherwise
        (with respect to cursor ``named`` and ``autocommit`` settings)

        """
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
    def arraysize(self):
        """
        Returns value of ``arraysize`` property of cursor

        """
        self._ensure_cursor()
        return self._cursor.arraysize

    @property
    def itersize(self):
        """
        Returns value of ``itersize`` property of cursor

        """
        self._ensure_cursor()
        return self._cursor.itersize

    @property
    def is_closed(self):
        """
        Returns current state of instance

        """
        return self._closed

    @property
    def queries(self):
        """
        Iterator over queries executed using this instance

        """
        for q in self._queries:
            yield q

    def _should_close_cursor(self):
        """
        Internal method to detect if we should close cursor or
        it was closed by server
        (server can close named cursor in case of error)

        :returns: boolean value

        """
        if (not self._named and self._cursor is not None) \
                or (self._named and not self._failed and self._queries):
            return True
        return False

    def close(self):
        """
        Closes instance (explicit way to free resources)

        """
        self._close_result()
        if self._should_close_cursor():
            self._cursor.close()
        self._cursor = self._queries = None
        self._pg_conn.reattach()
        self._pg_conn = None
        self._closed = True

    def _close_result(self):
        """
        Closes result if any (explicit way to free resources)

        """
        if self._result is not None:
            self._result.close()
            self._result = None

    def execute(self, query, params=None):
        """
        Executes query using provided parameters

        :param query:   sql query to execute
        :param params:  parameters for sql query (None or tuple)
        :returns:       results of query
        :rtype:         instance of ``PgResult``

        """
        return self._exec(query, params=params)

    def execute_and_get(self, query, params=None):
        """
        Executes query using provided parameters
        and returns first row from the results

        :param query:   sql query to execute
        :param params:  parameters for sql query (None or tuple)
        :returns:       first row from the results of thq query
        :rtype:         defaults to dict
                        (due to ``psycopg2.extras.RealDictCursor``)

        """
        return self.execute(query, params).get()

    def callproc(self, procname, params=None):
        """
        Executes stored procedure using provided parameters

        :param procname:    stored procedure to execute
        :param params:      parameters for procedure (None or tuple)
        :returns:           results of stored procedure call
        :rtype:             instance of ``PgResult``

        """

        return self._exec(procname, params=params, proc=True)

    def _exec(self, q_or_proc, params=None, proc=False):
        """
        Internal method to actually execute query or stored procedure using
        provided parameters
        Before execution ensures we have a cursor and closes results in case
        we already have them from previous exec
        Stores query or stored procedure name in internal queries cache
        (without parameters)

        :param q_or_proc:   query or stored procedure to execute
        :param params:      parameters for execution (None or tuple)
        :returns:           results of call
        :rtype:             instance of ``PgResult``

        """
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
    """
    Provides a way to work with ``psycopg2.extensions.connection``

    Lazily establishes connection to PostgreSQL and initializes
    parameters of connection:

    - client encoding to UTF8
    - time zone to UTC
    - search path to schema name from uri
      (defaults to "public" if not specified)
    - autocommit to True
    - transaction isolation level to READ COMMITED

    :param uri:         connection string
    :param pool:        instance of ``PgDbPool`` to return connection to
    :param autoclose:   if True closes connection after exit from transaction

    Usage example::

        >>> from jukoro import pg
        >>> uri = 'postgresql://localhost/jukoro_test.test_schema'
        >>> conn = pg.PgConnection(uri)
        >>> conn
        <PgConnection(uri="postgresql://localhost/jukoro_test.test_schema")> at 0x7fec361a7600
        >>> conn.conn
        <connection object at 0x7fec361be910; dsn: 'user=egorov host=localhost port=5432 dbname=jukoro_test', closed: 0>
        >>> conn.conn
        <connection object at 0x7fec361be910; dsn: 'user=egorov host=localhost port=5432 dbname=jukoro_test', closed: 0>
        >>> with conn.transaction() as cursor:
        ...     r = cursor.execute('SELECT \'{"a": 1}\' as "doc";')
        ...     r
        ...     doc = r.get()
        ...
        <jukoro.pg.db.PgResult object at 0x7fec3d05e638>
        >>> doc
        {'doc': '{"a": 1}'}
        >>> r
        <jukoro.pg.db.PgResult object at 0x7fec3d05e638>
        >>> r.is_closed
        True
        >>> cursor
        <jukoro.pg.db.PgTransaction object at 0x7fec361bd1b8>
        >>> cursor.is_closed
        True
        >>> conn
        <PgConnection(uri="postgresql://localhost/jukoro_test.test_schema")> at 0x7fec361a7600
        >>> conn.is_closed
        False
        >>> conn.close()
        >>> conn.is_closed
        True
        >>> cursor = conn.transaction()
        Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "jukoro/decorators.py", line 146, in wrapper
            raise exc_type(msg)
        jukoro.pg.exceptions.PgConnectionClosedError: connection closed
        >>> conn.conn
        Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "jukoro/decorators.py", line 146, in wrapper
            raise exc_type(msg)
        jukoro.pg.exceptions.PgConnectionClosedError: connection closed

    """

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
        """
        Initialize and return ``psycopg2.extensions.connection`` instance

        """
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
        """
        Returns connection ``autocommit`` property value

        """
        return self.conn.autocommit

    @autocommit.setter
    def autocommit(self, value):
        """
        Sets connection ``autocommit`` property to value

        :param value: boolean to set autocommit to

        """
        self.conn.autocommit = value

    @property
    def is_closed(self):
        """
        Returns current state of instance

        :rtype: boolean

        """
        return self._closed

    @property
    def schema(self):
        """
        Returns schema name

        :rtype: string

        """
        return self._schema

    @raise_if_connection_closed
    def commit(self):
        """
        Calls connection ``commit`` method

        """
        self.conn.commit()

    @raise_if_connection_closed
    def rollback(self):
        """
        Calls connection ``rollback`` method

        """
        self.conn.rollback()

    @raise_if_connection_closed
    def close(self):
        """
        Closes instance (explicit way to free resources)

        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._pg_pool = None
        self._closed = True

    @raise_if_connection_closed
    def cursor(self, named=False):
        """
        Creates new cursor for connection

        :param named:   if True creates named cursor
        :returns:       cursor
        :rtype:         ``psycopg2.extensions.cursor``

        """
        if named:
            return self.conn.cursor(
                name=str(uuid.uuid4()), scrollable=True, withhold=True)
        return self.conn.cursor()

    @raise_if_connection_closed
    def reattach(self):
        """
        Reattaches connection to ``PgDbPool`` instance
        or closes connection if ``autoclose`` was set to True

        """
        if self._pg_pool is not None:
            self._pg_pool.unlock(self)
        elif self._autoclose:
            self.close()

    @raise_if_connection_closed
    def transaction(self, **kwargs):
        """
        Starts new transaction

        :params kwargs: keyword arguments to initialize
                        transaction manager with
        :returns:       transaction manager
        :rtype:         instance of ``PgTransaction``

        """
        return PgTransaction(self, **kwargs)


class PgDbPool(object):
    """
    Manages pool of ``PgConnection`` instances
    In case all connections are busy (pool is exhausted) will transparently
    create new ``PgConnection`` with ``autoclose=True`` parameter

    :param uri:         connection string
    :param pool_size:   size of pool to manage

    Usage example::

        >>> from jukoro import pg
        >>> uri = 'postgresql://localhost/jukoro_test.test_schema'
        >>> pool = pg.PgDbPool(uri)
        >>> len(pool)
        0
        >>> with pool.transaction() as cursor:
        ...     r = cursor.execute('SELECT \'{"a": 1}\'::json as "doc";')
        ...     doc = r.get()
        ...
        >>> doc
        {'doc': {u'a': 1}}
        >>> len(pool)
        5
        >>> cursor.is_closed
        True
        >>> r.is_closed
        True
        >>> r.get()
        Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "jukoro/decorators.py", line 146, in wrapper
            raise exc_type(msg)
        jukoro.pg.exceptions.PgCursorClosedError: cursor closed
        >>> pool.close()
        >>> len(pool)
        0
        >>>

    """

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
        """
        Returns current state of instance

        :rtype: boolean

        """
        return self._closed

    @property
    def uri(self):
        """
        Returns connection string

        :rtype: string

        """
        return self._uri

    def __repr__(self):
        return '<PgDbPool("{}")> at {}'.format(self._uri, hex(id(self)))

    def __len__(self):
        """
        Returns current actual length of pool

        :rtype: int

        """
        return len(self._pool)

    def close(self):
        """
        Closes pool closing all connections

        """
        with self._lock:
            for __ in xrange(len(self._pool)):
                conn = self._pool.pop()
                conn.close()
            self._closed = True
            self._pool.reset()

    @raise_if_pool_closed
    def transaction(self, **kwargs):
        """
        Creates new transaction selecting connection from pool and calling
        ``PgConnection.transaction`` method

        :param kwargs:  keyword arguments to initialize ``PgTransaction``
                        instance
        :returns:       transaction manager instance
        :rtype:         ``PgTransaction``

        """
        with self._lock:
            conn = self._get_conn()
        return conn.transaction(**kwargs)

    def _get_conn(self):
        """
        Returns connection from pool if available
        Creates free connection if pool is exhausted
        Warms up pool if it is not yet warmed up

        :returns:   connection manager
        :rtype:     ``PgConnection``

        """
        if not self._warmed_up:
            self._warm_up()
        try:
            conn = self._pool.next()
        except (IndexError, StopIteration):
            logger.error('pool exhausted, making new connection')
            conn = self._new_conn(autoclose=True)
        return conn

    def _new_conn(self, **kwargs):
        """
        Returns new instance of ``PgConnection``

        """
        return PgConnection(self._uri, **kwargs)

    @raise_if_pool_closed
    def unlock(self, conn):
        """
        Unlocks connection after usage
        (returns it to pool and makes available to chose)

        :param conn:    instance of ``PgConnection`` from the pool

        """
        self._pool.push(conn)

    def _warm_up(self):
        """
        Warms up pool establishing connection to PostgreSQL

        """
        self._warmed_up = True
        for __ in xrange(self._pool_size):
            self._pool.push(self._new_conn(pool=self))
        logger.info(
            'warmed up pool "%s" length %s', repr(self), self._pool_size)


def _connect(**kwargs):
    """
    Creates and returns new ``psycopg2.extensions.connection`` instance using
    ``psycopg2.extras.RealDictCursor`` cursor factory by default

    Cursor factory can be overriden in ``kwargs``

    """
    kwargs.setdefault('cursor_factory', psycopg2.extras.RealDictCursor)
    return psycopg2.connect(**kwargs)
