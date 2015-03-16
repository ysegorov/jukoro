# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import logging
import os
import random
import warnings

import psycopg2

from jukoro import pg
from jukoro.pg import storage as pg_storage

from .attrs import *
from .db import *
from .entity import *
from .introspect import *
from .query import *
from .storage import *
from .utils import *


logger = logging.getLogger(__name__)


URI = os.environ.get('PG_URI', 'postgresql://localhost/jukoro_test')
TEST_URI = 'postgresql://test:test@localhost:5467/jukoro_test'
BAD_URI = 'postgresq://localhost:5432/jukoro_test.a1'

IS_ONLINE = False
SCHEMA = 'public'
FIRST_ID = LAST_ID = None
MOCK_COUNT = 1000

SQL_TEARDOWN = """
DROP SCHEMA {schema} CASCADE;
"""


def setUp():
    global IS_ONLINE
    global URI
    global SCHEMA
    global FIRST_ID, LAST_ID
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
        if not URI.endswith(kwargs['schema']):
            URI += '.%s' % kwargs['schema']
        SCHEMA = kwargs['schema']
        # create test schema and tables
        sql_create, sql_drop = pg_storage.syncdb(URI)
        # logger.debug(sql_create)
        assert not sql_drop.strip()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_create)

        query, params = mock_data(MOCK_COUNT)
        cursor.execute(query, params)

        cursor.execute('SELECT currval(\'public.global_entity_id_seq\');')
        LAST_ID = cursor.fetchone()[0]
        FIRST_ID = LAST_ID - MOCK_COUNT + 1

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


def mock_data(cnt):
    random.seed()

    _letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    def _d(attr1, attr3):
        return {
            'attr1': random.choice(attr1),
            'attr2': 'mistery',
            'attr3': random.choice(attr3),
            'attr4': random.randint(1, 100),
            'attr5': random.randint(1000, 100000),
        }

    attr1 = [''.join(random.sample(_letters, 5)) for __ in xrange(100)]
    attr3 = [''.join(random.sample(_letters, 7)) for __ in xrange(200)]

    k = ','.join('(%s)' for ___ in xrange(cnt))
    v = tuple(_d(attr1, attr3) for ___ in xrange(cnt))

    return 'INSERT INTO test_pg ("doc") VALUES {};'.format(k), v
