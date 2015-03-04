# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import os
import warnings

import psycopg2

from jukoro import pg
from jukoro.pg import storage as pg_storage

from .attrs import *
from .db import *
from .introspect import *
from .storage import *
from .utils import *


URI = os.environ.get('PG_URI', 'postgresql://localhost/jukoro_test')
TEST_URI = 'postgresql://test:test@localhost:5467/jukoro_test'
BAD_URI = 'postgresq://localhost:5432/jukoro_test.a1'

IS_ONLINE = False
SCHEMA = 'public'

SQL_TEARDOWN = """
DROP SCHEMA {schema} CASCADE;
"""


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
        sql_create, sql_drop = pg_storage.syncdb(URI)
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
