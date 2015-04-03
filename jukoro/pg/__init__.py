# -*- coding: utf-8 -*-
"""
Experimental package to work with PostgreSQL
(*expected PostgreSQL version >= 9.4*)

Features:

- separation of concerns to work with PostgreSQL (explicit ways to work with
  pool/connection/transaction/results,
  to describe entities (models in terms of Django's ORM),
  to introspect database,
  to create customized sql queries)
- ``psycopg2`` named cursors support out-of-the-box
- historical data (no delete operation in database, works transparently using
  database triggers)
- way to work using explicitly defined PostgreSQL schema in connection uri
- global entities numeration within single database
- table per entity approach using unified simple tables structure
  (all tables inherit from master ``entity`` table)
- ``jsonb`` field to store entities data
- support for indices and constraints per entity attribute
- auto ``created``, ``updated``, ``deleted`` timestamps per entity
- way to prepare sql queries to synchronize database state and entities
  (models) declarations (queries to create missed tables/indices/constraints
  and to drop orphaned tables/indices/constraints)

Modules:

- :mod:`jukoro.pg.attrs` - abstractions to describe entity attributes
- :mod:`jukoro.pg.db` - abstractions to work with
  pool/connection/transaction/result
- :mod:`jukoro.pg.entity` - abstractions to describe entity
- :mod:`jukoro.pg.exceptions` - package exceptions
- :mod:`jukoro.pg.introspect` - abstractions to introspect database
- :mod:`jukoro.pg.query` - abstraction to prepare entities-related sql
  statements to execute
- :mod:`jukoro.pg.storage` - abstractions to prepare sql statements
  to synchronize database state with entities declarartions
- :mod:`jukoro.pg.utils` - package utils

"""
from __future__ import absolute_import

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from psycopg2 import (
    Error, DataError, DatabaseError, ProgrammingError, IntegrityError,
    InterfaceError, InternalError, NotSupportedError, OperationalError)

from jukoro import json

from jukoro.pg.attrs import Attr, AttrDescr
from jukoro.pg.db import PgDbPool, PgConnection, PgTransaction, PgResult
from jukoro.pg.entity import AbstractEntity, AbstractUser
from jukoro.pg.exceptions import (
    PgError, BadUri, AlreadyRegistered, PoolClosed,
    ConnectionClosed, CursorClosed, DoesNotExist)
from jukoro.pg.introspect import inspect
from jukoro.pg.query import QueryViewBuilder, QueryBuilderDescr
from jukoro.pg.utils import pg_uri_to_kwargs


class PgJsonEncoder(json.JSONEncoder):
    """
    PostgreSQL-focused json encoder

    Custom object can be encoded using simple convention - object can have
    ``db_val`` property or method to get it's json-compatible value

    """
    json_attr = 'db_val'


class PgJson(psycopg2.extras.Json):
    """
    Custom ``psycopg2.extras.Json`` adapter using
    :class:`~jukoro.pg.PgJsonEncoder` to encode values

    """
    def dumps(self, obj):
        return json.dumps(obj, cls=PgJsonEncoder)


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

psycopg2.extensions.register_adapter(dict, PgJson)
psycopg2.extras.register_default_json(globally=True, loads=json.loads)
psycopg2.extras.register_default_jsonb(globally=True, loads=json.loads)
psycopg2.extras.register_uuid()
