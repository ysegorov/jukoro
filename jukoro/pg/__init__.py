# -*- coding: utf-8 -*-

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
from jukoro.pg.entity import BaseEntity, BaseUser
from jukoro.pg.exceptions import (
    PgError, PgUriError, PgAlreadyRegisteredError, PgPoolClosedError,
    PgConnectionClosedError, PgCursorClosedError, PgDoesNotExistError)
from jukoro.pg.introspect import inspect
from jukoro.pg.query import QueryViewBuilder, QueryBuilderDescr
from jukoro.pg.utils import pg_uri_to_kwargs


class JukoroPgJson(psycopg2.extras.Json):
    def dumps(self, obj):
        return json.dumps(obj)


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

psycopg2.extensions.register_adapter(dict, JukoroPgJson)
psycopg2.extras.register_default_json(globally=True, loads=json.loads)
psycopg2.extras.register_default_jsonb(globally=True, loads=json.loads)
psycopg2.extras.register_uuid()
