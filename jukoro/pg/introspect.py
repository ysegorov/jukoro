# -*- coding: utf-8 -*-

from collections import OrderedDict


_TABLES = """
SELECT
    table_name as qname
FROM
    information_schema.tables
WHERE
    table_type = 'BASE TABLE'
AND
    table_schema = %(schema)s;
"""

_VIEWS = """
SELECT
    table_name as qname
FROM
    information_schema.views
WHERE
    table_schema = %(schema)s;
"""

_TRIGGERS = """
SELECT
    trigger_name as qname
FROM
    information_schema.triggers
WHERE
    trigger_schema = %(schema)s;
"""

_CONSTRAINTS = """
SELECT
    constraint_name as qname
FROM
    information_schema.table_constraints
WHERE
    constraint_schema = %(schema)s;
"""

_SEQUENCES = """
SELECT
    sequence_name as qname
FROM
    information_schema.sequences
WHERE
    sequence_schema = %(schema)s;
"""

_SCHEMAS = """
SELECT
    schema_name as qname
FROM
    information_schema.schemata
WHERE
    schema_name = %(schema)s;
"""

_INDICES = """
SELECT
    indexname as qname
FROM
    pg_indexes
WHERE
    schemaname = %(schema)s;
"""

INTROMAP = OrderedDict(
    schemas=_SCHEMAS,
    sequences=_SEQUENCES,
    tables=_TABLES,
    views=_VIEWS,
    triggers=_TRIGGERS,
    indices=_INDICES,
    constraints=_CONSTRAINTS,
)


class PgIntrospect(object):

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.conn = None

    def _get(self, q, params):
        with self.conn.transaction() as _conn:
            resp = _conn.execute(q, params).all()
            resp = [r['qname'] for r in resp]
        return resp

    def __getattr__(self, name):
        if name not in INTROMAP:
            raise AttributeError('Unknown attribute "%s"' % name)
        return self._get(INTROMAP[name], {'schema': self.conn.schema})


class StateValues(object):

    def __init__(self, values):
        self._values = set(values or [])

    def __contains__(self, k):
        return k in self._values

    def pop(self, k):
        self._values.discard(k)

    def clear(self):
        self._values.clear()


class State(object):

    def __init__(self, pairs):
        self._pairs = OrderedDict(pairs)

    def __getattr__(self, name):
        return self._pairs.get(name)


def inspect(uri):

    from jukoro.pg import PgConnection

    conn = PgConnection(uri)
    schema = conn.schema

    with PgIntrospect(conn) as inspector:
        state = State(
            (k, StateValues(getattr(inspector, k))) for k in INTROMAP)

    conn.close()
    return schema, state
