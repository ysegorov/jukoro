# -*- coding: utf-8 -*-

from jukoro.structures import ObjectDict


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

INTROMAP = {
    'tables': _TABLES,
    'views': _VIEWS,
    'triggers': _TRIGGERS,
    'constraints': _CONSTRAINTS,
    'indices': _INDICES,
    'sequences': _SEQUENCES,
    'schemas': _SCHEMAS,
}


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
            resp = set(r['qname'] for r in resp)
        return resp

    def __getattr__(self, name):
        if name not in INTROMAP:
            raise AttributeError('Unknown attribute "%s"' % name)
        return self._get(INTROMAP[name], {'schema': self.conn.schema})


def inspect(uri):

    from jukoro.pg import PgConnection

    conn = PgConnection(uri)
    schema = conn.schema

    with PgIntrospect(conn) as inspector:
        state = ObjectDict((k, getattr(inspector, k)) for k in INTROMAP)

    conn.close()
    return schema, state
