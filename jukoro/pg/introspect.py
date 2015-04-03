# -*- coding: utf-8 -*-
"""
Provides a way to introspect database and returns its current state for:

- schemas
- sequences
- tables
- views
- triggers
- indices
- constraints

Usage example:

.. code-block:: ipythonconsole

    In [1]: uri='postgresql://localhost/jukoro_test.ju_20150403102042'

    In [2]: from jukoro.pg import inspect

    In [3]: schema, state = inspect(uri)

    In [4]: schema
    Out[4]: 'ju_20150403102042'

    In [5]: state
    Out[5]: <jukoro.pg.introspect.State at 0x7f8391a16e90>

    In [6]: state.tables
    Out[6]: <jukoro.pg.introspect.StateValues at 0x7f8391a16fd0>

    In [7]: list(state.tables)
    Out[7]: [u'test_pg', u'entity']

    In [8]: list(state.views)
    Out[8]: [u'test_pg__live']

    In [9]: list(state.triggers)
    Out[9]:
    [u'ju_before__test_pg__live__update',
    u'ju_before__test_pg__live__delete',
    u'ju_before__test_pg__live__insert']

    In [10]: list(state.indices)
    Out[10]:
    [u'ju_idx__test_pg__attr7_entity_start_entity_end',
    u'ju_idx__test_pg__doc',
    u'ju_idx__test_pg__attr1_entity_start_entity_end',
    u'ju_idx__test_pg__entity_id',
    u'ju_idx__test_pg__attr2_entity_start_entity_end',
    u'entity_pkey',
    u'test_pg_pkey',
    u'ju_idx__test_pg__attr4_entity_start_entity_end']

    In [11]: list(state.constraints)
    Out[11]:
    [u'243905_243922_5_not_null',
    u'243905_243908_1_not_null',
    u'243905_243922_1_not_null',
    u'ju_validate__test_pg__attr4',
    u'ju_validate__test_pg__attr5',
    u'243905_243908_5_not_null',
    u'ju_validate__test_pg__attr7',
    u'ju_validate__test_pg__attr1',
    u'ju_validate__test_pg__attr2',
    u'ju_validate__test_pg__attr3',
    u'243905_243908_2_not_null',
    u'entity_pkey',
    u'243905_243922_2_not_null',
    u'test_pg_pkey']

    In [12]: 'test_pg' in state.tables
    Out[12]: True

"""

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

# mapping for names and queries
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
    """
    Introspects database using predefined sql queries

    Can act as context manager

    :param conn:    instance of :class:`~jukoro.pg.db.PgConnection`

    See :func:`~jukoro.pg.introspect.inspect` for example usage

    """

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
    """
    Acts as a container for current database state for specific type
    (one of schemas/sequences/tables/views/triggers/indices/constraints)

    :param values:  list of values retrieved from database using
                    :class:`~jukoro.pg.introspect.PgIntrospect`

    """

    def __init__(self, values):
        self._values = set(values or [])

    def __contains__(self, k):
        return k in self._values

    def __iter__(self):
        for item in self._values:
            yield item

    def pop(self, k):
        """
        Removes value from current state

        :param k:   value to remove

        Primary usage for this is to clear out values expected to be present
        in database and keep values expected to be deleted from database

        """
        self._values.discard(k)

    def clear(self):
        """
        Clears all values from current state

        """
        self._values.clear()


class State(object):
    """
    Acts as a container for current database state for all types

    :param pairs:   iterator or a list of pairs
                    (key, :class:`~jukoro.pg.introspect.StateValues` instance)

    """

    def __init__(self, pairs):
        self._pairs = OrderedDict(pairs)

    def __getattr__(self, name):
        return self._pairs.get(name)


def inspect(uri):
    """
    Inspects current database state

    :param uri:     connection string
    :returns:       tuple of (schema, state)

    """

    from jukoro.pg import PgConnection

    conn = PgConnection(uri)
    schema = conn.schema

    with PgIntrospect(conn) as inspector:
        state = State(
            (k, StateValues(getattr(inspector, k))) for k in INTROMAP)

    conn.close()
    return schema, state
