# -*- coding: utf-8 -*-
"""
Provides simplified abstractions to create CRUD queries

- :class:`~jukoro.pg.query.QueryViewBuilder` - query builder expected to work
  with "live" database data (see :mod:`jukoro.pg.storage` for storage
  mechanics)
- :class:`~jukoro.pg.query.QueryBuilderDescr` - Python descriptor to provide
  access from :class:`Entity <jukoro.pg.entity.AbstractEntity>` to
  :class:`QueryBuilder <jukoro.pg.query.QueryViewBuilder>`

Roadmap:

- way to work with historical data in read only mode
- way to namespace fields within query
- way to nest queries

Example
-------

Describe :class:`Entity <jukoro.pg.entity.AbstractEntity>`:

.. code-block:: python

    # -*- coding: utf-8 -*-
    # file: project/entities.py

    from jukoro import arrow
    from jukoro import pg


    class User(pg.AbstractEntity):
        db_table = 'ju_user'

        username = pg.Attr(title='Username',
                        db_index=True, db_not_null=True, minlen=4)
        email = pg.Attr(title='Email',
                        db_index=True, db_not_null=True, minlen=6)
        password = pg.Attr(title='Password', db_not_null=True)
        logged_in = pg.Attr(title='Logged in',
                            db_index=True,
                            value_type=arrow.JuArrow,
                            db_not_null=True)

Usage example:

.. code-block:: ipythonconsole

    In [1]: from project.entities import User

    In [2]: User.db_table
    Out[2]: <jukoro.pg.storage.DBTableName at 0x7f975037c490>

    In [3]: User.db_table.name
    Out[3]: 'ju_user'

    In [4]: User.db_view
    Out[4]: <jukoro.pg.storage.DBViewName at 0x7f975037c4d0>

    In [5]: User.db_view.name
    Out[5]: 'ju_user__live'

    In [6]: User.qbuilder
    Out[6]: <jukoro.pg.query.QueryViewBuilder at 0x7f974ac0bf90>

    In [7]: User.qbuilder.by_id(11, 12, 13)
    Out[7]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE "entity_id" IN %s ORDER BY "entity_id" ASC;',
    ((11, 12, 13),))

    In [8]: User.qbuilder.select({'username': 'ysegorov'})
    Out[8]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE ("doc" @> %s);',
    [{'username': 'ysegorov'}])

    In [9]: User.qbuilder.select({'username': 'ysegorov'}, {'username': 'jmsmith'})
    Out[9]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE ("doc" @> %s) OR ("doc" @> %s);',
    [{'username': 'ysegorov'}, {'username': 'jmsmith'}])

    In [10]: User.qbuilder.select({'username': 'ysegorov'}, {'username': 'jmsmith'}, order_by='logged_in')
    Out[10]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE ("doc" @> %s) OR ("doc" @> %s) ORDER BY ("doc"->>\'logged_in\')::BIGINT ASC;',
    [{'username': 'ysegorov'}, {'username': 'jmsmith'}])

    In [11]: User.qbuilder.select({'username': 'ysegorov'}, {'username': 'jmsmith'}, order_by=(('logged_in', 'DESC'),))
    Out[11]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE ("doc" @> %s) OR ("doc" @> %s) ORDER BY ("doc"->>\'logged_in\')::BIGINT DESC;',
    [{'username': 'ysegorov'}, {'username': 'jmsmith'}])

    In [12]: User.qbuilder.select(limit=40, offset=100)
    Out[12]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" LIMIT %s  OFFSET %s;',
    [40, 100])

    In [13]: User.qbuilder.select(order_by=(('logged_in', 'DESC'),), limit=40, offset=100)
    Out[13]:
    ('SELECT "entity_id","doc" FROM "ju_user__live" ORDER BY ("doc"->>\'logged_in\')::BIGINT DESC LIMIT %s  OFFSET %s;',
    [40, 100])


Generated query and parameters can be sent to
:meth:`PgTransaction.execute <jukoro.pg.db.PgTransaction.execute>`
method to perform query.

"""


# TODO become really query builder

class QueryViewBuilder(object):
    """
    Query builder expected to create queries for "live" data in database, ie.
    data available or data to be stored from/to ``*__live`` view
    (see :mod:`jukoro.pg.storage` for reference).

    :param db_target:       database view name to create queries for
    :param klass:           :class:`Entity <jukoro.pg.entity.AbstractEntity>`
                            needed to work with ``Entity's``
                            :class:`attributes <jukoro.pg.attrs.Attr>`
                            for proper casts in queries

    """

    def __init__(self, db_target, klass):
        self._target = db_target
        self._klass = klass

    @property
    def fields(self):
        """
        Returns fields of interest to work with in database view

        :rtype:     string

        """
        return '"%s"' % '","'.join(['entity_id', 'doc'])

    def by_id(self, *ids):
        """
        Creates query to select row/rows from database view by
        entity_id/multiple ids

        Respects number of ``ids`` to create query using ``IN`` operator
        or ``=``

        :param ids:     list of ``ids`` to select rows by
        :returns:       query and query parameters
        :rtype:         ``tuple`` in form ``(str, tuple)``

        """
        # TODO chunks
        if not ids or not all(ids):
            raise ValueError(
                'at least one "entity_id" must be defined to get instance')
        target = self._target
        op = '=' if len(ids) == 1 else 'IN'
        params = ids if len(ids) == 1 else (ids, )
        where = 'WHERE "entity_id" {op} %s'.format(op=op)
        q = 'SELECT {fields} FROM "{target}" {where} ORDER BY "entity_id" ASC;'
        q = q.format(target=target, fields=self.fields, where=where)
        return (q, params)

    def create(self, *entities):
        """
        Creates query to create rows in database view returning newly created
        rows fields

        :param entities:    list of instances of
                            :class:`Entity <jukoro.pg.entity.AbstractEntity>`
                            (or of any other type having accessible and
                            jsonable ``.doc`` attribute)
        :returns:           query and query parameters
        :rtype:             ``tuple`` in form ``(str, list)``

        """
        # TODO chunks
        target = self._target
        placeholders = ','.join(['(%s)'] * len(entities))
        params = [x.doc for x in entities]
        q = 'INSERT INTO "{target}" ("doc") VALUES {placeholders} ' \
            'RETURNING {fields};'
        q = q.format(
            target=target, fields=self.fields, placeholders=placeholders)
        return (q, params)

    def update(self, *entities):
        """
        Creates query to update row/rows in database view returning updated
        fields

        :param entities:    list of instances of
                            :class:`Entity <jukoro.pg.entity.AbstractEntity>`
                            (or of any other type having accessible and
                            jsonable ``.entity_id`` and ``.doc`` attributes)
        :returns:           query and query parameters
        :rtype:             ``tuple`` in form ``(str, tuple)``

        **NB.** query will always update ``doc`` field meaning full "rewrite"
        of previous value (term "rewrite" in underlying data storage model
        means previous value will be closed/kept in database storing entity's
        end-of-live timestamp and new value will be created storing entity's
        start-of-live timestamp)

        """
        # TODO chunks
        target = self._target
        placeholders, params = [], []
        q = 'UPDATE "{target}" AS t SET "doc" = (v."doc")::jsonb ' \
            'FROM (VALUES {placeholders} ) AS v("entity_id", "doc") ' \
            'WHERE v."entity_id" = t."entity_id" ' \
            'RETURNING t."entity_id", t."doc";'
        for entity in entities:
            placeholders.append('(%s, %s)')
            params.extend([entity.entity_id, entity.doc])
        q = q.format(target=target, placeholders=','.join(placeholders))
        return (q, params)

    def delete(self, *entities):
        """
        Creates query to delete row/rows from database view

        Respects number of ``entities`` to create query using ``IN`` operator
        or ``=``

        :param entities:    list of instances of
                            :class:`Entity <jukoro.pg.entity.AbstractEntity>`
                            (or of any other type having accessible and
                            jsonable ``.entity_id`` attribute)
        :returns:           query and query parameters
        :rtype:             ``tuple`` in form ``(str, tuple)``
        :raises ValueError: if ``not entities or
                            not all(x.entity_id in entities)``

        """
        # TODO chunks
        entities = filter(None, entities)
        if not entities or not all(x.entity_id for x in entities):
            raise ValueError(
                'All entities to delete must have "entity_id" defined')
        target = self._target
        op = '=' if len(entities) == 1 else 'IN'
        params = tuple(x.entity_id for x in entities)
        if len(entities) > 1:
            # it must be only one parameter
            params = (params, )
        where = 'WHERE "entity_id" {op} %s'.format(op=op)
        q = 'DELETE FROM "{target}" {where};'
        q = q.format(target=target, where=where)
        return (q, params)

    def select(self, *conds, **kwargs):
        """
        Creates query to select rows from database view

        :param conds:               list of conditions to select rows by
                                    (can be empty,
                                    see below for supported formats)
        :param kwargs['order_by']:  rules to order data by
                                    (see below for supported formats)
        :param kwargs['limit']:     limit number of returned rows to
        :param kwargs['offset']:    offset returned rows by

        Let's assume ``cond`` is a single condition and::

            conds = [cond1, [cond2,] [cond3,] [] ]

        Specifying multiple conditions will transform them to ``OR`` ed
        conditions in a query like::

            ((cond1) OR (cond2) OR (cond3))

        Supported single condition ``cond`` formats:

        - dictionary::

            {'attr1': 12, 'attr2': 's2'}

          where dictionary is a form of ``AND`` conditions with ``=`` test
          operator like::

              ("attr1" = 12 AND "attr2" = 's2')

          and is suitable for ``jsonb`` **contains** ``@>`` operator

        - list of triplets::

            [t1, t2, ]

        where each triplet is a standalone condition in a form::

            (attribute, operation, value)

        and supported operations are:

            * **eq** - equality test using ``=`` in sql
            * **ne** - non-equality test using ``!=`` in sql
            * **in** - inclusing test using ``IN`` in sql
            * **lt** - less than test using ``<`` in sql
            * **lte** - less than or equal test using ``<=`` in sql
            * **gt** - greater than test using ``>`` in sql
            * **gte** - greater than or equal test using ``>=`` in sql

        and triplets are ``AND`` ed in a query like::

            (t1 AND t2)

        Conditions examples:

        .. code-block:: ipythonconsole

            In [1]: from project.entities import User

            In [2]: qb = User.qbuilder

            In [3]: qb.select({'first_name': 'Yuri', 'last_name': 'Egorov'}, {'first_name': 'Nick', 'last_name': 'Lomov'})
            Out[3]:
            ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE ("doc" @> %s) OR ("doc" @> %s);',
            [{'first_name': 'Yuri', 'last_name': 'Egorov'},
            {'first_name': 'Nick', 'last_name': 'Lomov'}])

            In [4]: qb.select((('username', 'in', ['ysegorov', 'nvlomov']),), order_by='username')
            Out[4]:
            ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE (("doc"->>\'username\')::TEXT IN %s) ORDER BY ("doc"->>\'username\')::TEXT ASC;',
            [['ysegorov', 'nvlomov']])

            In [7]: qb.select((('username', 'in', ['ysegorov', 'nvlomov']),), (('email', 'ne', 'root@example.com'), ),  order_by='username')
            Out[7]:
            ('SELECT "entity_id","doc" FROM "ju_user__live" WHERE (("doc"->>\'username\')::TEXT IN %s) OR (("doc"->>\'email\')::TEXT != %s) ORDER BY ("doc"->>\'username\')::TEXT ASC;',
            [['ysegorov', 'nvlomov'], 'root@example.com'])

        Supported ``order_by`` formats:

        - attribute name::

            order_by='attr1'  ## ascending
            order_by='-attr1' ## descending

        - list of attributes names::

            ## 'attr1' ascending and 'attr2' descending
            order_by=['attr1', '-attr2']

        """
        target, fields = self._target, self.fields
        q = 'SELECT {fields} FROM "{target}"' \
            '{where}{order_by}{limit}{offset}'

        where, params = _transform_conditions(self._klass, *conds)
        order_by = _transform_order_by(self._klass,
                                       kwargs.pop('order_by', []))
        limit, offset = '', ''

        if 'limit' in kwargs:
            limit = ' LIMIT %s '
            params.append(kwargs['limit'])
        if 'offset' in kwargs:
            offset = ' OFFSET %s '
            params.append(kwargs['offset'])

        q = q.format(target=target, fields=fields, where=where,
                     order_by=order_by, limit=limit, offset=offset)
        q = q.strip() + ';'
        return (q, params)


class QueryBuilderDescr(object):
    """
    Python descriptor acting as a factory for ``QueryBuilder`` instances
    accessed using :attr:`Entity.qbuilder <jukoro.pg.entity.AbstractEntity>`
    attribute

    It is expected ``qbuilder`` attribute to be accessed from
    :class:`Entity <jukoro.pg.entity.AbstractEntity>` having ``db_table``
    attribute defined

    :param db_target_name:  database table or view name to create queries for
    :param query_builder:   type of ``QueryBuilder`` to create while accessing
                            :attr:`~jukoro.pg.entity.AbstractEntity.qbuilder`
                            attribute (defaults to
                            :class:`~jukoro.pg.query.QueryViewBuilder`)
    :raises AttributeError: in case ``qbuilder`` attribute was accessed from
                            Entity instance (must be accessed from class)

    """

    def __init__(self, db_target_name, query_builder=None):
        self._target_name = db_target_name
        self._qb = query_builder or QueryViewBuilder

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError(
                'This is a "{}" class attribute, not an instance one'.format(
                    owner.__name__))
        return self._qb(self._target_name, owner)

    def __set__(self, instance, value):
        raise AttributeError


OPS = {
    'eq': '=',
    'ne': '!=',
    'in': 'IN',
    'lt': '<',
    'gt': '>',
    'lte': '<=',
    'gte': '>=',
}


def _transform_op(op):
    if op not in OPS:
        raise ValueError('Unknown operator "{}"'.format(op))
    return OPS.get(op)


def _transform_conditions(klass, *conditions):
    # examples for conditions and transformed sql results:
    #   - simple AND within dict and OR between dicts
    #       (dict1, dict2...) => ("doc" @> dict1) OR ("doc" @> dict2)
    #
    #   - complex AND
    #       (((attr1, 'lte', val1), (attr2, 'ne', val2))) becomes
    #           (("doc"->>attr1)::INT <= val1) AND
    #               (("doc"->>attr2)::INT != val2)
    #
    #   - complex OR
    #       (((attr1, 'lte', val1), ), ((attr2, 'ne', val2), )) becomes
    #           (("doc"->>attr1)::INT <= val1) OR
    #               (("doc"->>attr2)::INT != val2)
    #
    #   - complex OR with AND
    #       (((attr1, 'lte', val1), (attr1, 'ne', val1_2)),
    #        ((attr2, 'ne', val2), (attr2, 'gt', val2_2))) becomes
    #           (("doc"->>attr1)::INT <= val1
    #               AND
    #            ("doc"->>attr1)::INT != val1_2)
    #           OR
    #           (("doc"->>attr2)::INT != val2
    #               AND
    #            ("doc"->>attr2)::INT > val2_2)

    if not conditions:
        return '', []
    placeholders, params = [], []
    for cond in conditions:
        # cond must be a dict for simple AND
        #   or tuple of tuples with condition triplets, i.e.
        #   (triplet1, triplet2...)
        #   where triplet is a tuple in form (attr, op, value)

        # TODO own exception
        if isinstance(cond, dict):
            placeholders.append('("doc" @> %s)')
            params.append(cond)
        elif isinstance(cond, (list, tuple)):
            block = []
            for attr, op, value in cond:
                op = _transform_op(op)
                cast = (getattr(klass, attr)).db_cast
                block.append(
                    '("doc"->>\'{attr}\')::{cast} {op} %s'.format(
                        attr=attr, cast=cast, op=op))
                params.append(value)
            placeholders.append('(%s)' % ' AND '.join(block))
    placeholders = ' WHERE %s' % ' OR '.join(placeholders)
    return placeholders, params


def _transform_order_by(klass, fields):
    """
    :param klass:   AbstractEntity-based class
    :param fields:  attribute or a list of attributes to sort by,
                    attribute name can be prefixed with hyphen to indicate
                    descending sorting

    Examples::

        'first_name'
        ('first_name', 'last_name')
        ('-created', 'accessed')

    """
    spec = '("doc"->>\'{attr}\')::{cast} {direction}'
    if isinstance(fields, basestring):
        fields = (fields, )
    res = []
    fields = (x.strip() for x in fields if x.strip())
    for attr in fields:
        direction, cast = 'ASC', 'TEXT'
        if attr[0] == '-':
            direction = 'DESC'
            attr = attr[1:]
        if hasattr(klass, attr):
            cast = (getattr(klass, attr)).db_cast
        res.append(spec.format(attr=attr, cast=cast, direction=direction))
    return ' ORDER BY %s' % ', '.join(res) if res else ''
