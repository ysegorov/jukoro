# -*- coding: utf-8 -*-


# TODO become really query builder

class QueryViewBuilder(object):

    def __init__(self, db_target, klass):
        self._target = db_target
        self._klass = klass

    @property
    def fields(self):
        return '"%s"' % '","'.join(['entity_id', 'doc'])

    def by_id(self, *ids):
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

    def delete(self, *ids):
        # TODO chunks
        if not ids or not all(ids):
            raise ValueError(
                'at least one "entity_id" must be defined for delete')
        target = self._target
        op = '=' if len(ids) == 1 else 'IN'
        params = ids if len(ids) == 1 else (ids, )
        where = 'WHERE "entity_id" {op} %s'.format(op=op)
        q = 'DELETE FROM "{target}" {where};'
        q = q.format(target=target, where=where)
        return (q, params)

    def select(self, *conds, **kwargs):
        target, fields = self._target, self.fields
        q = 'SELECT {fields} FROM "{target}"' \
            '{where}{order_by}{limit}{offset}'

        where, params = _transform_conditions(self._klass, *conds)
        order_by = _transform_order_by(
            self._klass, kwargs.pop('order_by', None))
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


def _transform_op(op):
    ops = {
        'eq': '=',
        'ne': '!=',
        'in': 'IN',
        'lt': '<',
        'gt': '>',
        'lte': '<=',
        'gte': '>=',
    }
    if op not in ops:
        raise ValueError('Unknown operator "{}"'.format(op))
    return ops.get(op)


def _transform_conditions(klass, *conditions):
    # examples for conditions and transformed sql results:
    #   - simple AND
    #       (dict1, dict2...) => ("doc" @> dict1) AND ("doc" @> dict2)
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
                cast = (getattr(klass, attr)).db_cast()
                block.append(
                    '("doc"->>\'{attr}\')::{cast} {op} %s'.format(
                        attr=attr, cast=cast, op=op))
                params.append(value)
            placeholders.append('(%s)' % ' AND '.join(block))
    placeholders = ' WHERE %s' % ' OR '.join(placeholders)
    return placeholders, params


def _transform_order_by(klass, fields):
    spec = '("doc"->>\'{attr}\')::{cast} {direction}'
    if isinstance(fields, basestring):
        cast = (getattr(klass, fields)).db_cast()
        return ' ORDER BY %s ' % spec.format(attr=fields,
                                             cast=cast, direction='ASC')
    elif isinstance(fields, (list, tuple)):
        res = []
        for f in fields:
            if isinstance(f, (list, tuple)):
                attr, direction = f
            else:
                attr, direction = f, 'ASC'
            cast = (getattr(klass, attr)).db_cast()
            res.append(spec.format(attr=attr, cast=cast, direction=direction))
        return 'ORDER BY %s' % ', '.join(res)
    return ''
