# -*- coding: utf-8 -*-


class QueryBuilder(object):

    def __init__(self, klass, db_target):
        self._klass = klass
        self._target = db_target

    def by_id(self, entity_id):
        if not entity_id:
            raise ValueError('"entity_id" must be defined to get instance')
        klass, target = self._klass, self._target
        fields = '","'.join(klass.db_fields())
        q = 'SELECT "{fields}" FROM "{target}" WHERE "entity_id" = %s;'
        q = q.format(target=target, fields=fields)
        return (q, (entity_id, ))

    def save(self, entity):
        target = self._target
        fields = entity.db_fields()
        entity_id, doc = entity.db_values
        q = 'INSERT INTO "{target}" ("{fields}") VALUES ({placeholders}) ' \
            'RETURNING "{fields}";'
        fields = '","'.join(fields)
        placeholders = '%s, %s' if entity_id else 'DEFAULT, %s'
        params = (entity_id, doc) if entity_id else (doc,)
        q = q.format(target=target, fields=fields, placeholders=placeholders)
        return (q, params)


class QueryBuilderDescr(object):

    def __init__(self, db_target_name, query_builder=None):
        self._target_name = db_target_name
        self._qb = query_builder or QueryBuilder

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError(
                'This is a "{}" class attribute, not an instance one'.format(
                    owner.__name__))
        return self._qb(owner, self._target_name)

    def __set__(self, instance, value):
        raise AttributeError
