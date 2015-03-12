# -*- coding: utf-8 -*-


class QueryEntity(object):

    def __init__(self, entity, db_target):
        self._entity = entity
        self._target = db_target

    def save(self):
        entity, target = self._entity, self._target
        fields = entity.db_fields()
        eid, doc = entity.db_values
        q = 'INSERT INTO "{target}" ("{fields}") VALUES ({placeholders}) ' \
            'RETURNING "{fields}";'
        fields = '","'.join(fields)
        placeholders = '%s, %s' if eid else 'DEFAULT, %s'
        params = (eid, doc) if eid else (doc,)
        q = q.format(target=target, fields=fields, placeholders=placeholders)
        return (q, params)

    def get(self):
        entity = self._entity
        return type(entity).sql.by_id(entity.id)


class QueryEntityClass(object):

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


class QueryDescr(object):

    def __init__(self, db_target, db_fields=None):
        self._target = db_target

    def __get__(self, instance, owner):
        db_target = getattr(owner, self._target, None)
        if not db_target:
            raise AttributeError(
                'Class "{}" must have "{}" attribute defined'.format(
                    owner.__name__, self._target))
        if instance is None:
            return QueryEntityClass(owner, db_target)
        return QueryEntity(instance, db_target)

    def __set__(self, instance, value):
        raise AttributeError
