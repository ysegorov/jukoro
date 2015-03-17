# -*- coding: utf-8 -*-


# TODO become really query builder

class QueryViewBuilder(object):

    def __init__(self, db_target):
        self._target = db_target

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


class QueryBuilderDescr(object):

    def __init__(self, db_target_name, query_builder=None):
        self._target_name = db_target_name
        self._qb = query_builder or QueryViewBuilder

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError(
                'This is a "{}" class attribute, not an instance one'.format(
                    owner.__name__))
        return self._qb(self._target_name)

    def __set__(self, instance, value):
        raise AttributeError
