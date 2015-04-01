# -*- coding: utf-8 -*-

import logging

from jukoro import json

from jukoro.pg.attrs import Attr, AttrDescr, AttrsDescr
from jukoro.pg.query import QueryBuilderDescr
from jukoro.pg import storage


logger = logging.getLogger(__name__)


def _slugs(base):
    if hasattr(base, 'attrs'):
        return [attr.slug for attr in base.attrs]
    return []


class EntityMeta(type):

    def __new__(mcs, name, bases, dct):
        dct.setdefault('__slots__', tuple())
        tn, attrs = dct.pop('db_table', None), []
        if 'attrs' in dct:
            raise AttributeError(
                '"attrs" attribute is reserved, take another one')

        cud = {'_created', '_updated', '_deleted'}
        if cud.intersection(dct.iterkeys()):
            raise AttributeError(
                '"_created", "_updated" and "_deleted" have been reserved, '
                'take another names')
        if tn:
            if tn == 'entity':
                raise AttributeError(
                    '"entity" table name is reserved, take another one')
            dct['db_table'] = storage.DBTableName(tn)
            dct['db_view'] = db_view = storage.DBViewName(tn)

            if 'qbuilder' not in dct:
                dct['qbuilder'] = QueryBuilderDescr(db_view.name)

        own_slugs = [k for (k, v) in dct.iteritems() if isinstance(v, Attr)]
        attrs.extend(own_slugs)
        [attrs.extend(x) for x in map(_slugs, bases)]

        dct.update((k, AttrDescr(k, dct[k])) for k in own_slugs)
        dct['attrs'] = AttrsDescr(*set(attrs))

        klass = super(EntityMeta, mcs).__new__(mcs, name, bases, dct)

        if tn is not None and not dct.get('skip_registry', False):
            storage.register(klass)
        return klass


class AbstractEntity(object):
    __metaclass__ = EntityMeta
    __slots__ = ('_entity_id', '_doc')

    qbuilder = None

    def __init__(self, entity_id=None, doc=None):
        self._entity_id = entity_id
        self._doc = doc or {}

    @property
    def entity_id(self):
        return self._entity_id

    @property
    def doc(self):
        return self._doc

    @property
    def created(self):
        return self._doc.get('_created')

    @property
    def updated(self):
        return self._doc.get('_updated')

    @property
    def deleted(self):
        # TODO (useless for now)
        return self._doc.get('_deleted')

    def update(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def db_val(self):
        return self.entity_id

    def json_val(self):
        return self.entity_id

    @classmethod
    def by_id(cls, cursor, entity_id):
        q, params = cls.qbuilder.by_id(entity_id)
        res = cursor.execute_and_get(q, params)
        return cls(**res)

    def save(self, cursor):
        klass = type(self)
        if self._entity_id is None:
            q, params = klass.qbuilder.create(self)
        else:
            q, params = klass.qbuilder.update(self)
        res = cursor.execute_and_get(q, params)
        logger.debug('res: %s', res)
        return klass(**res)

    def delete(self, cursor):
        klass = type(self)
        q, params = klass.qbuilder.delete(self)
        cursor.execute(q, params)

    def serialize(self):
        return json.dumps({'entity_id': self._entity_id, 'doc': self._doc})

    @classmethod
    def deserialize(cls, value):
        return cls(**json.loads(value))

    def __eq__(self, other):
        klass = type(self)
        if isinstance(other, klass):
            attrs = klass.attrs
            return self.entity_id == other.entity_id and all(
                getattr(self, x.slug) == getattr(other, x.slug) for x in attrs)
        raise RuntimeError(
            'Unable to compare types "{}" and "{}"'.format(klass,
                                                           type(other)))

    def __ne__(self, other):
        klass = type(self)
        if isinstance(other, klass):
            attrs = klass.attrs
            return self.entity_id != other.entity_id or any(
                getattr(self, x.slug) != getattr(other, x.slug) for x in attrs)
        raise RuntimeError(
            'Unable to compare types "{}" and "{}"'.format(klass,
                                                           type(other)))


class AbstractUser(AbstractEntity):
    username = Attr(title='Username',
                    db_index=True, db_not_null=True, minlen=4)
    email = Attr(title='Email',
                 db_index=True, db_not_null=True, minlen=6)
    password = Attr(title='Password', db_not_null=True)
