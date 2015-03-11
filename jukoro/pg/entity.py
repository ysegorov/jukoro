# -*- coding: utf-8 -*-

import logging

from jukoro.pg.attrs import Attr, AttrDescr, AttrsDescr
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
        if tn:
            if tn == 'entity':
                raise AttributeError(
                    '"entity" table name is reserved, take another one')
            dct['db_table'] = storage.DBTableName(tn)
            dct['db_view'] = storage.DBViewName(tn)

        own_slugs = [k for (k, v) in dct.iteritems() if isinstance(v, Attr)]
        attrs.extend(own_slugs)
        [attrs.extend(x) for x in map(_slugs, bases)]

        dct.update((k, AttrDescr(k, dct[k])) for k in own_slugs)
        dct['attrs'] = AttrsDescr(*set(attrs))

        klass = super(EntityMeta, mcs).__new__(mcs, name, bases, dct)

        if tn is not None and not dct.get('skip_registry', False):
            storage.register(klass)
        return klass


class BaseEntity(object):
    __metaclass__ = EntityMeta
    __slots__ = ('_id', '_doc')

    def __init__(self, entity_id=None, doc=None):
        self._id = entity_id
        self._doc = doc or {}

    @property
    def id(self):
        return self._id

    @property
    def doc(self):
        return self._doc


class BaseUser(BaseEntity):
    username = Attr(title='Username',
                    db_index=True, db_type='text', db_not_null=True, minlen=4)
    email = Attr(title='Email',
                 db_index=True, db_type='text', db_not_null=True, minlen=6)
    password = Attr(title='Password', db_type='text', db_not_null=True)
