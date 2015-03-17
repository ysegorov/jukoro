# -*- coding: utf-8 -*-

import logging

from itertools import count


logger = logging.getLogger(__name__)


_counter = count()


class Attr(object):
    __slots__ = ('db_not_null', 'db_index', 'db_type',
                 'title', 'maxlen', 'minlen', 'wrapper', '_idx')

    def __init__(self, **kwargs):
        self.db_index = kwargs.pop('db_index', False)
        self.db_type = kwargs.pop('db_type', False)
        self.db_not_null = kwargs.pop('db_not_null', True)
        self.title = kwargs.pop('title', 'undefined')
        self.maxlen = kwargs.pop('maxlen', 0)
        self.minlen = kwargs.pop('minlen', 0)
        self.wrapper = kwargs.pop('wrapper', None)

        self._idx = next(_counter)

    @property
    def is_int(self):
        dbt = (self.db_type or 'undef').lower()
        return dbt in ['int', 'integer', 'bigint']

    @property
    def is_text(self):
        return self.db_type == 'text'

    @property
    def idx(self):
        return self._idx

    def db_cast(self):
        if self.is_int:
            return 'INT'
        return 'TEXT'

    def __cmp__(self, other):
        if isinstance(other, type(self)):
            return self.idx - other.idx
        raise RuntimeError(
            'Unable to compare types "{}" and "{}"'.format(type(self),
                                                           type(other)))


class AttrDescr(object):
    __slots__ = ('_slug', '_attr')

    def __init__(self, slug, attr):
        assert slug is not None
        assert isinstance(attr, Attr)
        self._slug = slug
        self._attr = attr

    @property
    def slug(self):
        return self._slug

    def __get__(self, instance, owner):
        if instance is None:
            return self
        val = instance.doc.get(self.slug)
        if callable(self._attr.wrapper):
            val = self._attr.wrapper(val)
        return val

    def __set__(self, instance, value):
        instance.doc[self.slug] = value

    def __delete__(self, instance):
        instance.doc.pop(self.slug, None)

    def __getattr__(self, name):
        return getattr(self._attr, name)

    def __cmp__(self, other):
        return self._attr.__cmp__(other._attr)


class AttrsDescr(object):
    __slots__ = ('_slugs', '_sorted')

    def __init__(self, *slugs):
        self._slugs = slugs
        self._sorted = False

    def __get__(self, instance, owner):
        target = instance or owner
        if not self._sorted:
            self._slugs = sorted(self._slugs,
                                 key=lambda x: getattr(target, x).idx)
            self._sorted = True
        for slug in self._slugs:
            yield getattr(target, slug)

    def __set__(self, instance):
        raise AttributeError
