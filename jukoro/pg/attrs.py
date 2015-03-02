# -*- coding: utf-8 -*-


class Attr(object):
    __slots__ = ('db_not_null', 'db_index', 'db_type',
                 'title', 'maxlen', 'minlen', 'wrapper')

    def __init__(self, **kwargs):
        self.db_index = kwargs.pop('db_index', False)
        self.db_type = kwargs.pop('db_type', False)
        self.db_not_null = kwargs.pop('db_not_null', True)
        self.title = kwargs.pop('title', 'undefined')
        self.maxlen = kwargs.pop('maxlen', 0)
        self.minlen = kwargs.pop('minlen', 0)
        self.wrapper = kwargs.pop('wrapper', None)

    @property
    def is_int(self):
        dbt = (self.db_type or 'undef').lower()
        return dbt in ['int', 'integer', 'bigint']

    @property
    def is_text(self):
        return self.db_type == 'text'


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
        val = instance.data.get(self.slug)
        if callable(self._attr.wrapper):
            val = self._attr.wrapper(val)
        return val

    def __set__(self, instance, value):
        instance.data[self.slug] = value

    def __delete__(self, instance):
        instance.data.pop(self.slug, None)

    def __getattr__(self, name):
        return getattr(self._attr, name)


class AttrsDescr(object):
    __slots__ = ('_slugs', )

    def __init__(self, *slugs):
        self._slugs = slugs

    def __get__(self, instance, owner):
        target = instance or owner
        for slug in self._slugs:
            yield getattr(target, slug)

    def __set__(self, instance):
        raise AttributeError
