# -*- coding: utf-8 -*-
"""
Provides generic classes to work with
:class:`Entity <jukoro.pg.entity.AbstractEntity>` attributes

- :class:`Attr <jukoro.pg.attrs.Attr>` - generic attribute class
- :class:`AttrDescr <jukoro.pg.attrs.AttrDescr>` - Python descriptor to work
  with :class:`Attr <jukoro.pg.attrs.Attr>` connected to
  :class:`Entity <jukoro.pg.entity.AbstractEntity>`
- :class:`AttrsDescr <jukoro.pg.attrs.AttrsDescr>` - Python descriptor to
  iterate over attributes connected to
  :class:`Entity <jukoro.pg.entity.AbstractEntity>`

Usage example::

    # -*- coding: utf-8 -*-
    # file: project/entities.py

    from jukoro import arrow
    from jukoro import pg


    class User(pg.AbstractEntity):
        username = pg.Attr(title='Username',
                           db_index=True, db_not_null=True, minlen=4)
        email = pg.Attr(title='Email',
                        db_index=True, db_not_null=True, minlen=6)
        password = pg.Attr(title='Password', db_not_null=True)
        logged_in = pg.Attr(title='Logged in',
                            db_index=True,
                            value_type=arrow.JuArrow,
                            db_not_null=True)

"""

import logging

from itertools import count

from jukoro import arrow


logger = logging.getLogger(__name__)


# infinite counter to keep order of attrs while iterating over them
_counter = count()


class Attr(object):
    """
    Provides an abstraction do describe
    :class:`Entity <jukoro.pg.entity.AbstractEntity>` attribute

    :param db_index:    boolean (optional) indicating there must be an index
                        for this attribute in db
    :param value_type:  Python class (optional) to declare value type,
                        defaults to ``unicode``
    :param db_not_null: boolean (optional) indicating there must be
                        ``NOT NULL`` constraint for this field in db
    :param title:       unicode (optional) title or label for the attribute
    :param maxlen:      int (optional) restricts maximum length for
                        text attribute
    :param minlen:      int (optional) restricts minimum length for
                        text attribute
    :param wrapper:     callable (optional) to wrap entity attribute value
                        to when accessing it

    """
    __slots__ = ('db_not_null', 'db_index', 'value_type',
                 'title', 'maxlen', 'minlen', 'wrapper', '_idx')

    def __init__(self, **kwargs):
        self.db_index = kwargs.pop('db_index', False)
        self.value_type = kwargs.pop('value_type', unicode)
        self.db_not_null = kwargs.pop('db_not_null', True)
        self.title = kwargs.pop('title', 'undefined')
        self.maxlen = kwargs.pop('maxlen', 0)
        self.minlen = kwargs.pop('minlen', 0)
        self.wrapper = kwargs.pop('wrapper', None)

        self._idx = next(_counter)

    @property
    def is_int(self):
        """
        Returns ``True`` if ``value_type`` is ``int``
        or :class:`JuArrow <jukoro.arrow.base.JuArrow>`
        and ``False`` otherwise

        NB. expected way to store :class:`JuArrow <jukoro.arrow.base.JuArrow>`
        is to transform its float timestamp value to integer
        (it will be done automatically using
        :class:`JSONEncoder <jukoro.pg.PgJsonEncoder>`) to be able to sort
        records in db

        """
        return self.value_type in (int, arrow.JuArrow)

    @property
    def is_text(self):
        """
        Returns ``True`` if ``value_type`` is ``str`` or ``unicode``
        and ``False`` otherwise

        """
        return self.value_type in (str, unicode)

    @property
    def idx(self):
        """
        Returns own index

        :rtype: int

        """
        return self._idx

    def db_cast(self):
        """
        Returns string to cast db values to in queries

        :rtype: string

        """
        if self.is_int:
            return 'BIGINT'
        return 'TEXT'

    def __cmp__(self, other):
        # FIXME useless?
        if isinstance(other, type(self)):
            return self.idx - other.idx
        raise RuntimeError(
            'Unable to compare types "{}" and "{}"'.format(type(self),
                                                           type(other)))


class AttrDescr(object):
    """
    Python descriptor
    Works as a proxy for ``Attr`` instance

    :param slug:    ``Attr`` name
    :param attr:    ``Attr`` instance

    NB. :class:`EntityMeta <jukoro.pg.entity.EntityMeta>` replaces ``Attr``
    with ``AttrDescr`` to keep ``Attr`` name (slug) for a future reference
    if needed (this way we can describe attribute and it's name only once and
    there is no need to keep ``slug`` within ``Attr``)

    """
    __slots__ = ('_slug', '_attr')

    def __init__(self, slug, attr):
        assert slug is not None
        assert isinstance(attr, Attr)
        self._slug = slug
        self._attr = attr

    @property
    def slug(self):
        """
        Returns ``Attr`` slug

        :rtype: str

        """
        return self._slug

    def __get__(self, instance, owner):
        """
        Getter for ``Attr`` from instance
        Expects ``instance`` to have ``doc`` attribute
        (container for all instance's attributes)
        Returns wrapped value

        """
        if instance is None:
            return self
        val = instance.doc.get(self.slug)
        if callable(self._attr.wrapper):
            val = self._attr.wrapper(val)
        return val

    def __set__(self, instance, value):
        """
        Setter for instance's ``Attr``
        Expects ``instance`` to have ``doc`` attribute

        """
        instance.doc[self.slug] = value

    def __delete__(self, instance):
        """
        Deleter for instance's ``Attr``
        Expects ``instance`` to have ``doc`` attribute

        """
        instance.doc.pop(self.slug, None)

    def __getattr__(self, name):
        return getattr(self._attr, name)

    def __cmp__(self, other):
        return self._attr.__cmp__(other._attr)


class AttrsDescr(object):
    """
    Python descriptor
    Works as an iterator over
    :class:`Entity <jukoro.pg.entity.AbstractEntity>` attributes
    (instances of :class:`AttrDescr <jukoro.pg.attrs.AttrDescr>`)

    :param slugs:   list of slugs (attributes names) as declared for
                    :class:`Entity <jukoro.pg.entity.AbstractEntity>`
                    -based class

    NB. Iterates in sorted order using ``Attr`` indexes to sort

    """
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
