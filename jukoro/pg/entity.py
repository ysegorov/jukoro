# -*- coding: utf-8 -*-
"""
Basic machinery to work with ``AbstractEntity`` derived classes

Contains:

- :class:`EntityMeta <jukoro.pg.entity.EntityMeta>` - metaclass to make needed
  checks and class attributes modifications and to register class within
  storage (if allowed)
- :class:`AbstractEntity <jukoro.pg.entity.AbstractEntity>` - abstract class to
  be the base for derived classes
- :class:`AbstractUser <jukoro.pg.entity.AbstractUser>` - abstract user
  implementation

Example definition of Entity class:

.. code-block:: python

    # -*- coding: utf-8 -*-
    # file: project/entities.py

    from jukoro import arrow
    from jukoro import pg


    class Mail(pg.AbstractEntity):
        db_table = 'ju_mail'

        from = pg.Attr(title='From',
                       db_index=True, db_not_null=True, minlen=4)
        to = pg.Attr(title='To',
                     db_index=True, db_not_null=True, minlen=4)
        subject = pg.Attr(title='Subject')
        body = pg.Attr(title='Body')

"""

import logging

from jukoro import json

from jukoro.pg.attrs import Attr, AttrDescr, AttrsDescr
from jukoro.pg.query import QueryBuilderDescr
from jukoro.pg import storage


logger = logging.getLogger(__name__)


def _slugs(base):
    """
    Returns list of attrs slugs attached to base

    :param base:    class inherited from ``AbstractEntity``
    :returns:       list of slugs
    :rtype:         list

    """
    if hasattr(base, 'attrs'):
        return [attr.slug for attr in base.attrs]
    return []


class EntityMeta(type):
    """
    Metaclass to create ``AbstractEntity`` derived class

    Performs checks for:

    - reserved attributes names are not used (``attrs``, ``_created``,
      ``_updated``, ``_deleted``)
    - ``db_table`` attribute name (if specified) != **entity**

    Transforms declared attributes
    (instances of :class:`Attr <jukoro.pg.attrs.Attr>`) to Python descriptors
    (instances of :class:`AttrDescr <jukoro.pg.attrs.AttrDescr>`)

    Creates ``attrs`` Python descriptor
    (instance of :class:`AttrDescrs <jukoro.pg.attrs.AttrsDescr>`) to iterate
    over attached attributes (collects all attributes from ``bases``)

    In case class has ``db_table`` attribute defined transforms it to
    Python descriptor :class:`DBTableName <jukoro.pg.storage.DBTableName>` and
    creates ``db_view`` attribute
    (instance of :class:`DBViewName <jukoro.pg.storage.DBViewName>`)

    Registers class within ``jukoro.pg.storage`` registry if it has
    ``db_table`` attribute defined
    (for proper ``jukoro.pg.storage.syncdb`` usage)

    Class can skip registration within registry using ``skip_registry``
    attribute set to True

    Creates ``qbuilder`` class attribute (instance of
    :class:`QueryBuilderDescr <jukoro.pg.query.QueryBuilderDescr>`) to provide
    access to simple sql query builder machinery

    """

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
    """
    Abstract class for entities

    Must be used to create derived classes

    By default has:

    - ``db_table`` attribute undefined
    - ``qbuilder`` attribute set to ``None``

    :param entity_id:   (int) entity_id
    :param doc:         (dict) dictionary containing attributes values

    """
    __metaclass__ = EntityMeta
    __slots__ = ('_entity_id', '_doc')

    qbuilder = None

    def __init__(self, entity_id=None, doc=None):
        self._entity_id = entity_id
        self._doc = doc or {}

    @property
    def entity_id(self):
        """
        Returns entity id

        :rtype: int or None

        """
        return self._entity_id

    @property
    def doc(self):
        """
        Returns container with attributes values

        :rtype: dict

        """
        return self._doc

    @property
    def created(self):
        """
        Returns value of ``_created`` attribute from ``doc``

        :rtype: ISO 8601 formatted string

        Value of this attribute is autofilled in PostgreSQL

        """
        return self._doc.get('_created')

    @property
    def updated(self):
        """
        Returns value of ``_updated`` attribute from ``doc``

        :rtype: ISO 8601 formatted string

        Value of this attribute is autofilled in PostgreSQL

        """
        return self._doc.get('_updated')

    @property
    def deleted(self):
        """
        Returns value of ``_deleted`` attribute from ``doc``

        :rtype: ISO 8601 formatted string

        Value of this attribute is autofilled in PostgreSQL

        """
        # TODO (useless for now)
        return self._doc.get('_deleted')

    def update(self, **kwargs):
        """
        Updates ``doc`` with provided ``attr=value`` pairs

        :param kwargs:  keyword arguments to update ``doc`` from

        """
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def db_val(self):
        """
        Method to return jsonable value of the instance to store
        within db (according to convention in
        :class:`PgJsonEncoder <jukoro.pg.PgJsonEncoder>`)

        :rtype: int or None

        Can be overriden to return ``doc`` to nest entities

        """
        return self.entity_id

    def json_val(self):
        """
        Method to return jsonable value of the instance for exchange
        between parties (according to convention in
        :class:`JSONEncoder <jukoro.json.JSONEncoder>`)

        :rtype: int or None

        """
        return self.entity_id

    @classmethod
    def by_id(cls, cursor, entity_id):
        """
        Loads entity from db

        :param cursor:      instance of
                            :class:`PgTransaction <jukoro.pg.db.PgTransaction>`
        :param entity_id:   (int) entity id to load
        :returns:           new instance
        :rtype:             ``cls``
        """
        q, params = cls.qbuilder.by_id(entity_id)
        res = cursor.execute_and_get(q, params)
        return cls(**res)

    def save(self, cursor):
        """
        Saves instance in db
        Detects operation to use (create or update) according to ``entity_id``
        value (if ``entity_id`` is ``None`` the operation is to create and
        to update otherwise)

        :param cursor:      instance of
                            :class:`PgTransaction <jukoro.pg.db.PgTransaction>`

        :returns:           saved or updated instance recreated (new instance)
        :rtype:             same as self

        """
        klass = type(self)
        if self._entity_id is None:
            q, params = klass.qbuilder.create(self)
        else:
            q, params = klass.qbuilder.update(self)
        res = cursor.execute_and_get(q, params)
        logger.debug('res: %s', res)
        return klass(**res)

    def delete(self, cursor):
        """
        Deletes instance from db

        :param cursor:      instance of
                            :class:`PgTransaction <jukoro.pg.db.PgTransaction>`

        """
        klass = type(self)
        q, params = klass.qbuilder.delete(self)
        cursor.execute(q, params)

    def serialize(self):
        """
        Serializes instance to json string

        :rtype:     str

        """
        return json.dumps({'entity_id': self._entity_id, 'doc': self._doc})

    @classmethod
    def deserialize(cls, value):
        """
        Deserializes json string to instance of ``cls``

        :param value:       json string
        :returns:           newly created instance
        :rtype:             ``cls``

        """
        return cls(**json.loads(value))

    def __eq__(self, other):
        """
        Magic method to test instances for equality
        Instances are equal in case they have equal ``entity_id`` and
        equal ``doc`` attributes/values pairs

        """
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
    """
    Abstract User implementation

    """
    username = Attr(title='Username',
                    db_index=True, db_not_null=True, minlen=4)
    email = Attr(title='Email',
                 db_index=True, db_not_null=True, minlen=6)
    password = Attr(title='Password', db_not_null=True)
