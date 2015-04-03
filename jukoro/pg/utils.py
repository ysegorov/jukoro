# -*- coding: utf -8 -*-
"""
Module for :mod:`jukoro.pg` specific utilities

"""
import urllib
import urlparse

from jukoro.utils import os_user

from jukoro.pg.exceptions import BadUri


def pg_uri_to_kwargs(uri):
    """
    Transforms connection string to dictionary consumable by
    :class:`~jukoro.pg.db.PgConnection` and
    consumable by ``psycopg2.connect`` without ``scheme`` attribute

    Provides defaults for:

    - "username" - defaults to os username
    - "port" - defaults to 5432
    - "dbname" - defaults to os username
    - "scheme" - defaults to "public"

    :param uri:     connection string
    :returns:       dictionary with connection parameters

    Required uri fields:

    - "protocol" - must be ``postgresql://``
    - "hostname" - must be specified
    - "path" - must be specified and transforms to "dbname"

    Scheme name can specified using dot notation::

        'db_name.scheme_name'

    Usage examples:

    .. code-block:: pycon

        >>> from jukoro.pg import pg_uri_to_kwargs
        >>> from pprint import pprint

        >>> pprint(pg_uri_to_kwargs('postgresql://localhost/jukoro_test.ju_20150403102042'))
        {'dbname': 'jukoro_test',
        'host': 'localhost',
        'password': None,
        'port': 5432,
        'scheme': 'ju_20150403102042',
        'user': 'egorov'}

        >>> pprint(pg_uri_to_kwargs('postgresql://localhost/jukoro_test'))
        {'dbname': 'jukoro_test',
        'host': 'localhost',
        'password': None,
        'port': 5432,
        'scheme': 'public',
        'user': 'egorov'}

        >>> pprint(pg_uri_to_kwargs('postgresql://localhost:5555/jukoro_test'))
        {'dbname': 'jukoro_test',
        'host': 'localhost',
        'password': None,
        'port': 5555,
        'scheme': 'public',
        'user': 'egorov'}

        >>> pprint(pg_uri_to_kwargs('postgres://localhost:5555/jukoro_test'))
        Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "jukoro/pg/utils.py", line 49, in pg_uri_to_kwargs
            raise BadUri('uri must start with "postgresql://"')
        jukoro.pg.exceptions.BadUri: uri must start with "postgresql://"

    """
    parsed = urlparse.urlparse(uri)

    if parsed.scheme != 'postgresql':
        raise BadUri('uri must start with "postgresql://"')

    _user = os_user()
    mapped = (
        ('hostname', 'host', lambda x: x),
        ('username', 'user', lambda x: x and x or _user),
        ('password', 'password', lambda x: x and urllib.unquote(x)),
        ('port', 'port', lambda x: x and int(x) or 5432),
        ('path', 'dbname', lambda x: x and _dbname(x[1:]) or _user),
        ('path', 'schema', lambda x: x and _scheme(x[1:]) or 'public'),
    )
    return dict((k, cast(getattr(parsed, pk))) for (pk, k, cast) in mapped)


def _dbname(name):
    return name.partition('.')[0]


def _scheme(name):
    return name.partition('.')[-1]
