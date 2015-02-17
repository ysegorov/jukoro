# -*- coding: utf -8 -*-

import urllib
import urlparse

from jukoro.utils import os_user

from jukoro.pg.exceptions import PgUriError


def pg_uri_to_kwargs(uri):
    parsed = urlparse.urlparse(uri)

    if parsed.scheme != 'postgresql':
        raise PgUriError('uri must start with "postgresql://"')

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
