# -*- coding: utf-8 -*-
"""
Abstraction to work with Redis connection

"""

from __future__ import absolute_import

import logging

import redis

from jukoro.redis.exceptions import NotRegisteredScript


logger = logging.getLogger(__name__)

STRICT_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class Lua(object):
    """
    Lua script to be registered within Redis abstraction

    :param script:  script to register

    """
    __slots__ = ('_script', '_lua')

    def __init__(self, script):
        self._script = script
        self._lua = None

    def register(self, conn):
        self._lua = conn.register_script(self._script)

    def __call__(self, keys, args):
        if self._lua is None:
            raise NotRegisteredScript
        return self._lua(keys=keys, args=args)


class RedisDb(object):
    """
    Proxy for ``StrictRedis`` supporting namespaced ``Redis`` keys and custom
    ``Lua`` scripts

    :param uri:     connection uri
    :param ns:      namespace for keys

    """

    def __init__(self, uri, ns='app'):
        self._db = None
        self._uri = uri
        self._ns = ns
        self.strict_release = Lua(STRICT_RELEASE)

    @property
    def db(self):
        """
        Initializes redis connection if not initialized yet

        :returns:   connection
        :rtype:     instance of ``redis.StrictRedis``

        """
        if self._db is None:
            self._db = redis.StrictRedis.from_url(self._uri)
            self.strict_release.register(self._db)
        return self._db

    def key(self, name):
        """
        Generates namespaced key

        :rtype:     str

        """
        return '{}:{}'.format(self._ns, name)

    def __getattr__(self, name):
        return getattr(self.db, name)
