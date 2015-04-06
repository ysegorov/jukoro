# -*- coding: utf-8 -*-
"""
Abstraction to work with Redis connection

"""

from __future__ import absolute_import

import logging

import redis


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

    """

    def __init__(self):
        self.strict_release = None

    def register(self, conn):
        self.strict_release = conn.register_script(STRICT_RELEASE)


class RedisDb(object):
    """
    ``StrictRedis`` proxy supporting namespaced ``Redis`` keys and custom
    ``Lua`` scripts (not extendable yet)

    :param uri:     connection uri
    :param ns:      namespace for keys

    """

    def __init__(self, uri, ns='app'):
        self._db = None
        self._uri = uri
        self._ns = ns
        self._lua = Lua()

    @property
    def db(self):
        """
        Initializes redis connection if not initialized yet

        :returns:   connection
        :rtype:     instance of ``redis.StrictRedis``

        """
        if self._db is None:
            self._db = redis.StrictRedis.from_url(self._uri)
            self._lua.register(self._db)
        return self._db

    def key(self, name):
        """
        Generates namespaced key

        :rtype:     str

        """
        return '{}:{}'.format(self._ns, name)

    def strict_release(self, keys, args):
        """
        Calls registered ``Lua`` script
        """
        return self._lua.strict_release(keys=keys, args=args)

    def __getattr__(self, name):
        return getattr(self.db, name)
