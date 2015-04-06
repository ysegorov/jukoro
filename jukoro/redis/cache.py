# -*- coding: utf-8 -*-
"""
Module to create and use Redis-based cache

Uses :mod:`jukoro.pickle` to transparently pickle/unpickle cached
Python values

"""

import hashlib
import logging

from jukoro import pickle


logger = logging.getLogger(__name__)

TTL = 60 * 60 * 24


class RedisCache(object):
    """
    Abstraction for Redis-based cache

    :param db:      instance of :class:`~jukoro.pg.db.RedisDb`
    :param ttl:     time-to-live for cache value
                    (defaults to one day if not specified)

    """

    def __init__(self, db, ttl=None):
        self._db = db
        self._ns = None
        self._ttl = ttl or TTL

    @property
    def db(self):
        """
        Returns instance of :class:`~jukoro.pg.db.RedisDb`

        """
        return self._db

    @property
    def ns(self):
        """
        Generates cache namespace value

        :returns:       cache namespace
        :rtype:         str

        """
        if self._ns is None:
            self._ns = self.db.key('cache:{}')
        return self._ns

    def key(self, *args):
        """
        Generates cache key

        :param args:    positional arguments to generate cache key from
                        (arguments must be stringable)
        :returns:       cache key
        :rtype:         str

        """
        k = ':'.join(str(x) for x in args)
        return self.ns.format(hashlib.sha256(k).hexdigest())

    def get(self, cache_key):
        """
        Returns unpickled cache value if it exists in redis

        :param cache_key:   cache key
        :returns:           None or unpickled value

        """
        res = self.db.get(cache_key)
        if res is not None:
            res = pickle.loads(res)
        return res

    def set(self, cache_key, value, ttl=None):
        """
        Stores pickled value in cache for a specified or default ttl

        :param cache_key:   cache key
        :param value:       Python value to store in cache
        :param ttl:         time-to-live for value (defaults to one day)

        """
        ttl = int(ttl or self._ttl)
        value = pickle.dumps(value)
        return self.db.set(cache_key, value, ex=ttl)

    def delete(self, cache_key):
        """
        Deleted cached value from cache

        :param cache_key:   cache key
        """
        return self.db.delete(cache_key)
