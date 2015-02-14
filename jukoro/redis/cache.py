# -*- coding: utf-8 -*-

import hashlib
import logging

from jukoro import pickle


logger = logging.getLogger(__name__)

TTL = 60 * 60 * 24


class RedisCache(object):

    def __init__(self, db, ttl=None):
        self._db = db
        self._ns = None
        self._ttl = ttl or TTL

    @property
    def db(self):
        return self._db

    @property
    def ns(self):
        if self._ns is None:
            self._ns = self.db.key('cache:{}')
        return self._ns

    def key(self, *args):
        k = ':'.join(str(x) for x in args)
        return self.ns.format(hashlib.sha256(k).hexdigest())

    def get(self, cache_key):
        res = self.db.get(cache_key)
        if res is not None:
            res = pickle.loads(res)
        return res

    def set(self, cache_key, value, ttl=None):
        ttl = int(ttl or self._ttl)
        value = pickle.dumps(value)
        return self.db.set(cache_key, value, ex=ttl)

    def delete(self, cache_key):
        return self.db.delete(cache_key)
