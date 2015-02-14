# -*- coding: utf-8 -*-

import logging
import time
import uuid

from jukoro.redis.exceptions import AlreadyLocked


logger = logging.getLogger(__name__)


class RedisLock(object):

    def __init__(self, db, key, ttl=10, wait=False):
        self._db = db  # instance of jukoro.redis.db.RedisDb
        self._key = db.key('lock:%s' % key)
        self._ttl = int(ttl)
        self._value = str(uuid.uuid4())
        self._wait = wait

    @property
    def db(self):
        return self._db

    def __enter__(self):
        self.acquire()

    def _set_lock(self):
        return self.db.set(self._key, self._value, ex=self._ttl, nx=True)

    def acquire(self):
        if self._set_lock():
            logger.debug('redis lock acquired')
            return
        if self._wait:
            end = time.time() + self._ttl + 1
            while end > time.time():
                if self._set_lock():
                    logger.debug('redis lock aquired waiting')
                    return
                time.sleep(.002)
        logger.debug('failed to acquire redis lock')
        raise AlreadyLocked('Lock for key "{}" exists'.format(self._key))

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def release(self):
        self.db.strict_release(keys=[self._key], args=[self._value])
        logger.debug('redis lock released')
