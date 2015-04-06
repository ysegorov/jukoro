# -*- coding: utf-8 -*-
"""
Simple Redis-based distributed lock manager implementation
(for an only master Redis-node scenario)

See also:

- `Distributed locks with Redis <http://redis.io/topics/distlock>`_

Roadmap:

- support multiple master Redis nodes (Redlock algorithm)

"""

import logging
import time
import uuid

from jukoro.redis.exceptions import AlreadyLocked


logger = logging.getLogger(__name__)


class RedisLock(object):
    """
    DLM simple implementation with safe acquire and release operations
    acting as context manager

    :param db:      instance of :class:`~jukoro.redis.db.RedisDb`
    :param key:     lock key
    :param ttl:     time-to-live to acquire lock in case it is not free
    :param wait:    boolean indicating we must wait for ``ttl`` seconds
                    to acquire lock if it is not free

    Usage example:

    .. code-block:: python

        from jukoro import redis

        URI = 'redis://localhost/2'
        NS = 'JuTest'

        db = redis.RedisDb(URI, ns=NS)
        key = 'b-lock-key'
        with redis.RedisLock(db, key):
            db.set(db.key('b'), '2015')

    """

    def __init__(self, db, key, ttl=10, wait=False):
        self._db = db  # instance of jukoro.redis.db.RedisDb
        self._key = db.key('lock:%s' % key)
        self._ttl = int(ttl)
        self._value = str(uuid.uuid4())
        self._wait = wait

    @property
    def db(self):
        """
        Returns instance of :class:`~jukoro.pg.db.RedisDb`

        """
        return self._db

    def __enter__(self):
        self.acquire()

    def _set_lock(self):
        """
        Safely creates lock if it does not exist in redis

        :returns:   boolean indicating key was created or not

        """
        return self.db.set(self._key, self._value, ex=self._ttl, nx=True)

    def acquire(self):
        """
        Acquires lock waiting for busy lock for ``ttl`` seconds
        if we must ``wait``

        :raises AlreadyLocked:      in case ttl passed and we still have
                                    no lock

        """
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
        """
        Safely releases lock from redis

        """
        self.db.strict_release(keys=[self._key], args=[self._value])
        logger.debug('redis lock released')
