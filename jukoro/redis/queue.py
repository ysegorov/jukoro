# -*- coding: utf-8 -*-

import logging

from jukoro.redis.exceptions import QueueError


logger = logging.getLogger(__name__)

CONTROL_QUEUE = 'qCtl'


class RedisQueue(object):

    def __init__(self, db, queues=None, timeout=None):
        self._db = db
        self._queues = queues or []
        self._keys = None
        self._timeout = timeout

        if self._queues and not isinstance(self._queues, (list, tuple)):
            self._queues = [self._queues]

    @property
    def db(self):
        return self._db

    @property
    def keys(self):
        if self._keys is None:
            if CONTROL_QUEUE in self._queues:
                raise QueueError('reserved name in queues')
            self._queues.append(CONTROL_QUEUE)
            self._keys = [self.db.key(x) for x in self._queues]
        return self._keys

    def put(self, queue, *values):
        key = self.db.key(queue)
        self.db.rpush(key, *values)

    def consume(self):
        cq = self.db.key(CONTROL_QUEUE)
        while True:
            item = self.db.blpop(self.keys, timeout=self._timeout)
            if item:
                if item[0] == cq and item[1] == 'STOP':
                    break
                yield item

    def stop(self):
        key = self.db.key(CONTROL_QUEUE)
        self.db.rpush(key, 'STOP')
