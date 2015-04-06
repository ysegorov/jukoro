# -*- coding: utf-8 -*-
"""
Simple Redis-based queue abstraction supporting multiple queues to watch for

Roadmap:

- support multiple queue managers using different control queues

"""

import logging

from jukoro.redis.exceptions import QueueError


logger = logging.getLogger(__name__)

CONTROL_QUEUE = 'qCtl'


class RedisQueue(object):
    """
    Redis-based queue abstraction watching for multiple queues in a blocking
    manner

    :param db:          instance of :class:`~jukoro.redis.db.RedisDb`
    :param queues:      name of a single queue or list of queues to watch for
    :param timeout:     timeout value for ``db.blpop`` call watching
                        for queues

    """

    def __init__(self, db, queues=None, timeout=None):
        self._db = db
        self._queues = queues or []
        self._keys = None
        self._timeout = timeout

        if self._queues and not isinstance(self._queues, (list, tuple)):
            self._queues = [self._queues]

    @property
    def db(self):
        """
        Returns instance of :class:`~jukoro.pg.db.RedisDb`

        """
        return self._db

    @property
    def keys(self):
        """
        Populates internal list of namespaced redis keys for corresponding
        queues names

        :returns:               list of keys to watch for
        :raises QueueError:     in case control queue name (``qCtl``)
                                was specified while initializing this
                                queue manager

        """
        if self._keys is None:
            if CONTROL_QUEUE in self._queues:
                raise QueueError('reserved name in queues')
            self._queues.append(CONTROL_QUEUE)
            self._keys = [self.db.key(x) for x in self._queues]
        return self._keys

    def put(self, queue, *values):
        """
        Populates queue with values

        :param queue:       queue name
        :param values:      values to populate queue with

        """
        key = self.db.key(queue)
        self.db.rpush(key, *values)

    def consume(self):
        """
        Iterates over values from controlled queues, stops iteration in case
        it was signalled to stop

        :yields item:   consisting of (queue_name, value)

        """
        cq = self.db.key(CONTROL_QUEUE)
        while True:
            item = self.db.blpop(self.keys, timeout=self._timeout)
            if item:
                if item[0] == cq and item[1] == 'STOP':
                    break
                yield item

    def stop(self):
        """
        Signals queue manager to stop processing queue

        """
        key = self.db.key(CONTROL_QUEUE)
        self.db.rpush(key, 'STOP')
