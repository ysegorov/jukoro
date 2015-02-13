# -*- coding: utf-8 -*-

import collections
import logging


logger = logging.getLogger(__name__)


class ObjectDict(dict):
    """ Borrowed from ``tornado.util.ObjectDict`` """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        self.pop(name, None)

    def copy(self):
        # FIXME better way?
        return self.__class__(super(ObjectDict, self).copy())


class DefaultObjectDict(collections.defaultdict):

    def __getattr__(self, name):
        return self[name]

    def __setattr(self, name, value):
        self[name] = value


class LockRing(object):

    __slots__ = ('_store', '_locks', '_ring')

    def __init__(self):
        self._store = set()
        self._locks = set()
        self._ring = None

    def push(self, item):
        self._store.add(item)
        if item in self._locks:
            if len(self._locks) == len(self._store):
                self._ring = None
            self.unlock(item)
            return True

    def pop(self):
        if not self:
            raise IndexError('empty ring')
        item = self._store.pop()
        if item in self._locks:
            self._locks.remove(item)
        self._ring = None
        return item

    def lock(self, item):
        self._locks.add(item)

    def unlock(self, item):
        self._locks.remove(item)

    def is_locked(self, item):
        return item in self._locks

    def reset(self):
        self._store = set()
        self._locks = set()
        self._ring = None

    def _iter(self):
        while self._store:
            for item in self._store:
                if len(self._store) == len(self._locks):
                    raise IndexError('all locked')
                if item not in self._locks:
                    # logger.info('yielding item %s', item)
                    yield item

    def next(self):
        if not self:
            raise IndexError('empty ring')
        if not self._ring:
            self._ring = self._iter()
        item = next(self._ring)
        self.lock(item)
        return item

    def __len__(self):
        return len(self._store)
