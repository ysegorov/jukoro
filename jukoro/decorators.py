# -*- coding: utf-8 -*-

from __future__ import division

import cProfile
import functools
import hashlib
import inspect
import logging
import pstats
import time


logger = logging.getLogger(__name__)


def coroutine(fn):
    @functools.wraps(fn)
    def deco(*args, **kwargs):
        cr = fn(*args, **kwargs)
        cr.next()
        return cr
    return deco


def profile(quick=False, sort_by='cumtime', stats_limit=40):
    def inner(fn):
        _lname = getattr(inspect.getmodule(fn), '__name__')
        _fname = fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not quick:
                p = cProfile.Profile()
                res = p.runcall(fn, *args, **kwargs)
                pstats.Stats(p).sort_stats(sort_by).print_stats(stats_limit)
                return res
            started = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                finished = time.time()
                logger.debug('"%s.%s" done for %.5f s',
                             _lname, _fname, finished - started)
        return wrapper
    return inner


class memoize(object):

    def __init__(self, fn):
        self._fn = fn
        self._cache = {}

    def key(self, *args):
        return hashlib.sha1(':'.join(str(x) for x in args)).hexdigest()

    def __call__(self, *args, **kwargs):
        k = self.key(*args)
        try:
            return self._cache[k]
        except KeyError:
            self._cache[k] = res = self._fn(*args, **kwargs)
            return res
