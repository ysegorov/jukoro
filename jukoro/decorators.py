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
    """ Decorator to simplify coroutine initialization """
    @functools.wraps(fn)
    def deco(*args, **kwargs):
        cr = fn(*args, **kwargs)
        cr.next()
        return cr
    return deco


def profile(quick=False, sort_by='cumtime', stats_limit=40):
    """
    Decorator to profile function or method execution using ``cProfile`` or
    simple time measurement

    :param quick:       use ``cProfile`` if True
    :param sort_by:     ``pstats.Stats.sort_stats`` method argument to order
                        ``cProfile`` profiling results by
    :param stats_limit: ``pstats.Stats.print_stats`` method argument to limit
                        number of profiling results to show

    """
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
    """
    Decorator to memoize decorated function results using function arguments
    as cache key

    """
    __slots__ = ('_fn', '_cache')

    def __init__(self, fn):
        """
        Initialize decorator

        :param fn: function to decorate

        """
        self._fn = fn
        self._cache = {}

    def key(self, *args):
        """
        Method to build cache key

        :param *args: list of positional arguments to be passed
                      to decorated function

        """
        return hashlib.sha1(':'.join(str(x) for x in args)).hexdigest()

    def __call__(self, *args, **kwargs):
        """
        Method to get cached decorated function call result

        :param *args:    positional arguments to be passed
                         to decorated function and to be used as key
        :param **kwargs: keyword arguments to be passed to decorated function

        """
        k = self.key(*args)
        try:
            return self._cache[k]
        except KeyError:
            self._cache[k] = res = self._fn(*args, **kwargs)
            return res


class raise_if(object):
    """
    Decorator factory to raise exception with specified message
    if specified condition fails

    """
    __slots__ = ('_exc_type', '_msg', '_test_fn')

    def __init__(self, exc_type, msg, test_fn):
        """
        Initialize decorator factory

        :param exc_type: exception class to be raised
        :param msg:      exception message
        :param test_fn:  callable to test some condition before the decorated
                         function or method call
                         (must have same signature as decorated
                          function or method)
                         (must return True (any Python object which
                          evaluates to True) to raise)

        """
        self._exc_type = exc_type
        self._msg = msg
        self._test_fn = test_fn

    def __call__(self, fn):
        """
        Create and return decorator for decorated function

        :param fn: function or method to decorate

        """
        test_fn = self._test_fn
        exc_type = self._exc_type
        msg = self._msg

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if test_fn(*args, **kwargs):
                raise exc_type(msg)
            return fn(*args, **kwargs)

        return wrapper
