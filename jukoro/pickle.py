# -*- coding: utf-8 -*-
"""
Module to pickle/unpickle Python objects using highest pickle protocol
by default

"""

import cPickle


load = cPickle.load
loads = cPickle.loads


def dump(obj, f):
    """
    Write an object in pickle format to the given file

    :param obj: object to pickle
    :param f:   file to write pickled object to

    """
    cPickle.dump(obj, f, protocol=cPickle.HIGHEST_PROTOCOL)


def dumps(obj):
    """
    Return a string containing an object in pickle format

    :param obj: object to pickle
    :return: pickled object
    :rtype: str

    """
    return cPickle.dumps(obj, protocol=cPickle.HIGHEST_PROTOCOL)
