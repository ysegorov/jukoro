# -*- coding: utf-8 -*-

import cPickle


load = cPickle.load
loads = cPickle.loads


def dump(obj, f):
    return cPickle.dump(obj, f, protocol=cPickle.HIGHEST_PROTOCOL)


def dumps(obj):
    return cPickle.dumps(obj, protocol=cPickle.HIGHEST_PROTOCOL)
