# -*- coding: utf-8 -*-

import base32_crockford as b32


PREFIX = '__'


def encode(val):
    assert isinstance(val, int)
    prefix, num = '', val
    if num < 0:
        prefix = PREFIX
        num = abs(num)
    return '{p}{n}'.format(p=prefix, n=b32.encode(num))


def decode(val):
    assert isinstance(val, basestring)
    factor = 1
    if val[:len(PREFIX)] == PREFIX:
        factor = -1
        val = val[len(PREFIX):]
    return factor * b32.decode(val)
