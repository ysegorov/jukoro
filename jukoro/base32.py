# -*- coding: utf-8 -*-
"""
Module for base32 integer serialization/deserialization using
``base32_crockford`` library

Allows negative numbers to be encoded/decoded using predefined prefix

"""

import base32_crockford as b32


PREFIX = '__'


def encode(val):
    """
    Encode integer to base32 string (supporting negative integers)

    :param val: integer to encode
    :return:    base32-encoded value
    :rtype:     string
    :raises AssertionError: if ``val`` is not instance of ``int``

    """
    assert isinstance(val, int)
    prefix, num = '', val
    if num < 0:
        prefix = PREFIX
        num = abs(num)
    return '{p}{n}'.format(p=prefix, n=b32.encode(num))


def decode(val):
    """
    Decode base32 string to integer (supporting negative integers)

    :param val: base32-encoded string
    :return:    decoded value
    :rtype:     int
    :raises AssertionError: if ``val`` is not instance of ``basestring``

    """
    assert isinstance(val, basestring)
    factor = 1
    if val[:len(PREFIX)] == PREFIX:
        factor = -1
        val = val[len(PREFIX):]
    return factor * b32.decode(val)
