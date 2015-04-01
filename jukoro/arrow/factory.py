# -*- coding: utf-8 -*-
"""
Provides factory to construct :class:`JuArrow <jukoro.arrow.JuArrow>` instances

"""

from __future__ import absolute_import, division

import arrow

from .base import JuArrow, PRECISION


_factory = arrow.ArrowFactory(JuArrow)


def get(*args, **kwargs):
    """
    Create :class:`JuArrow <jukoro.arrow.JuArrow>` instance using
    factory's ``get`` method

    """
    return _factory.get(*args, **kwargs)

# convience name for json deserialization
from_json_val = get


def now(*args, **kwargs):
    """
    Create :class:`JuArrow <jukoro.arrow.JuArrow>` instance using
    factory's ``now`` method

    """
    return _factory.now(*args, **kwargs)


def utcnow(*args, **kwargs):
    """
    Create :class:`JuArrow <jukoro.arrow.JuArrow>` instance using
    factory's ``utcnow`` method

    """
    return _factory.utcnow(*args, **kwargs)


def from_db_val(val):
    """
    Create :class:`JuArrow <jukoro.arrow.JuArrow>` from integer stored in
    PostgreSQL

    """
    return get(int(val) / PRECISION)
