# -*- coding: utf-8 -*-

from __future__ import absolute_import, division

import arrow

from .base import JuArrow, PRECISION


_factory = arrow.ArrowFactory(JuArrow)


def get(*args, **kwargs):
    return _factory.get(*args, **kwargs)

from_json_val = get


def now(*args, **kwargs):
    return _factory.now(*args, **kwargs)


def utcnow(*args, **kwargs):
    return _factory.utcnow(*args, **kwargs)


def from_db_val(val):
    return get(int(val) / PRECISION)
