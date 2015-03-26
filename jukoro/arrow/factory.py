# -*- coding: utf-8 -*-

from __future__ import absolute_import

import arrow

from .base import JuArrow


_factory = arrow.ArrowFactory(JuArrow)


def get(*args, **kwargs):
    return _factory.get(*args, **kwargs)


def now(*args, **kwargs):
    return _factory.now(*args, **kwargs)


def utcnow(*args, **kwargs):
    return _factory.utcnow(*args, **kwargs)
