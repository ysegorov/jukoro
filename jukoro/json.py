# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import decimal
import json as _json


load = _json.load
loads = _json.loads

JSONDecoder = _json.JSONDecoder


class JSONEncoder(_json.JSONEncoder):
    """
    JSON encoder that converts additional Python types to JSON.
    """
    def default(self, obj):
        """
        Converts datetime objects to ISO-compatible strings
            during json serialization
        Converts Decimal objects to floats during json serialization
        """
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return None


def _patch_kwargs(**kwargs):
    if 'cls' not in kwargs:
        kwargs['cls'] = JSONEncoder
    if 'ensure_ascii' not in kwargs:
        kwargs['ensure_ascii'] = False
    return kwargs


def dump(*args, **kwargs):
    kwargs = _patch_kwargs(**kwargs)
    return _json.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    kwargs = _patch_kwargs(**kwargs)
    return _json.dumps(*args, **kwargs)
