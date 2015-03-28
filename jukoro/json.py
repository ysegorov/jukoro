# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import decimal
import json as _json


D = decimal.Decimal


_encoders = {}
_encoders_by_meta = {}


class JSONDecoder(_json.JSONDecoder):
    """
        JSON decoder that converts number in JSON to Decimal in Python
    """
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('parse_float', D)
        super(JSONDecoder, self).__init__(*args, **kwargs)


class JSONEncoder(_json.JSONEncoder):
    """
        JSON encoder that converts additional Python types to JSON
    """
    json_attr = 'json_val'

    def default(self, obj):
        if hasattr(obj, self.json_attr):
            attr = getattr(obj, self.json_attr)
            return attr() if callable(attr) else attr
        _type = type(obj)
        if _type in _encoders:
            return _encoders[_type](obj)
        return super(JSONEncoder, self).default(obj)


def register_encoder(klass, encoder_fn):
    _encoders[klass] = encoder_fn


def unregister_encoder(klass):
    return _encoders.pop(klass, None)


def isoformat(dt):
    return dt.isoformat()


register_encoder(datetime.datetime, isoformat)
register_encoder(D, float)


def dump(*args, **kwargs):
    kwargs = _patch_encoder_kwargs(**kwargs)
    return _json.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    kwargs = _patch_encoder_kwargs(**kwargs)
    return _json.dumps(*args, **kwargs)


def load(*args, **kwargs):
    kwargs = _patch_decoder_kwargs(**kwargs)
    return _json.load(*args, **kwargs)


def loads(*args, **kwargs):
    kwargs = _patch_decoder_kwargs(**kwargs)
    return _json.loads(*args, **kwargs)


def _patch_encoder_kwargs(**kwargs):
    kwargs.setdefault('cls', JSONEncoder)
    kwargs.setdefault('ensure_ascii', False)
    return kwargs


def _patch_decoder_kwargs(**kwargs):
    kwargs.setdefault('cls', JSONDecoder)
    return kwargs
