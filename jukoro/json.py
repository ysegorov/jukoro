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
    def default(self, obj):
        _type = type(obj)
        _meta = type(_type)
        if _meta in _encoders_by_meta:
            return _encoders_by_meta[_meta](obj)
        if _type in _encoders:
            return _encoders[_type](obj)
        return super(JSONEncoder, self).default(obj)


def register_encoder(klass, encoder_fn):
    _encoders[klass] = encoder_fn


def unregister_encoder(klass):
    return _encoders.pop(klass, None)


def register_encoder_by_meta(meta, encoder_fn):
    _encoders_by_meta[meta] = encoder_fn


def unregister_encoder_by_meta(meta):
    return _encoders_by_meta.pop(meta, None)


register_encoder(datetime.datetime, lambda x: x.isoformat())
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
    if 'cls' not in kwargs:
        kwargs['cls'] = JSONEncoder
    if 'ensure_ascii' not in kwargs:
        kwargs['ensure_ascii'] = False
    return kwargs


def _patch_decoder_kwargs(**kwargs):
    if 'cls' not in kwargs:
        kwargs['cls'] = JSONDecoder
    return kwargs
