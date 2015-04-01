# -*- coding: utf-8 -*-
"""
Module for JSON serialization/deserialization providing a way to register
custom encoders or use api convention to get object's to encode value

By default decodes JSON number to :class:`Decimal <decimal.Decimal>`

"""

from __future__ import absolute_import

import datetime
import decimal
import json as _json


D = decimal.Decimal


_encoders = {}


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

    Custom object can be encoded using simple convention - object can have
    ``json_val`` property or method to get it's encoded value

    """
    json_attr = 'json_val'

    def default(self, obj):
        """
        Encode object using:
            - api convention (``json_val`` property or method)
            - registered encoders for specified types

        Calls ``super`` in case object's type doesn't have ``json_val``
        attribute and it's not registered

        """
        if hasattr(obj, self.json_attr):
            attr = getattr(obj, self.json_attr)
            return attr() if callable(attr) else attr
        _type = type(obj)
        if _type in _encoders:
            return _encoders[_type](obj)
        return super(JSONEncoder, self).default(obj)


def register_encoder(klass, encoder_fn):
    """
    Function to register ``encoder_fn`` as encoder for specified ``klass``

    :param klass:      class to register encoder for
    :param encoder_fn: callable to encode instance of ``klass`` having
                       signature ``encoder_fn(instance)``

    """
    _encoders[klass] = encoder_fn


def unregister_encoder(klass):
    """
    Function to unregister encoder for specified ``klass``

    :param klass: class to unregister encoder for
    :return:      previously registered encoder or None
    :rtype:       callable or None

    """
    return _encoders.pop(klass, None)


def isoformat(dt):
    """
    Function to act as ``datetime.datetime`` instance encoder

    :param dt: instance of ``datetime.datetime``
    :return:   ISO 8601 formatted string
    :rtype:    str

    """
    return dt.isoformat()


# register encoder for datetime.datetime
register_encoder(datetime.datetime, isoformat)
# register encoder for decimal.Decimal
register_encoder(D, float)


def dump(obj, fp, **kwargs):
    """
    Serialize ``obj`` as a JSON formatted stream to ``fp``
    (a ``.write()``-supporting file-like object)

    Provides defaults for ``kwargs['cls']`` and ``kwargs['ensure_ascii']``
    in case they are not specified

    :param obj: object to encode
    :param fp:  file-like object to write JSON formatted stream to
    :param kwargs[cls]: (optional) encoder class, defaults to
                        :class:`JSONEncoder <jukoro.json.JSONEncoder>`
    :param kwargs[ensure_ascii]: (optional) defaults to False

    """
    kwargs = _patch_encoder_kwargs(**kwargs)
    return _json.dump(obj, fp, **kwargs)


def dumps(obj, **kwargs):
    """
    Serialize ``obj`` to JSON formatted ``str``

    Provides defaults for ``kwargs['cls']`` and ``kwargs['ensure_ascii']``
    in case they are not specified

    :param obj: object to encode
    :param kwargs[cls]: (optional) encoder class, defaults to
                        :class:`JSONEncoder <jukoro.json.JSONEncoder>`
    :param kwargs[ensure_ascii]: (optional) defaults to False
    :return: encoded ``obj``
    :rtype: ``str`` or ``unicode``

    """
    kwargs = _patch_encoder_kwargs(**kwargs)
    return _json.dumps(obj, **kwargs)


def load(fp, **kwargs):
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing
    a JSON document)

    Provides default for ``kwargs['cls']`` in case it's not specified

    :param fp:  file-like object to read JSON formatted stream from
    :param kwargs[cls]: (optional) decoder class, defaults to
                        :class:`JSONDecoder <jukoro.json.JSONDecoder>`
    :return: Python object

    """
    kwargs = _patch_decoder_kwargs(**kwargs)
    return _json.load(fp, **kwargs)


def loads(s, **kwargs):
    """
    Deserialize ``s`` (a ``str`` or ``unicode`` instance containing a JSON
    document)

    Provides default for ``kwargs['cls']`` in case it's not specified

    :param s:  ``str`` or ``unicode`` instance to decode JSON from
    :param kwargs[cls]: (optional) decoder class, defaults to
                        :class:`JSONDecoder <jukoro.json.JSONDecoder>`
    :return: Python object

    """
    kwargs = _patch_decoder_kwargs(**kwargs)
    return _json.loads(s, **kwargs)


def _patch_encoder_kwargs(**kwargs):
    """ Helper to set encoder ``kwargs`` defaults """
    kwargs.setdefault('cls', JSONEncoder)
    kwargs.setdefault('ensure_ascii', False)
    return kwargs


def _patch_decoder_kwargs(**kwargs):
    """ Helper to set decoder ``kwargs`` defaults """
    kwargs.setdefault('cls', JSONDecoder)
    return kwargs
