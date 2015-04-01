# -*- coding: utf-8 -*-
"""
Module to work with datetimes using :class:`JuArrow <jukoro.arrow.JuArrow>`
derived from :class:`Arrow <arrow.Arrow>`

Usage examples::

    >>> from jukoro import arrow
    >>> arrow.utcnow()
    <JuArrow [2015-04-01T07:12:50.969849+00:00]>
    >>> arrow.utcnow().db_val()
    '1427872406346229'
    >>> arrow.utcnow().json_val()
    '2015-04-01T07:13:31.532397+00:00'
    >>> a = arrow.utcnow()
    >>> b = a.db_val()
    >>> a == arrow.from_db_val(b)
    True


"""

from __future__ import absolute_import

from .base import JuArrow
from .factory import get, now, utcnow, from_db_val, from_json_val
