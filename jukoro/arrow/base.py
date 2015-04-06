# -*- coding: utf-8 -*-
"""
Wrapper :class:`JuArrow <jukoro.arrow.JuArrow>`
around :class:`Arrow <arrow.Arrow>`
to support :class:`JSONEncoder <jukoro.json.JSONEncoder>` api for serialization

"""

from __future__ import absolute_import

import arrow


PRECISION = 10 ** 6


class JuArrow(arrow.Arrow):
    """ Wrapper around :class:`Arrow <arrow.Arrow>` """

    def db_val(self):
        """
        Method to serialize instance value to integer to store in PostgreSQL

        """
        return str(int(self.float_timestamp * PRECISION))

    def json_val(self):
        """
        Method to serialize instance value to ISO 8601 string for
        server-client exchange

        """
        return self.isoformat()
