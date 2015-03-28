# -*- coding: utf-8 -*-

from __future__ import absolute_import

import arrow


PRECISION = 10 ** 6


class JuArrow(arrow.Arrow):

    def db_val(self):
        return str(int(self.float_timestamp * PRECISION))

    def json_val(self):
        return self.isoformat()
