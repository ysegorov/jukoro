# -*- coding: utf-8 -*-

import datetime
import logging

from unittest import TestCase

from jukoro import arrow


logger = logging.getLogger(__name__)


class TestArrow(TestCase):

    def test_utcnow(self):
        dt = arrow.utcnow()
        iso = dt.isoformat()

        self.assertIsInstance(dt, arrow.JuArrow)
        self.assertTrue('+' in iso)
        self.assertTrue('+00:00' in iso)

    def test_now(self):
        dt = arrow.now()

        self.assertIsInstance(dt, arrow.JuArrow)

    def test_get(self):
        raw = datetime.datetime.utcnow()
        iso = '2015-03-26T07:59:44.210642+00:00'

        dt = arrow.get(raw)
        self.assertIsInstance(dt, arrow.JuArrow)

        dt = arrow.get(iso)
        self.assertIsInstance(dt, arrow.JuArrow)
        self.assertEqual(dt.isoformat(), iso)
