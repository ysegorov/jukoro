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

    def test_db_val(self):
        dt = arrow.utcnow()
        db_val = dt.db_val()

        self.assertIsInstance(db_val, basestring)
        self.assertIsInstance(arrow.from_db_val(db_val), arrow.JuArrow)
        self.assertEqual(arrow.from_db_val(db_val), dt)

    def test_json_val(self):
        dt = arrow.utcnow()
        json_val = dt.json_val()

        self.assertIsInstance(json_val, basestring)
        self.assertIsInstance(arrow.from_json_val(json_val), arrow.JuArrow)
        self.assertEqual(arrow.from_json_val(json_val), dt)
