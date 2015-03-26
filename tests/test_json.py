# -*- coding: utf-8 -*-

import datetime
import decimal
from unittest import TestCase

from jukoro import arrow
from jukoro import json
from jukoro import pg


A = arrow.JuArrow
D = decimal.Decimal


class TestJson(TestCase):

    def setUp(self):
        self.entity_encoder = json.unregister_encoder_by_meta(
            type(pg.BaseEntity))

    def tearDown(self):
        self._register_encoders()

    def _register_encoders(self):
        json.register_encoder_by_meta(type(pg.BaseEntity), self.entity_encoder)

    def test_arrow(self):
        utcnow = arrow.utcnow()
        a = {
            'a': utcnow
        }
        jsoned = json.dumps(a)
        b = json.loads(jsoned)
        self.assertEqual(utcnow.isoformat(), b['a'])

    def test_dict(self):
        now = datetime.datetime.now()
        utcnow = datetime.datetime.utcnow()
        a = {
            'a': 12,
            'b': D('1.2'),
            'c': now,
            'd': utcnow,
        }
        jsoned = json.dumps(a)
        b = json.loads(jsoned)

        self.assertEqual(a['a'], b['a'])
        self.assertIsInstance(b['b'], D)
        self.assertEqual(a['b'], b['b'])
        self.assertEqual(a['c'].isoformat(), b['c'])
        self.assertEqual(a['d'].isoformat(), b['d'])

    def test_pg(self):
        c = {
            'e': pg.BaseUser(123)
        }
        with self.assertRaises(TypeError):
            json.dumps(c)

        self._register_encoders()

        d = json.loads(json.dumps(c))
        self.assertEqual(c['e'].entity_id, d['e'])
