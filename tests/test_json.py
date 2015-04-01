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

    def test_arrow(self):
        utcnow = arrow.utcnow()
        now = arrow.now()
        a = {
            'a': utcnow,
            'b': now,
        }
        jsoned = json.dumps(a)
        b = json.loads(jsoned)
        self.assertEqual(utcnow, arrow.get(b['a']))
        self.assertEqual(utcnow.to('local'), arrow.get(b['a']))
        self.assertEqual(now, arrow.get(b['b']))
        self.assertEqual(now.to('UTC'), arrow.get(b['b']))

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
            'e': pg.AbstractUser(123)
        }

        d = json.loads(json.dumps(c))
        self.assertEqual(c['e'].entity_id, d['e'])
