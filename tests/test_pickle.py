# -*- coding: utf-8 -*-

from unittest import TestCase

from jukoro import pickle
from jukoro.structures import ObjectDict


class TestPickle(TestCase):

    def test_dict(self):
        a = {'a': 12, 'b': 13}
        b = pickle.dumps(a)
        c = pickle.loads(b)

        self.assertEqual(c, a)

    def test_object_dict(self):
        a = ObjectDict(a=12, b=24)
        b = pickle.dumps(a)
        c = pickle.loads(b)

        self.assertIsInstance(c, ObjectDict)
        self.assertEqual(c, a)
