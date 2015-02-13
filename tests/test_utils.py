# -*- coding: utf-8

from unittest import TestCase

from jukoro.structures import ObjectDict
from jukoro.utils import mergedicts


class TestMergeDicts(TestCase):

    def test_simple(self):
        a, b = {'a': 12}, {'b': 14}
        c = mergedicts(a, b)

        self.assertIsNot(c, a)
        self.assertIsNot(c, b)
        self.assertTrue('a' in c)
        self.assertTrue('b' in c)
        self.assertEqual(a['a'], c['a'])
        self.assertEqual(b['b'], c['b'])

    def test_list(self):
        a, b = {'a': 12}, {'b': [1, 2, 3]}
        c = mergedicts(a, b)

        self.assertIsNot(c['b'], b['b'])
        self.assertEqual(c['b'], b['b'])

    def test_list2(self):
        a, b = {'a': 12, 'b': [3, 2, 1]}, {'b': [1, 2, 3]}
        c = mergedicts(a, b)

        self.assertIsNot(c['b'], b['b'])
        self.assertEqual(c['b'], b['b'])
        self.assertNotEqual(c['b'], a['b'])

    def test_list3(self):
        self.skipTest('pending: list members deep copy?')

        a, b = {'a': 12, 'b': [3, 2, 1]}, {'b': [object(), 2, 3]}
        c = mergedicts(a, b)

        self.assertIsNot(c['b'], b['b'])
        self.assertIsNot(c['b'][0], b['b'][0])

    def test_object_dict(self):
        a, b = ObjectDict({'a': 12}), {'b': [1, 2, 3]}
        c = mergedicts(a, b)

        self.assertIsInstance(c, ObjectDict)
        self.assertEqual(c.a, a.a)
        self.assertEqual(c.b, b['b'])

    def test_object_dict2(self):
        a, b = {'a': 12}, ObjectDict({'b': 1})
        c = mergedicts(a, b)

        self.assertIsInstance(c, ObjectDict)
        self.assertEqual(c.a, a['a'])
        self.assertEqual(c.b, b.b)

    def test_nested(self):
        a, b = {'a': 1, 'b': {'c': 3}}, {'b': {'d': 4}}
        c = mergedicts(a, b)

        self.assertIsInstance(c['b'], dict)
        self.assertEqual(c['b'], {'c': 3, 'd': 4})
        self.assertIsNot(c['b'], a['b'])
        self.assertIsNot(c['b'], b['b'])

    def test_nested_object_dict(self):
        a, b = {'a': 1, 'b': {'c': 3}}, {'b': ObjectDict({'d': 4})}
        c = mergedicts(a, b)

        self.assertIsInstance(c['b'], ObjectDict)
        self.assertEqual(c['b'], {'c': 3, 'd': 4})
        self.assertIsNot(c['b'], a['b'])
        self.assertIsNot(c['b'], b['b'])

    def test_nested_object_dict2(self):
        a, b = {'a': 1, 'b': {'c': 3}}, ObjectDict({'b': ObjectDict({'d': 4})})
        c = mergedicts(a, b)

        self.assertIsInstance(c['b'], ObjectDict)
        self.assertEqual(c.b, {'c': 3, 'd': 4})
        self.assertIsNot(c.b, a['b'])
        self.assertIsNot(c.b, b['b'])
