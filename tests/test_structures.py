# -*- coding: utf-8 -*-

from unittest import TestCase

from jukoro.structures import ObjectDict, DefaultObjectDict, LockRing


class TestStructures(TestCase):

    def test_object_dict(self):
        a = ObjectDict(a=12, b=24, c=54)
        b = ObjectDict(a=1, b=2)

        self.assertEqual(a.a, 12)
        self.assertEqual(a.b, 24)
        self.assertRaises(AttributeError, lambda: a.d)

        self.assertRaises(AttributeError, lambda: b.attr)
        b.attr = 'ok'
        self.assertEqual(getattr(b, 'attr'), 'ok')
        setattr(b, 'attr', 'a')
        self.assertEqual(b.attr, 'a')
        delattr(b, 'attr')
        self.assertRaises(AttributeError, lambda: getattr(b, 'attr'))

        self.assertIsInstance(a.copy(), ObjectDict)

    def test_default_object_dict(self):
        a = DefaultObjectDict(int)

        self.assertEqual(a.a, 0)
        a.b += 10
        self.assertEqual(a.b, 10)
        a.b += 10
        self.assertEqual(getattr(a, 'b'), 20)
        delattr(a, 'b')
        self.assertEqual(a.b, 0)

    def test_lock_ring(self):
        a, b, c = objs = object(), object(), object()
        r = LockRing()

        self.assertRaises(IndexError, lambda: r.next())
        r.push(a)
        r.push(b)
        r.push(c)
        self.assertEqual(len(r), len(objs))

        for __ in xrange(len(objs)):
            o = r.next()
            self.assertTrue(r.is_locked(o))

        self.assertRaises(IndexError, lambda: r.next())

        self.assertTrue(r.push(a))
        self.assertTrue(r.push(b))
        self.assertTrue(r.push(c))

        prev = None
        for __ in xrange(len(objs)):
            o = r.next()
            self.assertIsNot(prev, o)
            r.push(o)

        for __ in xrange(len(objs)):
            o = r.pop()

        self.assertRaises(IndexError, lambda: r.pop())
        self.assertRaises(IndexError, lambda: r.next())
