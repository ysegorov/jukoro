# -*- coding: utf-8 -*-

from unittest import TestCase

from jukoro import base32 as b32


class TestBase32(TestCase):

    def test_a(self):
        for x in (1, -23, 145, -76390, 60 * 60 * 24 * 365):
            a = x
            b = b32.decode(b32.encode(a))
            self.assertEqual(a, b)
            if a < 0:
                self.assertEqual(b32.encode(a)[:2], b32.PREFIX)

    def test_b(self):
        self.assertRaises(AssertionError, lambda: b32.encode('a'))
        self.assertRaises(AssertionError, lambda: b32.encode({}))
        self.assertRaises(AssertionError, lambda: b32.encode([]))
        self.assertRaises(AssertionError, lambda: b32.encode(1.2))

    def test_c(self):
        self.assertRaises(AssertionError, lambda: b32.decode(12))
        self.assertRaises(AssertionError, lambda: b32.decode({}))
        self.assertRaises(AssertionError, lambda: b32.decode([]))
        self.assertRaises(AssertionError, lambda: b32.decode(set()))
        self.assertRaises(AssertionError, lambda: b32.decode(1.2))
