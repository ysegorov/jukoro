# -*- coding: utf-8 -*-

from jukoro import pickle


def test():
    d = {'a': 12, 'b': 13}
    dumped = pickle.dumps(d)
    restored = pickle.loads(dumped)

    assert d == restored
