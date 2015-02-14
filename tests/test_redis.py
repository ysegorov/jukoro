# -*- coding: utf-8 -*-

import threading
import time
import unittest

from jukoro import redis


URI = 'redis://localhost/2'
NS = 'JuTest'
TTL = 1


class Base(unittest.TestCase):
    db = None

    @classmethod
    def setUpClass(cls):
        cls.db = redis.RedisDb(URI, ns=NS)


class TestRedisDb(Base):

    def test_key(self):
        self.assertEqual(self.db.key('a'), 'JuTest:a')

    def test_set_get(self):
        db = self.db
        key, val = self.db.key('a'), 'TV'
        db.set(key, val)
        self.assertEqual(val, db.get(key))
        db.delete(key)
        self.assertNotEqual(val, db.get(key))

    def test_expire(self):
        db = self.db
        key, val = self.db.key('a'), 'TV'
        db.set(key, val, ex=TTL)
        self.assertEqual(val, db.get(key))
        time.sleep(TTL)
        self.assertNotEqual(val, db.get(key))


class TestRedisLock(Base):

    def test_no_wait(self):
        db = self.db
        key = 'write:lock'

        lock_a = redis.RedisLock(db, key)

        with lock_a:
            lock_b = redis.RedisLock(db, key)
            self.assertRaises(redis.AlreadyLocked, lock_b.acquire)

    def test_wait_ok(self):
        db = self.db
        key = 'wl'

        self.assertNotEqual(db.get(db.key('a')), '2015')

        lock = redis.RedisLock(db, key, ttl=2, wait=True)

        def lock_and_set():
            db = redis.RedisDb(URI, ns=NS)
            key = 'wl'
            with redis.RedisLock(db, key):
                db.set(db.key('a'), '2015')
                time.sleep(1.5)

        task = threading.Thread(target=lock_and_set)
        task.daemon = True
        task.start()

        time.sleep(.1)  # delay to allow thread to start

        with lock:
            v = db.get(db.key('a'))
        self.assertEqual(v, '2015')
        db.delete(db.key('a'))

    def test_wait_failed(self):
        db = self.db
        key = 'wl2'

        db.delete(db.key('b'))

        lock = redis.RedisLock(db, key, ttl=2, wait=True)

        def lock_and_set():
            db = redis.RedisDb(URI, ns=NS)
            key = 'wl2'
            with redis.RedisLock(db, key):
                db.set(db.key('b'), '2015')
                time.sleep(3.5)
            db.delete(db.key('b'))

        task = threading.Thread(target=lock_and_set)
        task.daemon = True
        task.start()

        time.sleep(.1)  # delay to allow thread to start

        def get_val():
            with lock:
                db.get(db.key('b'))
        self.assertRaises(redis.AlreadyLocked, get_val)


class TestRedisCache(Base):

    def test_ns(self):
        db = self.db
        cache = redis.RedisCache(db)
        self.assertTrue(cache.ns.startswith('JuTest:cache'))

    def test_cache(self):
        cache = redis.RedisCache(self.db)
        key = cache.key('a', 'b', 3)
        a = {'a': 12, 'b': 33}
        cache.set(key, a)
        b = cache.get(key)
        self.assertEqual(a, b)
        self.assertIsNot(a, b)
        cache.delete(key)
        self.assertIsNone(cache.get(key))

    def test_cache_expire(self):
        cache = redis.RedisCache(self.db, ttl=1)
        key = cache.key('a', 'b', 3)
        a = {'a': 12, 'b': 33}
        cache.set(key, a)
        time.sleep(1.1)
        self.assertIsNone(cache.get(key))

    def test_cache_expirei2(self):
        cache = redis.RedisCache(self.db)
        key = cache.key('a', 'b', 3)
        a = {'a': 12, 'b': 33}
        cache.set(key, a, ttl=1)
        self.assertEqual(a, cache.get(key))
        time.sleep(1.1)
        self.assertIsNone(cache.get(key))
