==========
Change Log
==========

All notable changes to this project will be documented in this file.
This project adheres to `Semantic Versioning <http://semver.org/>`_.

[Unreleased] - YYYY-MM-DD
=========================


[0.1.2] - 2015-04-06
====================

Fixed
-----

- documentation fixes


[0.1.0] - 2015-04-06
====================

Added
-----

- ``jukoro.arrow`` - wrapper around ``arrow.Arrow`` from `arrow library`_
- ``jukoro.base32`` - wrapper around ``base32_crockford``
- ``jukoro.decorators`` - ``coroutine``, ``profile``, ``memoize``,
  ``raise_if`` decorators
- ``jukoro.email`` - ``send_email`` helper based on
  `this <http://stackoverflow.com/a/3363254>`_
- ``jukoro.json`` - wrapper around Python stdlib ``json``
- ``jukoro.pickle`` - wrapper around Python stdlib ``cPickle``
- ``jukoro.pg`` - ``psycopg2``-based module to work with PostgreSQL
- ``jukoro.redis`` - ``RedisDb``, ``RedisCache``, ``RedisLock`` and
  ``RedisQueue``
- ``jukoro.structures`` - ``ObjectDict``, ``DefaultObjectDict`` and
  ``LockRing``

.. _arrow library: https://github.com/crsmithdev/arrow
