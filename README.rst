
About
=====

An experimental library to work with PostgreSQL and Redis and to have several
typical wrappers around ``json``, ``cPickle`` and others.


Motivation
==========

Curiosity.


Installation
============

.. code-block:: bash

    $  pip install git+https://github.com/ysegorov/jukoro.git@0.1.0#egg=jukoro


Documentation
=============

Is available `here <https://ysegorov.github.io/jukoro>`_.


Packages and modules
====================

- :mod:`jukoro.pg` - package to work with PostgreSQL (see below for a bit of
  explanation about tables structure) built on top of ``psycopg2``
- :mod:`jukoro.redis` - package to work with Redis having several useful
  helpers
- :mod:`jukoro.arrow` - package built on top of ``arrow`` library
- :mod:`jukoro.decorators` - module containing some useful decorators
- :mod:`jukoro.json` - abstraction built on top of Python's stdlib ``json``
- :mod:`jukoro.pickle` - abstraction built on top of Python's stdlib
  ``cPickle``
- :mod:`jukoro.base32` - abstraction built on top of ``base32_crockford``
  library
- :mod:`jukoro.email` - email helpers
- :mod:`jukoro.structures` - some useful structures
- :mod:`jukoro.utils` - utilities


Concept
=======

Intention is the following:

- to support schemas
- to have unified tables structure in PostgreSQL storing data in a single per
  table jsonb_ field
- to have single per-database sequence generator for objects identifiers
- to have out-of-the-box way to keep historical data in tables
- to have out-of-the-box support for created/updated/deleted timestamps
- to support indices and constraints for data stored in jsonb_ field
- to always operate with timestamps in UTC

.. _jsonb: http://www.postgresql.org/docs/9.4/static/datatype-json.html


Terminology
===========

:class:`Entity <jukoro.pg.entity.AbstractEntity>` is a term describing
some data type abstraction stored in a separate table in PostgreSQL
(to some extent is an equivalent to Model).

:class:`Attribute <jukoro.pg.attrs.Attr>` is a term describing some
``Entity``'s attribute (attribute can be indexable and can have constraints).


PostgreSQL tables structure
===========================

Please pay attention to :mod:`jukoro.pg.storage` module to understand
underlying tables structure.

There is one master per schema table called ``entity`` defined as:

.. code-block:: sql

    -- basic entity table (for inheritance)
    CREATE TABLE IF NOT EXISTS "entity" (
        "id" serial PRIMARY KEY,
        "entity_id" bigint NOT NULL DEFAULT nextval('public.global_entity_id_seq'),
        "entity_start" timestamp with time zone DEFAULT current_timestamp,
        "entity_end" timestamp with time zone
                DEFAULT '2999-12-31 23:59:59.999+0'::timestamp with time zone,
        "doc" jsonb NOT NULL
    );

This table will never be touched for CRUD operations and supposed to be empty.

Every :func:`registered <jukoro.pg.storage.register>` ``Entity`` described
like

.. code-block:: python

    class CoolEntity(AbstractEntity):
        db_table = 'cool_entity'


will have table defined as

.. code-block:: sql

    -- cool_entity table
    CREATE TABLE IF NOT EXISTS "cool_entity" (
        "id" serial PRIMARY KEY
    ) INHERITS ("entity");

    CREATE INDEX ju_idx__cool_entity__doc ON "cool_entity"
        USING GIN("doc" jsonb_path_ops);
    CREATE INDEX ju_idx__cool_entity__entity_id ON "cool_entity"
        USING btree("entity_id", "entity_start", "entity_end" DESC);

and view defined as

.. code-block:: sql

    -- cool_entity master view
    CREATE OR REPLACE VIEW "cool_entity__live" AS SELECT * FROM "cool_entity"
        WHERE "entity_start" <= now() AND "entity_end" > now();


.. note:: There is no need to manually register :class:`Entity
    <jukoro.pg.entity.AbstractEntity>`-derived class except for very special
    cases. By default every class definition having ``db_table`` attribute
    defined will be registered (see :class:`it's metaclass
    <jukoro.pg.entity.EntityMeta>`).

In general case all CRUD operations will go to ``__live`` table view as it is
updatable_.

.. _updatable: http://www.postgresql.org/docs/9.3/static/sql-createview.html

There is currently no way to get historical data for ``Entity`` but this is
just a very first release of this library. For sure there will be a way to work
with history.

Please check `jukoro's unannotated tests
<https://github.com/ysegorov/jukoro/tree/master/tests>`_ for code examples.


Tests
=====

For ``jukoro.redis`` tests it is expected Redis to be running locally
on standard port (``redis://localhost:6379``).

For ``jukoro.pg`` tests you will have to create PostgreSQL database named
``jukoro_test`` or specify db connection uri using ``PG_URI`` environment
variable.

To perform ``jukoro.pg`` tests a separate time-stamped schema prefixed with
``ju_`` will be created (something like ``ju_20150330124323``).

Install ``nose`` first::

    $ pip install nose

Run tests then::

    $ nosetests

or::

    $ PG_URI="postgresql://username:userpass@localhost:5432/test_db" nosetests


You can keep PostgreSQL test db schema created during tests with
``KEEP_SCHEMA`` environment variable::

    $ KEEP_SCHEMA=y nosetests


Docs generation
===============

For proper ``ipython`` code blocks highlights within documentation run:

.. code-block:: bash

    $ pip install sphinx ipython jsonschema mistune


Contributions
=============

Always welcomed.


License
=======

MIT
