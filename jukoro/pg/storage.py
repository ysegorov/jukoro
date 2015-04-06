# -*- coding: utf-8 -*-

from collections import OrderedDict

from jukoro.pg.exceptions import AlreadyRegistered
from jukoro.pg.introspect import inspect


class SqlDescr(object):
    """
    Python descriptor acting as a factory for sql abstraction

    :param target:    :class:`~AbstractSql`-derived class

    """

    def __init__(self, target):
        self.target = target

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.target(instance)

    def __set__(self, instance, value):
        raise AttributeError('read only attribute')


class AbstractSql(object):
    """
    Abstraction to work with some typical sql for :class:`Table`

    :param instance:  instance of :class:`Table`

    Must have ``query`` attribute defined with some sql template to render

    """
    query = None

    def __init__(self, instance):
        self.instance = instance

    @property
    def sql(self):
        """
        Renders ``query`` using variables

        :returns:   sql query
        :rtype:     str

        """
        return self.query.format(**self.sql_vars())

    def sql_vars(self, **extras):
        """
        Returns dictionary suitable to act as kwargs to "render" sql query

        :param extras:  keyword arguments to extend return dictionary with

        """
        instance = self.instance
        kwargs = {}
        for attr in ('db_table', 'db_view'):
            if hasattr(instance, attr):
                kwargs[attr] = (getattr(instance, attr)).name
        for k, v in extras.iteritems():
            kwargs[k] = v
        return kwargs


SEQ = 'global_entity_id_seq'  # for internal reference only
ET = 'entity'  # for internal reference only
# base table name intentionally hardcoded
INIT_SCHEMA = """
CREATE SCHEMA {schema};

-- CREATE EXTENSION IF NOT EXISTS btree_gin;

SET search_path TO {schema};

-- global (per db) id sequence
DO $$
BEGIN
    CREATE SEQUENCE public.global_entity_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
EXCEPTION WHEN duplicate_table THEN
    -- do nothing, it's already there
END $$;

-- json related functions
-- http://michael.otacoo.com/postgresql-2/manipulating-jsonb-data-with-key-unique/
CREATE OR REPLACE FUNCTION public.jsonb_merge(jsonb, jsonb)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    WITH ju AS
        (SELECT * FROM jsonb_each($1)
            UNION ALL
        SELECT * FROM jsonb_each($2))
     SELECT json_object_agg(key, value)::jsonb FROM ju;
$$;

CREATE OR REPLACE FUNCTION public.jsonb_merge_key_value_pairs(jsonb, variadic text[])
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT public.jsonb_merge($1, json_object($2)::jsonb);
$$;

-- convert current timestamp to iso 8601
CREATE OR REPLACE FUNCTION public.current_timestamp_to_iso8601()
RETURNS text
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT to_char(CURRENT_TIMESTAMP at TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
$$;

-- basic entity table (for inheritance)
CREATE TABLE IF NOT EXISTS "entity" (
    "id" serial PRIMARY KEY,
    "entity_id" bigint NOT NULL DEFAULT nextval('public.global_entity_id_seq'),
    "entity_start" timestamp with time zone DEFAULT current_timestamp,
    "entity_end" timestamp with time zone
            DEFAULT '2999-12-31 23:59:59.999+0'::timestamp with time zone,
    "doc" jsonb NOT NULL
);
"""

CREATE_TABLE = """
-- {db_table} table
CREATE TABLE IF NOT EXISTS "{db_table}" (
    "id" serial PRIMARY KEY
) INHERITS ("entity");

CREATE INDEX ju_idx__{db_table}__doc ON {db_table}
    USING GIN("doc" jsonb_path_ops);
-- CREATE INDEX ju_idx__{db_table}__doc_entity_start_entity_end ON {db_table}
--     USING GIN("doc" jsonb_path_ops, "entity_start", "entity_end");
CREATE INDEX ju_idx__{db_table}__entity_id ON "{db_table}"
    USING btree("entity_id", "entity_start", "entity_end" DESC);
"""


class CreateTableSql(AbstractSql):
    """
    Abstraction for "create table" sql query

    :param instance:  instance of :class:`Table`

    """
    query = CREATE_TABLE

    @property
    def name(self):
        """
        Returns table name (needed to check if table exists in database)

        """
        return self.instance.db_table.name


CREATE_VIEW = """
-- {db_table} master view
CREATE OR REPLACE VIEW "{db_view}" AS SELECT * FROM "{db_table}"
    WHERE "entity_start" <= now() AND "entity_end" > now();
"""


class CreateViewSql(AbstractSql):
    """
    Abstraction for "create view" sql query

    :param instance:  instance of :class:`Table`

    """
    query = CREATE_VIEW

    @property
    def name(self):
        """
        Returns view name (needed to check if view exists in database)

        """
        return self.instance.db_view.name


class AbstractTrigger(AbstractSql):
    """
    Abstraction for "create trigger" sql query

    :param instance:  instance of :class:`Table`

    Must have ``suffix`` attribute defined

    """

    @property
    def name(self):
        """
        Returns trigger name (needed to check if trigger exists in database)

        """
        instance = self.instance
        return 'ju_before__{}__{}'.format(instance.db_view.name, self.suffix)

    def sql_vars(self):
        """
        Returns dictionary suitable to act as kwargs to "render" sql query

        """
        return super(AbstractTrigger, self).sql_vars(name=self.name,
                                                     suffix=self.suffix)


TRIGGER_INSERT = """
-- {db_view} trigger on insert
CREATE OR REPLACE FUNCTION {name}() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO "{db_table}" ("doc") VALUES
        (public.jsonb_merge_key_value_pairs(
         NEW.doc, '_created', public.current_timestamp_to_iso8601(),
                  '_updated', public.current_timestamp_to_iso8601()))
        RETURNING * INTO NEW;

    -- RAISE WARNING 'inserted %', NEW;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "{name}"
    INSTEAD OF INSERT
    ON "{db_view}"
    FOR EACH ROW
    EXECUTE PROCEDURE {name}();

"""


class TriggerOnInsertSql(AbstractTrigger):
    """
    Abstraction for "create trigger instead of insert" sql query

    """
    query = TRIGGER_INSERT
    suffix = 'insert'


TRIGGER_UPDATE = """
-- {db_view} trigger on update
CREATE OR REPLACE FUNCTION {name}() RETURNS TRIGGER AS $$
BEGIN
    UPDATE "{db_table}" SET "entity_end" = CURRENT_TIMESTAMP
        WHERE "id" = OLD.id;
    INSERT INTO "{db_table}" ("entity_id", "doc")
        VALUES (NEW.entity_id,
                public.jsonb_merge_key_value_pairs(
                    NEW.doc,
                    '_updated',
                    public.current_timestamp_to_iso8601()))
        RETURNING * INTO NEW;

    -- RAISE WARNING 'updated %', NEW;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "{name}"
    INSTEAD OF UPDATE
    ON "{db_view}"
    FOR EACH ROW
    EXECUTE PROCEDURE {name}();

"""


class TriggerOnUpdateSql(AbstractTrigger):
    """
    Abstraction for "create trigger instead of update" sql query

    """
    query = TRIGGER_UPDATE
    suffix = 'update'


TRIGGER_DELETE = """
-- {db_view} trigger on delete
CREATE OR REPLACE FUNCTION {name}() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id IS NOT NULL THEN
        UPDATE "{db_table}"
            SET ("entity_end", "doc") = (
                CURRENT_TIMESTAMP,
                public.jsonb_merge_key_value_pairs(
                    OLD.doc,
                    '_deleted',
                    public.current_timestamp_to_iso8601()))
            WHERE "id" = OLD.id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "{name}"
    INSTEAD OF DELETE
    ON "{db_view}"
    FOR EACH ROW
    EXECUTE PROCEDURE {name}();
"""


class TriggerOnDeleteSql(AbstractTrigger):
    """
    Abstraction for "create trigger instead of delete" sql query

    """
    query = TRIGGER_DELETE
    suffix = 'delete'


class Schema(object):
    """
    Abstraction to "render" sql to create schema in database

    :param schema:  schema name

    """

    def __init__(self, schema):
        self._schema = schema

    @property
    def sql(self):
        """
        Renders sql to create schema

        :returns:   sql
        :rtype:     str

        """
        return INIT_SCHEMA.format(schema=self._schema)


class Table(object):
    """
    Abstraction to work with sql queries for table describing
    :class:`Entity <jukoro.pg.entity.AbstractEntity>`

    :param entity_class:    ``Entity``-derived class

    """
    #: Attribute to "render" sql to create database table
    sql_create_table = SqlDescr(CreateTableSql)
    #: Attribute to "render" sql to create database *live* view
    sql_create_view = SqlDescr(CreateViewSql)
    #: Attribute to "render" sql to create view's trigger on insert
    sql_trigger_on_insert = SqlDescr(TriggerOnInsertSql)
    #: Attribute to "render" sql to create view's trigger on update
    sql_trigger_on_update = SqlDescr(TriggerOnUpdateSql)
    #: Attribute to "render" sql to create view's trigger on delete
    sql_trigger_on_delete = SqlDescr(TriggerOnDeleteSql)

    #: Attribute replicating :class:`~jukoro.pg.introspect.State`
    #: available attributes
    state_values = ('tables', 'views', 'triggers', 'indices', 'constraints')

    def __init__(self, entity_class):
        self._entity_class = entity_class

    @property
    def eclass(self):
        """
        Returns :class:`Entity <jukoro.pg.entity.AbstractEntity>` class

        """
        return self._entity_class

    @property
    def db_table(self):
        """
        Returns table name defined for
        :class:`Entity <jukoro.pg.entity.AbstractEntity>` class

        :rtype:     str

        """
        return self._entity_class.db_table

    @property
    def db_view(self):
        """
        Returns view name defined for
        :class:`Entity <jukoro.pg.entity.AbstractEntity>` class

        :rtype:     str

        """
        return self._entity_class.db_view

    @property
    def tables(self):
        """
        Iterates over abstraction to create table sql

        :yields:    own attribute (Python descriptor) to generate sql query

        """
        yield self.sql_create_table

    @property
    def views(self):
        """
        Iterates over abstraction to create view sql

        :yields:    own attribute (Python descriptor) to generate sql query

        """
        yield self.sql_create_view

    @property
    def triggers(self):
        """
        Iterates over abstractions to create triggers sql

        :yields:    own attribute (Python descriptor) to generate sql query

        """
        for nm in ('sql_trigger_on_insert',
                   'sql_trigger_on_update',
                   'sql_trigger_on_delete'):
            yield getattr(self, nm)

    @property
    def indices(self):
        """
        Iterates over abstractions to create indices sql

        :yields:    own attribute (Python descriptor) to generate sql query

        """
        for attr in self._entity_class.attrs:
            if attr.db_index:
                yield Index(self, attr)

    @property
    def constraints(self):
        """
        Iterates over abstractions to create constraints sql

        :yields:    own attribute (Python descriptor) to generate sql query

        """
        for attr in self._entity_class.attrs:
            if attr.db_not_null:
                yield Constraint(self, attr)

    @property
    def items(self):
        """
        Iterates over ``.state_values`` to walk over own attributes

        :yields:    tuple(attribute_name, attribute)

        """
        for name in self.state_values:
            yield name, getattr(self, name)


INDEX = """
CREATE INDEX {index_name} ON
    "{db_table}" USING btree({spec}, "entity_start", "entity_end" DESC);
"""


class Index(object):
    """
    Abstraction to create index sql query for entity attribute

    :param table:   instance of :class:`~jukoro.pg.storage.Table`
    :param attr:    instance of :class:`~jukoro.pg.attrs.AttrDescr`

    """

    def __init__(self, table, attr):
        self._table = table
        self._attr = attr

    @property
    def db_table(self):
        """
        Returns table name

        :rtype: str

        """
        return self._table.db_table

    @property
    def name(self):
        """
        Returns index name

        :rtype: str

        """
        return 'ju_idx__{}__{}_entity_start_entity_end'.format(
            self.db_table.name, self._attr.slug)

    @property
    def spec(self):
        """
        Returns index spec according to attribute definition

        :rtype: str

        """
        attr = self._attr
        spec = '(("doc"->>\'{attr}\')::{cast})'
        return spec.format(attr=attr.slug, cast=attr.db_cast)

    def sql_vars(self):
        """
        Returns dictionary with key/value pairs suitable for sql
        template for index

        :rtype: dict

        """
        return {
            'index_name': self.name,
            'spec': self.spec,
            'db_table': self.db_table.name,
        }

    @property
    def sql(self):
        """
        Returns "rendered" sql query to create constraint

        :rtype: str

        """
        return INDEX.format(**self.sql_vars())


CONSTRAINT_INT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("doc"->>'{attr}') IS NOT NULL
    AND ("doc"->>'{attr}')::BIGINT >= 0);
"""
CONSTRAINT_TEXT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("doc"->>'{attr}') IS NOT NULL
    AND length("doc"->>'{attr}') > {minlen});
"""
CONSTRAINT_NOT_NULL = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("doc"->>'{attr}') IS NOT NULL);
"""


class Constraint(object):
    """
    Abstraction to create constraint sql query for entity attribute

    :param table:   instance of :class:`~jukoro.pg.storage.Table`
    :param attr:    instance of :class:`~jukoro.pg.attrs.AttrDescr`

    """

    def __init__(self, table, attr):
        self._table = table
        self._attr = attr

    @property
    def db_table(self):
        """
        Returns table name

        :rtype: str

        """
        return self._table.db_table

    @property
    def name(self):
        """
        Returns constraint name

        :rtype: str

        """
        return 'ju_validate__{}__{}'.format(self.db_table.name,
                                            self._attr.slug)

    def sql_vars(self):
        """
        Returns dictionary with key/value pairs suitable for sql
        template for constraint

        :rtype: dict

        """
        return {
            'constraint_name': self.name,
            'attr': self._attr.slug,
            'db_table': self.db_table.name,
            'minlen': self._attr.minlen,
        }

    def sql_tmpl(self):
        """
        Returns sql template for constraint according to attribute type

        :rtype: str

        """
        if self._attr.is_int:
            return CONSTRAINT_INT
        if self._attr.is_text:
            return CONSTRAINT_TEXT
        return CONSTRAINT_NOT_NULL

    @property
    def sql(self):
        """
        Returns "rendered" sql query to create constraint

        :rtype: str

        """
        return self.sql_tmpl().format(**self.sql_vars())


class DBTableName(object):
    """
    Python descriptor to return database table name defined for
    :class:`Entity <jukoro.pg.entity.AbstractEntity>`

    :param name:    database table name

    """

    def __init__(self, name):
        self._nm = name

    def __get__(self, instance, owner):
        return self

    def __set__(self, instance):
        raise AttributeError

    @property
    def name(self):
        """
        Returns stored name

        :rtype: str

        """
        return self._nm

    # TODO
    # @property
    # def fields(self):
    #     return ('id', 'entity_id', 'entity_start', 'entity_end', 'doc')


class DBViewName(DBTableName):
    """
    Python descriptor to return database view name (view built on top of
    table to select only "live" entities) defined for
    :class:`Entity <jukoro.pg.entity.AbstractEntity>`

    :param name:    database table name

    """
    suffix = '__live'

    def __init__(self, name):
        self._nm = '{}{}'.format(name, self.suffix)

    # TODO
    # @property
    # def fields(self):
    #     return ('entity_id', 'doc')


# registry to keep AbstractEntity-derived classes having db_table attribute
# defined
_registry = OrderedDict()


def register(entity_class):
    """
    Registers :class:`Entity <jukoro.pg.entity.AbstractEntity>`-derived
    classes having :attr:`~jukoro.pg.entity.AbstractEntity.db_table` defined
    within internal registry

    Stores ``entity_class`` wrapped in :class:`~jukoro.pg.storage.Table`
    instance

    :param entity_class:    class to register

    """
    tn = entity_class.db_table.name
    if tn in _registry:
        raise AlreadyRegistered(
            'Model for "%s" already registered' % tn)
    _registry[tn] = Table(entity_class)


def unregister(entity_class):
    """
    Unregisters previously registered
    :class:`Entity <jukoro.pg.entity.AbstractEntity>`-derived class

    :param entity_class:    class to unregister

    """
    if entity_class.db_table:
        _registry.pop(entity_class.db_table.name, None)


def is_registered(entity_class):
    """
    Tests if :class:`Entity <jukoro.pg.entity.AbstractEntity>`-derived
    class is registered

    :param entity_class:    class to check
    :returns:               boolean indicating current state

    """
    if hasattr(entity_class, 'db_table'):
        return entity_class.db_table and \
            entity_class.db_table.name in _registry
    return False


def tables():
    """
    Iterates over registered within registry tables - instances of
    :class:`~jukoro.pg.storage.Table`

    :yields:    :class:`~jukoro.pg.storage.Table`

    """
    for table in _registry.itervalues():
        yield table


def _to_create(table, state):
    """
    Iterates over sql queries to create missing
    structures/triggers/indices/constraints for
    :class:`~jukoro.pg.storage.Table` specified

    :param table:   :class:`~jukoro.pg.storage.Table` instance
    :param state:   :class:`~jukoro.pg.introspect.State` instance

    :yields:        sql query

    """

    for name, items in table.items:
        current = getattr(state, name)
        for it in items:
            if it.name not in current:
                yield it.sql
            else:
                current.pop(it.name)


DROPS = {
    'indices': 'DROP INDEX IF EXISTS {name};',
    'tables': 'DROP TABLE "{name}" CASCADE;',
    'views': 'DROP VIEW IF EXISTS "{name}";',
    'constraints': 'ALTER TABLE "{db_table}" DROP CONSTRAINT IF EXISTS {name};'
}


def _to_drop(state):
    """
    Iterates over sql queries to drop orphaned
    structures/triggers/indices/constraints according to
    :class:`~jukoro.pg.introspect.State` state

    :param state:   :class:`~jukoro.pg.introspect.State` instance

    :yields:        sql query

    """

    for name in reversed(Table.state_values):
        if name == 'triggers':
            continue
        items = getattr(state, name)
        sql = DROPS[name]
        for it in items:
            if name in ('indices', 'constraints') and not it.startswith('ju'):
                continue
            tn = ''
            if name == 'constraints':
                tn = '__'.join(it.split('__')[1:-1])
            yield sql.format(db_table=tn, name=it) + '\n'


def _syncdb(uri):
    """
    Introspects database and transforms current
    :class:`~jukoro.pg.introspect.State` to sql queries

    Iterates over sql to create and sql to drop splitting them
    yielding **CUT** word

    :param uri:     connection string

    :yields:        sql query to create, **CUT**, sql query to drop

    """

    schema_name, state = inspect(uri)

    if schema_name not in state.schemas:
        schema = Schema(schema_name)
        yield schema.sql

    state.schemas.clear()
    state.sequences.clear()  # TODO
    state.tables.pop(ET)

    for table in tables():
        for sql in _to_create(table, state):
            yield sql

    yield 'CUT'

    for sql in _to_drop(state):
        yield sql


def syncdb(uri):
    """
    Generates sql queries to synchronize database state and registered
    entities declarations

    :param uri:     connection string
    :returns:       pair of sql queries - to create missed
                    structures/procedures/triggers/etc. and to drop orphaned
    :rtype:         tuple

    To generate proper queries introspects database state using
    :func:`jukoro.pg.introspect.inspect` function

    """
    proc = _syncdb(uri)
    create_sql = list(iter(lambda: proc.next(), 'CUT'))
    drop_sql = list(proc)
    create_sql = ''.join(create_sql)
    drop_sql = ''.join(drop_sql)
    return create_sql, drop_sql
