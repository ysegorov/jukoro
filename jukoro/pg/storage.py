# -*- coding: utf-8 -*-

from collections import OrderedDict

from jukoro.pg.exceptions import PgAlreadyRegisteredError
from jukoro.pg.introspect import inspect


class SqlDescr(object):

    def __init__(self, target):
        self.target = target

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.target(instance)

    def __set__(self, instance, value):
        raise AttributeError('read only attribute')


class BaseSql(object):
    query = None

    def __init__(self, instance):
        self.instance = instance

    @property
    def sql(self):
        return self.query.format(**self.sql_vars())

    def sql_vars(self, **extras):
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


class CreateTableSql(BaseSql):
    query = CREATE_TABLE

    @property
    def name(self):
        return self.instance.db_table.name


CREATE_VIEW = """
-- {db_table} master view
CREATE OR REPLACE VIEW "{db_view}" AS SELECT * FROM "{db_table}"
    WHERE "entity_start" <= now() AND "entity_end" > now();
"""


class CreateViewSql(BaseSql):
    query = CREATE_VIEW

    @property
    def name(self):
        return self.instance.db_view.name


class BaseTrigger(BaseSql):

    @property
    def name(self):
        instance = self.instance
        return 'ju_before__{}__{}'.format(instance.db_view.name, self.suffix)

    def sql_vars(self):
        return super(BaseTrigger, self).sql_vars(name=self.name,
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


class TriggerOnInsertSql(BaseTrigger):
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


class TriggerOnUpdateSql(BaseTrigger):
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


class TriggerOnDeleteSql(BaseTrigger):
    query = TRIGGER_DELETE
    suffix = 'delete'


class Schema(object):

    def __init__(self, schema):
        self._schema = schema

    @property
    def sql(self):
        return INIT_SCHEMA.format(schema=self.schema)

    @property
    def schema(self):
        return self._schema


class Table(object):
    sql_create_table = SqlDescr(CreateTableSql)
    sql_create_view = SqlDescr(CreateViewSql)
    sql_trigger_on_insert = SqlDescr(TriggerOnInsertSql)
    sql_trigger_on_update = SqlDescr(TriggerOnUpdateSql)
    sql_trigger_on_delete = SqlDescr(TriggerOnDeleteSql)

    state_values = ('tables', 'views', 'triggers', 'indices', 'constraints')

    def __init__(self, entity_class):
        self._entity_class = entity_class

    @property
    def eclass(self):
        return self._entity_class

    @property
    def db_table(self):
        return self._entity_class.db_table

    @property
    def db_view(self):
        return self._entity_class.db_view

    @property
    def tables(self):
        yield self.sql_create_table

    @property
    def views(self):
        yield self.sql_create_view

    @property
    def triggers(self):
        for nm in ('sql_trigger_on_insert',
                   'sql_trigger_on_update',
                   'sql_trigger_on_delete'):
            yield getattr(self, nm)

    @property
    def indices(self):
        for attr in self._entity_class.attrs:
            if attr.db_index:
                yield Index(self, attr)

    @property
    def constraints(self):
        for attr in self._entity_class.attrs:
            if attr.db_not_null:
                yield Constraint(self, attr)

    @property
    def items(self):
        for name in self.state_values:
            yield name, getattr(self, name)


INDEX = """
CREATE INDEX {index_name} ON
    "{db_table}" USING btree({spec}, "entity_start", "entity_end" DESC);
"""


class Index(object):

    def __init__(self, table, attr):
        self._table = table
        self._attr = attr

    @property
    def db_table(self):
        return self._table.db_table

    @property
    def name(self):
        return 'ju_idx__{}__{}_entity_start_entity_end'.format(
            self.db_table.name, self._attr.slug)

    @property
    def spec(self):
        attr = self._attr
        spec = '(("doc"->>\'{attr}\')::{cast})'
        return spec.format(attr=attr.slug, cast=attr.db_cast())

    def sql_vars(self):
        return {
            'index_name': self.name,
            'spec': self.spec,
            'db_table': self.db_table.name,
        }

    @property
    def sql(self):
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

    def __init__(self, table, attr):
        self._table = table
        self._attr = attr

    @property
    def db_table(self):
        return self._table.db_table

    @property
    def name(self):
        return 'ju_validate__{}__{}'.format(self.db_table.name,
                                            self._attr.slug)

    def sql_vars(self):
        return {
            'constraint_name': self.name,
            'attr': self._attr.slug,
            'db_table': self.db_table.name,
            'minlen': self._attr.minlen,
        }

    def sql_tmpl(self):
        if self._attr.is_int:
            return CONSTRAINT_INT
        if self._attr.is_text:
            return CONSTRAINT_TEXT
        return CONSTRAINT_NOT_NULL

    @property
    def sql(self):
        return self.sql_tmpl().format(**self.sql_vars())


class DBTableName(object):

    def __init__(self, name):
        self._nm = name

    def __get__(self, instance, owner):
        return self

    def __set__(self, instance):
        raise AttributeError

    @property
    def name(self):
        return self._nm

    # TODO
    # @property
    # def fields(self):
    #     return ('id', 'entity_id', 'entity_start', 'entity_end', 'doc')


class DBViewName(DBTableName):
    suffix = '__live'

    def __init__(self, name):
        self._nm = '{}{}'.format(name, self.suffix)

    # TODO
    # @property
    # def fields(self):
    #     return ('entity_id', 'doc')


_registry = OrderedDict()


def register(entity_class):
    tn = entity_class.db_table.name
    if tn in _registry:
        raise PgAlreadyRegisteredError(
            'Model for "%s" already registered' % tn)
    _registry[tn] = Table(entity_class)


def unregister(entity_class):
    if entity_class.db_table:
        _registry.pop(entity_class.db_table.name, None)


def is_registered(entity_class):
    if hasattr(entity_class, 'db_table'):
        return entity_class.db_table and \
            entity_class.db_table.name in _registry
    return False


def tables():
    for table in _registry.itervalues():
        yield table


def _to_create(table, state):

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
    proc = _syncdb(uri)
    create_sql = list(iter(lambda: proc.next(), 'CUT'))
    drop_sql = list(proc)
    create_sql = ''.join(create_sql)
    drop_sql = ''.join(drop_sql)
    return create_sql, drop_sql
