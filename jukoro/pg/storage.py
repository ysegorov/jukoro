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

    def sql_vars(self):
        instance = self.instance
        kwargs = {}
        for attr in ('db_table', 'db_view'):
            if hasattr(instance, attr):
                kwargs[attr] = getattr(instance, attr)
        return kwargs


SEQ = 'global_entity_id_seq'  # for internal reference only
ET = 'entity'  # for internal reference only
# base table name intentionally hardcoded
INIT_SCHEMA = """
CREATE SCHEMA {schema};

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

-- basic entity table (for inheritance)
CREATE TABLE IF NOT EXISTS "entity" (
    "id" serial PRIMARY KEY,
    "entity_id" bigint NOT NULL DEFAULT nextval('public.global_entity_id_seq'),
    "entity_start" timestamp with time zone DEFAULT current_timestamp,
    "entity_end" timestamp with time zone
            DEFAULT '2999-12-31 23:59:59.999+0'::timestamp with time zone,
    "data" jsonb NOT NULL
);
"""

CREATE_TABLE = """
-- {db_table} table
CREATE TABLE IF NOT EXISTS "{db_table}" (
    "id" serial PRIMARY KEY
) INHERITS ("entity");
"""


class CreateTableSql(BaseSql):
    query = CREATE_TABLE

    @property
    def name(self):
        return self.instance.db_table


CREATE_VIEW = """
-- {db_table} master view
CREATE OR REPLACE VIEW "{db_view}" AS SELECT * FROM "{db_table}"
    WHERE "entity_start" <= now() AND "entity_end" > now();
"""


class CreateViewSql(BaseSql):
    query = CREATE_VIEW

    @property
    def name(self):
        return self.instance.db_view


class BaseTrigger(BaseSql):

    @property
    def trigger_proc_name(self):
        instance = self.instance
        return 'ju_before__{}__{}'.format(instance.db_table, self.suffix)

    @property
    def name(self):
        instance = self.instance
        return 'ju_before__{}__{}'.format(instance.db_table, self.suffix)

    def sql_vars(self):
        kwargs = super(BaseTrigger, self).sql_vars()
        kwargs.update({
            'trigger_name': self.name,
            'trigger_proc_name': self.trigger_proc_name
        })
        return kwargs


TRIGGER_INSERT = """
-- {db_table} trigger on insert
CREATE OR REPLACE FUNCTION {trigger_proc_name}() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.entity_id IS NOT NULL THEN
        UPDATE "{db_table}" SET "entity_end" = now()
            WHERE "entity_id" = NEW.entity_id AND "entity_end" > now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "{trigger_name}"
    BEFORE INSERT
    ON "{db_table}"
    FOR EACH ROW
    EXECUTE PROCEDURE {trigger_proc_name}();

"""


class TriggerOnInsertSql(BaseTrigger):
    query = TRIGGER_INSERT
    suffix = 'insert'


TRIGGER_DELETE = """
-- {db_table} trigger on delete
CREATE OR REPLACE FUNCTION {trigger_proc_name}() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.entity_id IS NOT NULL THEN
        UPDATE "{db_table}" SET "entity_end" = now()
            WHERE "entity_id" = OLD.entity_id AND "entity_end" > now();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "{trigger_name}"
    BEFORE DELETE
    ON "{db_table}"
    FOR EACH ROW
    EXECUTE PROCEDURE {trigger_proc_name}();
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
        for nm in ('sql_trigger_on_insert', 'sql_trigger_on_delete'):
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
            self.db_table, self._attr.slug)

    @property
    def spec(self):
        spec = '("data"->>\'{attr}\')'
        if self._attr.is_int:
            spec = '(%s::INTEGER)' % spec
        return spec.format(attr=self._attr.slug)

    def sql_vars(self):
        return {
            'index_name': self.name,
            'spec': self.spec,
            'db_table': self.db_table,
        }

    @property
    def sql(self):
        return INDEX.format(**self.sql_vars())


CONSTRAINT_INT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("data"->>'{attr}') IS NOT NULL
    AND ("data"->>'{attr}')::INTEGER >= 0);
"""
CONSTRAINT_TEXT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("data"->>'{attr}') IS NOT NULL
    AND length("data"->>'{attr}') > {minlen});
"""
CONSTRAINT_NOT_NULL = """
ALTER TABLE "{db_table}" ADD CONSTRAINT {constraint_name}
    CHECK (("data"->>'{attr}') IS NOT NULL);
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
        return 'ju_validate__{}__{}'.format(self.db_table,
                                            self._attr.slug)

    def sql_vars(self):
        return {
            'constraint_name': self.name,
            'attr': self._attr.slug,
            'db_table': self.db_table,
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
        return self._nm

    def __set__(self, instance):
        raise AttributeError


class DBViewName(DBTableName):
    suffix = '__live'

    def __init__(self, name):
        self._nm = '{}{}'.format(name, self.suffix)


_registry = OrderedDict()


def register(entity_class):
    tn = entity_class.db_table
    if tn in _registry:
        raise PgAlreadyRegisteredError(
            'Model for "%s" already registered' % tn)
    _registry[entity_class.db_table] = Table(entity_class)


def unregister(entity_class):
    if entity_class.db_table:
        _registry.pop(entity_class.db_table, None)


def is_registered(entity_class):
    if hasattr(entity_class, 'db_table'):
        return entity_class.db_table and entity_class.db_table in _registry
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
