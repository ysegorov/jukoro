# -*- coding: utf-8 -*-

from collections import OrderedDict

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

    def __str__(self):
        return self.query.format(**self.sql_vars())

    def sql_vars(self):
        instance = self.instance
        return {
            'db_table': instance.db_table,
            'db_view': instance.db_view,
        }


SEQ = 'global_entity_id_seq'  # for internal reference only
ET = 'entity'  # for internal reference only
# base table name intentionally hardcoded
INIT_SCHEMA = """
CREATE SCHEMA {schema};

SET search_path TO {schema};

CREATE SEQUENCE global_entity_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- basic entity table (for inheritance)
CREATE TABLE IF NOT EXISTS "entity" (
    "id" serial PRIMARY KEY,
    "entity_id" bigint NOT NULL DEFAULT nextval('global_entity_id_seq'),
    "entity_start" timestamp with time zone DEFAULT current_timestamp,
    "entity_end" timestamp with time zone
            DEFAULT '2999-12-31 23:59:59.999+0'::timestamp with time zone,
    "data" jsonb NOT NULL
);
"""


class InitSchemaSql(BaseSql):
    query = INIT_SCHEMA

    def sql_vars(self):
        return {'schema': self.instance.schema}


CREATE_TABLE = """
-- {db_table} table
CREATE TABLE IF NOT EXISTS "{db_table}" (
    "id" serial PRIMARY KEY
) INHERITS ("entity");
"""


class CreateTableSql(BaseSql):
    query = CREATE_TABLE


CREATE_VIEW = """
-- {db_table} master view
CREATE OR REPLACE VIEW "{db_view}" AS SELECT * FROM "{db_table}"
    WHERE "entity_start" <= now() AND "entity_end" > now();
"""


class CreateViewSql(BaseSql):
    query = CREATE_VIEW


class BaseTrigger(BaseSql):

    @property
    def trigger_proc_name(self):
        instance = self.instance
        return 'ju_{}__{}'.format(instance.db_table, self.suffix)

    @property
    def trigger_name(self):
        instance = self.instance
        return 'ju_before_{}__{}'.format(instance.db_table, self.suffix)

    def sql_vars(self):
        kwargs = super(BaseTrigger, self).sql_vars()
        kwargs.update({
            'trigger_name': self.trigger_name,
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
    sql_init_schema = SqlDescr(InitSchemaSql)

    def __init__(self, schema):
        self._schema = schema

    @property
    def schema(self):
        return self._schema

    @property
    def tables(self):
        for table in _registry.itervalues():
            yield table


class Table(object):
    sql_create_table = SqlDescr(CreateTableSql)
    sql_create_view = SqlDescr(CreateViewSql)
    sql_trigger_on_insert = SqlDescr(TriggerOnInsertSql)
    sql_trigger_on_delete = SqlDescr(TriggerOnDeleteSql)

    def __init__(self, entity_class):
        self._entity_class = entity_class

    @property
    def db_table(self):
        return self._entity_class.db_table

    @property
    def db_view(self):
        return self._entity_class.db_view

    @property
    def db_indices(self):
        for attr in self._entity_class.attrs:
            if attr.db_index:
                yield Index(self, attr)

INDEX = """
CREATE INDEX {index_name} ON
    "{db_table}" USING btree({spec}, "entity_start", "entity_end" DESC);
"""


class IndexSql(BaseSql):
    query = INDEX

    @property
    def index_name(self):
        return 'ju_idx__{}_{}_entity_start_entity_end'.format(
            self.instance.db_table, self.instance.attr_slug)

    @property
    def spec(self):
        spec = '("data"->>\'{attr}\')'
        if self.instance.attr_type == 'int':
            spec = '(%s::INTEGER)' % spec
        return spec.format(attr=self.instance.attr_slug)

    def sql_vars(self):
        kwargs = super(IndexSql, self).sql_vars()
        kwargs.update({'index_name': self.index_name, 'spec': self.spec})
        return kwargs


class Index(object):
    sql_index = SqlDescr(IndexSql)

    def __init__(self, table, attr):
        self._table = table
        self._attr = attr

    @property
    def db_table(self):
        return self._table.db_table

    @property
    def db_view(self):
        return self._table.db_view

    @property
    def attr_slug(self):
        return self._attr.slug

    @property
    def attr_type(self):
        return self._attr.db_type


CONSTRAINT_INT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT ju_validate__{db_table}_{attr}
    CHECK (("data"->>'{attr}') IS NOT NULL
    AND ("data"->>'{attr}')::INTEGER >= 0);
"""
CONSTRAINT_TEXT = """
ALTER TABLE "{db_table}" ADD CONSTRAINT ju_validate__{db_table}_{attr}
    CHECK (("data"->>'{attr}') IS NOT NULL AND length("data"->>'{attr}') > 0);
"""
CONSTRAINT_NOT_NULL = """
ALTER TABLE "{db_table}" ADD CONSTRAINT ju_validate__{db_table}_{attr}
    CHECK (("data"->>'{attr}') IS NOT NULL);
"""


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
    assert bool(entity_class.db_table)
    tn = entity_class.db_table
    if tn in _registry:
        raise AttributeError('Model for "%s" already registered' % tn)
    _registry[entity_class.db_table] = Table(entity_class)


def unregister(entity_class):
    if entity_class.db_table:
        _registry.pop(entity_class.db_table, None)


def syncdb(uri):

    schema_name, current = inspect(uri)
    schema = Schema(schema_name)

    if schema_name not in current.schemas:
        yield str(schema.sql_init_schema)
    current.schemas = []
    current.sequences = []
    current.tables.discard(ET)

    for table in schema.tables:
        if table.db_table not in current.tables:
            yield str(table.sql_create_table)
        else:
            current.tables.discard(table.db_table)
        if table.db_view not in current.views:
            yield str(table.sql_create_view)
        else:
            current.views.discard(table.db_view)

        for trig in (table.sql_trigger_on_insert, table.sql_trigger_on_delete):
            if trig.trigger_name not in current.triggers:
                yield str(trig)
            else:
                current.triggers.discard(trig.trigger_name)

        for idx in table.db_indices:
            sql_index = idx.sql_index
            if sql_index.index_name not in current.indices:
                yield str(sql_index)
            else:
                current.indices.discard(sql_index.index_name)

    yield 'CUT'
