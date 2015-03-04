# -*- coding: utf-8 -*-

from .base import Base

from jukoro import pg


__all__ = ['TestPgIntrospect']


class TestPgIntrospect(Base):

    def test(self):
        schema, current = pg.inspect(self.uri())
        self.assertTrue(schema in current.schemas)
        self.assertTrue('test_pg' in current.tables)
        self.assertTrue('test_pg__live' in current.views)
        self.assertTrue('ju_before__test_pg__insert' in current.triggers)
        self.assertTrue('ju_before__test_pg__delete' in current.triggers)
        self.assertTrue('ju_idx__test_pg__attr1_entity_start_entity_end'
                        in current.indices)
        self.assertTrue('ju_idx__test_pg__attr2_entity_start_entity_end'
                        in current.indices)
        for idx in xrange(1, 6):
            self.assertTrue('ju_validate__test_pg__attr%s' % idx
                            in current.constraints)
        self.assertFalse('ju_validate__test_pg__attr6' in current.constraints)
