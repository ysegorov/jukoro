# -*- coding: utf-8 -*-

from .base import Base

from jukoro import pg


__all__ = ['TestPgIntrospect']


class TestPgIntrospect(Base):

    def test(self):
        schema, state = pg.inspect(self.uri())
        self.assertTrue(schema in state.schemas)
        self.assertTrue('test_pg' in state.tables)
        self.assertTrue('test_pg__live' in state.views)
        self.assertTrue('ju_before__test_pg__insert' in state.triggers)
        self.assertTrue('ju_before__test_pg__delete' in state.triggers)
        self.assertTrue('ju_idx__test_pg__attr1_entity_start_entity_end'
                        in state.indices)
        self.assertTrue('ju_idx__test_pg__attr2_entity_start_entity_end'
                        in state.indices)
        self.assertTrue('ju_idx__test_pg__doc' in state.indices)
        self.assertTrue('ju_idx__test_pg__entity_id' in state.indices)
        for idx in xrange(1, 6):
            self.assertTrue('ju_validate__test_pg__attr%s' % idx
                            in state.constraints)
        self.assertFalse('ju_validate__test_pg__attr6' in state.constraints)
