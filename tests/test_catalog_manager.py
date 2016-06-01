import unittest
import catalog_manager


class TestCatalogManager(unittest.TestCase):
    def test_add_table(self):
        catalog_manager.add_table(meta, 'spam',
                                  Column('id', '5s', True, False))
