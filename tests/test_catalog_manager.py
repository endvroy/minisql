import unittest
from catalog_manager import *
import os

os.chdir('..')


class TestColumn(unittest.TestCase):
    def test_iter(self):
        column = Column('id', '5s', primary_key=True, unique=True)
        self.assertEqual(list(column), ['id', '5s', True, True])


class TestLoad(unittest.TestCase):
    def test_load(self):
        meta1 = load_metadata()
        meta2 = load_metadata()
        self.assertTrue(meta1 is meta2)

    def test_import_load(self):
        import tests.import_a, tests.import_b
        self.assertTrue(tests.import_a.meta is tests.import_b.meta)


def prepare_metadata():
    metadata = Metadata()
    metadata.add_table('spam',
                       Column('id', '5s', primary_key=True),
                       Column('spammer', 'd', unique=True))
    return metadata


class TestMetadata(unittest.TestCase):
    def test_add_table(self):
        metadata = Metadata()
        metadata.add_table('spam',
                           Column('id', '5s', primary_key=True),
                           Column('spammer', 'd', unique=True))

        self.assertTrue('spam' in metadata.tables)
        self.assertTrue('PRIMARY' in metadata.tables['spam'].indexes)
        self.assertEqual(metadata.tables['spam'].indexes['PRIMARY'].columns, ['id'])

    def test_add_duplicate_table(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.add_table('spam', Column('foo', 'd'))

    def test_add_table_without_primary_key(self):
        metadata = Metadata()
        with self.assertRaises(ValueError):
            metadata.add_table('spam',
                               Column('id', '5s'),
                               Column('spammer', 'd', unique=True))

    def test_add_table_without_columns(self):
        metadata = Metadata()
        with self.assertRaises(ValueError):
            metadata.add_table('spam')

    def test_add_index(self):
        metadata = prepare_metadata()
        metadata.add_index('spam', 'spammer_index', 'id', 'spammer')
        self.assertTrue('PRIMARY' in metadata.tables['spam'].indexes)
        self.assertTrue('spammer_index' in metadata.tables['spam'].indexes)
        self.assertEqual(list(metadata.tables['spam'].indexes['spammer_index'].columns),
                         ['id', 'spammer'])

    def test_add_duplicate_index(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.add_index('spam', 'PRIMARY', 'spammer')

    def test_add_index_without_columns(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.add_index('spam', 'foo')

    def test_add_index_on_missing_columns(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.add_index('spam', 'foo', 'id', 'not exist')

    def test_add_index_on_missing_table(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.add_index('foo', 'bar', 'id')

    def test_drop_table(self):
        metadata = prepare_metadata()
        metadata.drop_table('spam')
        self.assertTrue('spam' not in metadata.tables)

    def test_drop_missing_table(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.drop_table('bar')

    def test_drop_index(self):
        metadata = prepare_metadata()
        metadata.add_index('spam', 'spammer_index', 'id', 'spammer')
        metadata.drop_index('spam', 'spammer_index')
        self.assertTrue('spammer_index' not in metadata.tables['spam'].indexes)

    def test_drop_primary_index(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.drop_index('spam', 'PRIMARY')

    def test_drop_missing_index(self):
        metadata = prepare_metadata()
        with self.assertRaises(ValueError):
            metadata.drop_index('spam', 'bar')


if __name__ == '__main__':
    unittest.main()
