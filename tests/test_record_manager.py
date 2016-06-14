import unittest
from record_manager import RecordManager, Record
import os


class TestRecord(unittest.TestCase):
    def setUp(self):
        RecordManager.init_table('foo')
        RecordManager.init_table('foo2')

    def tearDown(self):
        os.remove('foo.table')
        os.remove('foo2.table')

    def test_calc(self):
        record = Record('foo.table', '<i')
        self.assertEqual(record._calc(3), (0, 3))
        self.assertEqual(record._calc(454), (1, 0))
        self.assertEqual(record._calc(909), (2, 0))

    def test_header(self):
        record = Record('foo.table', '<idi')
        self.assertEqual(record._parse_header(), (-1, 0))
        record.first_free_rec = 2
        record.rec_tail = 2
        record._update_header()
        self.assertEqual(record._parse_header(), (2, 2))
        record.first_free_rec = -1
        record.rec_tail = 0
        record._update_header()
        self.assertEqual(record._parse_header(), (-1, 0))

    def test_record(self):
        record = Record('foo.table', '<idi')
        record.insert((1, 2.0, -1))
        record.insert((-1, -1.5, 1))
        self.assertEqual(record.read(0), (1, 2.0, -1))
        self.assertEqual(record.read(1), (-1, -1.5, 1))  # test insert and read
        self.assertEqual(record._parse_header(), (-1, 2))
        record.remove(1)
        self.assertEqual(record._parse_header(), (1, 2))  # test remove, free list
        record.insert((1, 2.0, 3))
        self.assertEqual(record.read(1), (1, 2.0, 3))
        self.assertEqual(record.read(0), (1, 2.0, -1))
        self.assertEqual(record._parse_header(), (-1, 2))  # test free list
        record.insert((1, 1.0, 1))
        record.remove(2)
        record.remove(0)
        self.assertEqual(record._parse_header(), (0, 3))  # test free list
        record.insert((1, 1.0, 1))  # offset 0 and 1 are legal
        self.assertEqual(record._parse_header(), (2, 3))
        with self.assertRaises(IndexError):
            record.remove(4)
        record.modify((1, 1.3, -1), 1)  # test modify
        self.assertEqual(record.read(1), (1, 1.3, -1))

    def test_attributes_conversion(self):
        record = Record('foo.table', '<idi')
        self.assertEqual((record._convert_str_to_bytes((1, 'abcd', 3, 'qweqwe'))),
                         (1, b'abcd', 3, b'qweqwe'))

        self.assertEqual(record._convert_bytes_to_str((1, b'abcd', 3, b'qweqwe')),
                         (1, 'abcd', 3, 'qweqwe'))

    def test_without_index(self):
        record = Record('foo2.table', '<id5s')
        record.insert((1, 1.0, 'abcde'))
        record.insert((1, 1.0, 'bcdef'))
        record.insert((1, 1.0, 'cdefg'))
        conditions = dict()
        conditions[0] = {'=': 1}
        self.assertEqual(record.scanning_select(conditions), [(1, 1.0, 'abcde'), (1, 1.0, 'bcdef'),
                                                              (1, 1.0, 'cdefg')])
        record.scanning_delete(conditions)  # test deletion, case#1
        self.assertEqual(record.scanning_select(conditions), [])
        record.insert((1, 1.0, 'abcde'))
        record.insert((1, 1.0, 'bcdef'))
        record.insert((1, 1.0, 'cdefg'))
        conditions[2] = {'=': 'abcde'}
        record.scanning_delete(conditions)  # test deletion case#2
        del conditions[2]
        self.assertEqual(record.scanning_select(conditions), [(1, 1.0, 'cdefg'), (1, 1.0, 'bcdef')])
        conditions[2] = {'=': 'bcdef'}
        record.scanning_update(conditions, (2, 1.0, 'new'))
        del conditions[2]
        del conditions[0]
        conditions[1] = {'=': 1.0}
        self.assertEqual(record.scanning_select(conditions), [(1, 1.0, 'cdefg'), (2, 1.0, 'new')])


class TestRecordManager(unittest.TestCase):
    def setUp(self):
        RecordManager.init_table('gg')
        RecordManager.init_table('ggg')
        RecordManager.init_table('foo3')

    def tearDown(self):
        os.remove('gg.table')
        os.remove('ggg.table')
        os.remove('foo3.table')

    def test_record_manager(self):
        RecordManager.insert('gg', '<idi', (1, 3.0, 4))
        RecordManager.insert('gg', '<idi', (-1, 3.5, -1))
        self.assertEqual(RecordManager.select('gg', '<idi', with_index=True, record_offset=1),
                         (-1, 3.5, -1))
        RecordManager.update('gg', '<idi', (1, 3.0, 4), with_index=True, record_offset=1)
        self.assertEqual(RecordManager.select('gg', '<idi', with_index=True, record_offset=1),
                         (1, 3.0, 4))
        RecordManager.delete('gg', '<idi', with_index=True, record_offset=1)
        with self.assertRaises(RuntimeError):
            RecordManager.select('gg', '<idi', with_index=True, record_offset=1)
            RecordManager.update('gg', '<idi', (1, 2.0, 3), with_index=True, record_offset=1)

    def test_records_with_string(self):
        RecordManager.insert('ggg', '<idi4s', (1, 2.0, 3, 'temps'))
        # todo: check the length of string in the given attributes ?
        self.assertEqual(RecordManager.select('ggg', '<idi4s', with_index=True, record_offset=0),
                         (1, 2.0, 3, 'temp'))
        RecordManager.insert('ggg', '<idi4s', (1, 2.0, 3, 'no'))  # less than maximum length
        self.assertEqual(RecordManager.select('ggg', '<idi4s', with_index=True, record_offset=1),
                         (1, 2.0, 3, 'no'))
        RecordManager.update('ggg', '<idi4s', (1, 2.0, 3, 'nono'), with_index=True, record_offset=1)
        self.assertEqual(RecordManager.select('ggg', '<idi4s', with_index=True, record_offset=1),
                         (1, 2.0, 3, 'nono'))
        RecordManager.delete('ggg', '<idi4s', with_index=True, record_offset=1)
        RecordManager.delete('ggg', '<idi4s', with_index=True, record_offset=0)
        with self.assertRaises(RuntimeError):
            RecordManager.select('ggg', '<idi4s', with_index=True, record_offset=0)
            RecordManager.update('ggg', '<idi4s', (1, 2.0, 3, 'nono'), with_index=True, record_offset=1)

    def test_repeat_file(self):
        with self.assertRaises(RuntimeError):
            RecordManager.init_table('foo3')
        with self.assertRaises(RuntimeError):
            RecordManager.init_table('gg')

    def test_without_index(self):
        RecordManager.insert('foo3', 'i4sd', (1, 'abcd', 1.0))
        RecordManager.insert('foo3', 'i4sd', (1, 'bcde', 1.0))
        conditions = dict()
        conditions[0] = {'=': 1}
        self.assertEqual(RecordManager.select('foo3', 'i4sd', with_index=False, conditions=conditions),
                         [(1, 'abcd', 1.0), (1, 'bcde', 1.0)])
        RecordManager.delete('foo3', 'i4sd', with_index=False, conditions=conditions)
        self.assertEqual(RecordManager.select('foo3', 'i4sd', with_index=False, conditions=conditions),
                         [])
        RecordManager.insert('foo3', 'i4sd', (1, 'abcd', 1.0))
        RecordManager.insert('foo3', 'i4sd', (1, 'bcde', 1.0))
        del conditions[0]
        conditions[1] = {'=': 'abcd'}
        RecordManager.update('foo3', 'i4sd', (2, 'cccc', 1.0), with_index=False,
                             conditions=conditions)
        del conditions[1]
        conditions[2] = {'=': 1.0}
        self.assertEqual(RecordManager.select('foo3', 'i4sd', with_index=False, conditions=conditions),
                         [(1, 'bcde', 1.0), (2, 'cccc', 1.0)])


if __name__ == '__main__':
    unittest.main()
