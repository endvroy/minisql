import unittest
from record_manager import RecordManager, Record
import os

os.chdir('..')

'''
# Called only once to initialize table files
 RecordManager.init_table('foo')
 RecordManager.init_table('gg')
 RecordManager.init_table('ggg')
'''

class TestRecord(unittest.TestCase):
    def test_header(self):
        record = Record('./schema/tables/foo.table', '<idi')
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
        record = Record('./schema/tables/foo.table', '<idi')
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
        record = Record('./schema/tables/foo.table', '<idi')
        self.assertEqual((record._convert_str_to_bytes((1, 'abcd', 3, 'qweqwe'))),
                         (1, b'abcd', 3, b'qweqwe'))

        self.assertEqual(record._convert_bytes_to_str((1, b'abcd', 3, b'qweqwe')),
                         (1, 'abcd', 3, 'qweqwe'))


class TestRecordManager(unittest.TestCase):
    def test_record_manager(self):
        RecordManager.insert('gg', '<idi', (1, 3.0, 4))
        RecordManager.insert('gg', '<idi', (-1, 3.5, -1))
        self.assertEqual(RecordManager.select('gg', '<idi', 1), (-1, 3.5, -1))
        RecordManager.update('gg', '<idi', (1, 3.0, 4), 1)
        self.assertEqual(RecordManager.select('gg', '<idi', 1), (1, 3.0, 4))
        RecordManager.delete('gg', '<idi', 1)
        with self.assertRaises(RuntimeError):
            RecordManager.select('gg', '<idi', 1)
            RecordManager.update('gg', '<idi', (1, 2.0, 3), 1)


    def test_records_with_string(self):
        RecordManager.insert('ggg', '<idi4s', (1, 2.0, 3, 'temps'))
        # todo: check the length of string in the given attributes ?
        self.assertEqual(RecordManager.select('ggg', '<idi4s', 0), (1, 2.0, 3, 'temp'))
        RecordManager.insert('ggg', '<idi4s', (1, 2.0, 3, 'no'))  # less than maximum length
        self.assertEqual(RecordManager.select('ggg', '<idi4s', 1), (1, 2.0, 3, 'no'))
        RecordManager.update('ggg', '<idi4s', (1, 2.0, 3, 'nono'), 1)
        self.assertEqual(RecordManager.select('ggg', '<idi4s', 1), (1, 2.0, 3, 'nono'))
        RecordManager.delete('ggg', '<idi4s', 1)
        RecordManager.delete('ggg', '<idi4s', 0)
        with self.assertRaises(RuntimeError):
            RecordManager.select('ggg', '<idi4s', 0)
            RecordManager.update('ggg', '<idi4s', (1, 2.0, 3, 'nono'), 1)

    def test_repeat_file(self):
        with self.assertRaises(RuntimeError):
            RecordManager.init_table('foo')
        with self.assertRaises(RuntimeError):
            RecordManager.init_table('gg')


if __name__ == '__main__':
    unittest.main()
