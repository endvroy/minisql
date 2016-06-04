import unittest
from record_manager import RecordManager, Record
from struct import Struct


def prepare_file(filename):
    with open(filename, 'w+b') as f:
        header_struct = Struct('<ii')
        f.write(header_struct.pack(*(-1, 0)))


def display_file(filename):
    with open(filename, 'r+b') as f:
        content = f.read()
        print(content, len(content), sep='\n')


class TestRecord(unittest.TestCase):
    def test_header(self):
        prepare_file('a.record')
        record = Record('a.record', '<idi')
        self.assertEqual(record._parse_header(), (-1, 0))
        record.first_free_rec = 2
        record.rec_amount = 2
        record._update_header()
        self.assertEqual(record._parse_header(), (2, 2))
        record.first_free_rec = -1
        record.rec_amount = 0
        record._update_header()
        self.assertEqual(record._parse_header(), (-1, 0))

    def test_record(self):
        record = Record('a.record', '<idi')
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


class TestRecordManager(unittest.TestCase):
    def test_record_manager(self):
        prepare_file('b.record')
        RecordManager.insert('b.record', '<idi', (1, 3.0, 4))
        RecordManager.insert('b.record', '<idi', (-1, 3.5, -1))
        self.assertEqual(RecordManager.select('b.record', '<idi', 1), (-1, 3.5, -1))
        RecordManager.update('b.record', '<idi', (1, 3.0, 4), 1)
        self.assertEqual(RecordManager.select('b.record', '<idi', 1), (1, 3.0, 4))
        RecordManager.delete('b.record', '<idi', 1)


if __name__ == '__main__':
    unittest.main()
