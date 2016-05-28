import unittest
import os
import time
from buffer_manager import Block, BufferManager


def prepare_file():
    with open('foo', 'wb') as file:
        file.write(b'Hello World')


class TestBlock(unittest.TestCase):
    def test_block(self):
        prepare_file()

        block = Block(5, 'foo', 0)
        self.assertEqual(block.read(), b'Hello')  # test read
        block.write(b'abcde')
        self.assertEqual(block.read(), b'abcde')  # test write
        self.assertTrue(block.dirty)  # test that write sets dirty bit
        block.flush()
        self.assertFalse(block.dirty)  # test that flush resets dirty bit
        with open('foo', 'rb') as f:
            self.assertEqual(f.read(), b'abcde World')  # test flush writes back to file
        block.pin()
        self.assertEqual(block.pin_count, 1)  # test pin increases pin count
        # with self.assertRaises(RuntimeError):  # test pinned block cannot be freed
        #     block.flush()
        block.unpin()
        self.assertEqual(block.pin_count, 0)  # test that unpin increases pin count
        block.flush()

    def test_partial_read(self):
        prepare_file()
        block = Block(5, 'foo', 2)  # test partial read
        self.assertEqual(block.effective_bytes, 1)
        self.assertEqual(block.read(), b'd')
        block.write(b'D')
        self.assertEqual(block.read(), b'D')
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello WorlD')

    def test_expanding_write(self):
        prepare_file()
        block = Block(5, 'foo', 2)
        block.write(b'D12')
        self.assertEqual(block.effective_bytes, 3)
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello WorlD12')

    def test_overflow_write(self):
        prepare_file()
        block = Block(5, 'foo', 2)
        with self.assertRaises(RuntimeError):
            block.write(b'whos your daddy')
        self.assertEqual(block.read(), b'd')  # test the data is not corrupted
        self.assertEqual(block.effective_bytes, 1)
        block.write(b'whos your daddy', trunc=True)
        self.assertEqual(block.read(), b'whos ')
        self.assertEqual(block.effective_bytes, 5)
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello Worlwhos ')


class TestBufferManager(unittest.TestCase):
    def test_buffer_manager(self):
        BufferManager.block_size = 5
        prepare_file()
        manager = BufferManager(total_blocks=2)
        a = manager.get_file_block('foo', 0)
        a.pin()
        self.assertEqual(a.read(), b'Hello')
        b = manager.get_file_block('./foo', 0)
        self.assertTrue(a is b)  # test cache hit

        b = manager.get_file_block('foo', 1)
        b.pin()
        time.sleep(0.5)
        self.assertEqual(b.read(), b' Worl')
        with self.assertRaises(RuntimeError):
            c = manager.get_file_block('foo', 2)  # test buffer run out of space
        a.unpin()
        b.unpin()
        c = manager.get_file_block('foo', 2)  # test lru swap
        self.assertFalse((os.path.abspath('foo'), 0) in manager._blocks.keys())  # a should be swapped out
        self.assertTrue((os.path.abspath('foo'), 1) in manager._blocks.keys())  # b should remain in the buffer
        # manager.free()


if __name__ == '__main__':
    unittest.main()
