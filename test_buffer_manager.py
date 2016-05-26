import unittest
import os
import time
from buffer_manager import Block, BufferManager


def prepare_file():
    file = open('foo', 'wb')
    file.write(b'Hello World')
    file.close()


class TestBlock(unittest.TestCase):
    def test_block(self):
        prepare_file()

        file = open('foo', 'r+b')
        block = Block(5, file, 0)
        self.assertEqual(block.read(), b'Hello')  # test read
        block.write(b'abcde')
        self.assertEqual(block.read(), b'abcde')  # test write
        self.assertTrue(block.dirty)  # test that write sets dirty bit
        block.flush()
        self.assertFalse(block.dirty)  # test that flush resets dirty bit
        with open('foo', 'rb') as f:
            self.assertEqual(f.read(), b'abcde World')  # test flush writes back to file
        # block.pin()
        self.assertTrue(block.pinned)  # test that blocks are pinned by default
        block.release()
        self.assertFalse(block.pinned)  # test that release unpins the block
        self.assertFalse(file.closed)  # test that release doesn't close the file
        file.close()

    def test_partial_read(self):
        prepare_file()
        file = open('foo', 'r+b')
        block = Block(5, file, 2)  # test partial read
        self.assertEqual(block.effective_bytes, 1)
        self.assertEqual(block.read(), b'd')
        block.write(b'D')
        self.assertEqual(block.read(), b'D')
        block.release()
        file.seek(0)
        self.assertEqual(file.read(), b'Hello WorlD')
        file.close()


class TestBufferManager(unittest.TestCase):
    def test_buffer_manager(self):
        BufferManager.block_size = 5
        BufferManager.total_blocks = 2
        prepare_file()
        manager = BufferManager()
        a = manager.get_file_block('foo', 0)
        self.assertEqual(a.read(), b'Hello')
        b = manager.get_file_block('./foo', 0)
        self.assertTrue(a is b)  # test cache hit, don't do this in production code!
        b = manager.get_file_block('foo', 1)
        self.assertEqual(b.read(), b' Worl')
        with self.assertRaises(RuntimeError):
            c = manager.get_file_block('foo', 2)  # test buffer run out of space
        a.release()
        time.sleep(1)
        b.release()
        c = manager.get_file_block('foo', 2)  # test lru swap
        self.assertFalse((os.path.abspath('foo'), 0) in manager.blocks.keys())  # a should be swapped out
        self.assertTrue((os.path.abspath('foo'), 1) in manager.blocks.keys())  # b should remain in the buffer


if __name__ == '__main__':
    unittest.main()
