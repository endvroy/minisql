import unittest
import os
import time
from buffer_manager import pin, Block, BufferManager


class TestBlock(unittest.TestCase):
    def setUp(self):
        with open('foo', 'wb') as file:
            file.write(b'Hello World')

    def tearDown(self):
        os.remove('foo')

    def test_block(self):
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
        block = Block(5, 'foo', 2)  # test partial read
        self.assertEqual(block.effective_bytes, 1)
        self.assertEqual(block.read(), b'd')
        block.write(b'D')
        self.assertEqual(block.read(), b'D')
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello WorlD')

    def test_expanding_write(self):
        block = Block(5, 'foo', 2)
        block.write(b'D12')
        self.assertEqual(block.effective_bytes, 3)
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello WorlD12')

    def test_overflow_write(self):
        block = Block(5, 'foo', 2)
        with self.assertRaises(RuntimeError):
            block.write(b'whos your daddy')
        self.assertEqual(block.read(), b'd')  # test the data is not corrupted
        self.assertEqual(block.effective_bytes, 1)
        block.write(b'whos your daddy', trunc=True)
        self.assertEqual(block.read(), b'whos ')
        self.assertEqual(block.effective_bytes, 5)
        self.assertEqual(len(block._memory), 5)
        block.flush()
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'Hello Worlwhos ')


class TestContextManager(unittest.TestCase):
    def setUp(self):
        with open('foo', 'wb') as file:
            file.write(b'Hello World')

    def tearDown(self):
        os.remove('foo')

    def test_pin(self):
        block = Block(5, 'foo', 0)
        with pin(block):
            self.assertEqual(block.pin_count, 1)
            self.assertEqual(block.read(), b'Hello')
        self.assertEqual(block.pin_count, 0)


class TestBufferManager(unittest.TestCase):
    def setUp(self):
        with open('foo', 'wb') as file:
            file.write(b'Hello World')
        BufferManager.block_size = 5
        BufferManager.total_blocks = 2

    def tearDown(self):
        os.remove('foo')
        BufferManager.block_size = 4096
        BufferManager.total_blocks = 1024

    def test_buffer_manager(self):
        manager = BufferManager()
        a = manager.get_file_block('foo', 0)
        a.pin()
        self.assertEqual(a.read(), b'Hello')
        b = manager.get_file_block('./foo', 0)
        self.assertTrue(a is b)  # test cache hit
        a.write(b'hello')
        # a is not flushed

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
        with open('foo', 'rb') as file:
            self.assertEqual(file.read(), b'hello World')  # test the swapped out block is flushed

    def test_singleton(self):
        manager_a = BufferManager()
        manager_b = BufferManager()
        self.assertTrue(manager_a is manager_b)

    def test_import_singleton(self):
        import tests.buffer_import_a, tests.buffer_import_b
        self.assertTrue(tests.buffer_import_a.manager is tests.buffer_import_b.manager)

    def test_detach(self):
        manager = BufferManager()
        manager.get_file_block('foo', 0)
        manager.detach_from_file('foo')
        self.assertFalse(manager._blocks)


if __name__ == '__main__':
    unittest.main()
