import unittest
import os
from buffer_manager import Block, BufferManager
from index_manager import _convert_to_tuple, _convert_to_tuple_list, _encode_sequence, _decode_sequence, iter_chunk
from index_manager import node_factory, IndexManager


class TestHelperFunctions(unittest.TestCase):
    def test_convert_to_tuple(self):
        self.assertEqual(_convert_to_tuple(42), (42,))
        self.assertEqual(_convert_to_tuple([42]), (42,))

    def test_convert_to_tuples(self):
        self.assertEqual(_convert_to_tuple_list([42, 666]), [(42,), (666,)])
        self.assertEqual(_convert_to_tuple_list([[42, 666, 'spam'], (233, 987, 'foo')]),
                         [(42, 666, 'spam'), (233, 987, 'foo')])

    def test_encode_sequence(self):
        self.assertEqual(_encode_sequence(['spam', 42, 'foo']), (b'spam', 42, b'foo'))

    def test_decode_sequence(self):
        self.assertEqual(_decode_sequence([b'spam', 42, b'foo']), ('spam', 42, 'foo'))

    def test_iter_chunk(self):
        octets = b'12345678'
        chunks = list(iter_chunk(octets, 2, 3, 2))
        self.assertEqual(chunks, [b'345', b'678'])


class TestBPlusNode(unittest.TestCase):
    def test_bytes(self):
        Node = node_factory('<ii5s')
        node = Node(True, [(42, 666, 'spam'), (233, 987, 'foo')], [518, 2, 42])
        octets = bytes(node)
        node2 = Node.frombytes(octets.ljust(BufferManager.block_size, b'\0'))
        self.assertEqual(node2.is_leaf, True)
        self.assertEqual(node2.keys, [(42, 666, 'spam'), (233, 987, 'foo')])
        self.assertEqual(node2.children, [518, 2, 42])

    def test_insert_into_leaf(self):
        Node = node_factory('<i')
        node = Node(True, [0, 2, 4, 6, 8], [0, 1, 2, 3, 4, 5])
        node.insert(3, 518)
        self.assertEqual(node.keys, [(0,), (2,), (3,), (4,), (6,), (8,)])
        self.assertEqual(node.children, [0, 1, 518, 2, 3, 4, 5])

    def test_insert_into_internal(self):
        Node = node_factory('<i')
        node = Node(False, [0, 2, 4, 6, 8], [0, 1, 2, 3, 4, 5])
        node.insert(3, 518)
        self.assertEqual(node.keys, [(0,), (2,), (3,), (4,), (6,), (8,)])
        self.assertEqual(node.children, [0, 1, 518, 2, 3, 4, 5])

    def test_split_leaf(self):
        Node = node_factory('<i')
        Node.n = 4
        node = Node(True, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 42])
        new_node = node._split()
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(0,), (1,), (2,)])
        self.assertEqual(node.children, [0, 1, 2])
        self.assertEqual(new_node.is_leaf, True)
        self.assertEqual(new_node.keys, [(3,), (4,)])
        self.assertEqual(new_node.children, [3, 4, 42])

    def test_split_internal(self):
        Node = node_factory('<i')
        Node.n = 4
        node = Node(False, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5])
        new_node = node._split()
        self.assertEqual(node.is_leaf, False)
        self.assertEqual(node.keys, [(0,), (1,), (2,)])
        self.assertEqual(node.children, [0, 1, 2])
        self.assertEqual(new_node.is_leaf, False)
        self.assertEqual(new_node.keys, [(3,), (4,)])
        self.assertEqual(new_node.children, [3, 4, 5])

    def test_fuse_leaves(self):
        Node = node_factory('<i')
        Node.n = 4
        left = Node(True, [2], [6, 42])
        right = Node(True, [5, 7], [25, 76])
        left.fuse_with(right)
        self.assertEqual(left.is_leaf, True)
        self.assertEqual(left.keys, [(2,), (5,), (7,)])
        self.assertEqual(left.children, [6, 25, 76])

    def test_fuse_internals(self):
        Node = node_factory('<i')
        Node.n = 4
        left = Node(False, [2], [6, 42])
        right = Node(False, [5, 7], [25, 76])
        left.fuse_with(right)
        self.assertEqual(left.is_leaf, False)
        self.assertEqual(left.keys, [(2,), (5,), (7,)])
        self.assertEqual(left.children, [6, 42, 25, 76])

    def test_mixed_fuse(self):
        Node = node_factory('<i')
        Node.n = 4
        left = Node(True, [2], [6, 42])
        right = Node(False, [5, 7], [25, 76])
        with self.assertRaises(ValueError):
            left.fuse_with(right)


class TestBPlusNodeWithFile(unittest.TestCase):
    def setUp(self):
        with open('bar', 'wb') as file:
            file.write(b'\0' * BufferManager.block_size * 100)

    def tearDown(self):
        os.remove('bar')

    def test_leaf_split_and_write(self):
        Node = node_factory('<i')
        node = Node(True, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 42])
        Node.n = 4
        block = Block(BufferManager.block_size, 'bar', 6)
        new_block = Block(BufferManager.block_size, 'bar', 50)
        key, value = node.split_and_write(block, new_block)

        self.assertEqual(key, (3,))
        self.assertEqual(value, 50)
        node = Node.frombytes(block.read())
        new_node = Node.frombytes(new_block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(0,), (1,), (2,)])
        self.assertEqual(node.children, [0, 1, 2, 50])
        self.assertEqual(new_node.is_leaf, True)
        self.assertEqual(new_node.keys, [(3,), (4,)])
        self.assertEqual(new_node.children, [3, 4, 42])


class TestIndexManager(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass

    def tearDown(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass

    def test_init(self):
        manager = IndexManager('spam', '<id')
        self.assertEqual(manager.root, 0)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 1)

    def test_initial_insert(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)

        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)
        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(42, 7.6)])
        self.assertEqual(node.children, [518, 0])

    def test_later_insert(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)

        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)
        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(42, 7.6), (233, 66.6)])
        self.assertEqual(node.children, [518, 7, 0])

    def test_find(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        result = manager.find([42, 7.6])
        self.assertEqual(result, [518])

    def test_find_all(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([42, 7.6], 212)
        manager.insert([233, 66.6], 7)
        results = manager.find([42, 7.6])
        self.assertEqual(sorted(results), [212, 518])

    def test_find_from_empty(self):
        manager = IndexManager('spam', '<id')
        result = manager.find([23, 3])
        self.assertEqual(result, [])

    def test_find_not_exists(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        result = manager.find([233, 7.6])
        self.assertEqual(result, [])

    def test_delete_from_empty(self):
        manager = IndexManager('spam', '<id')
        deleted_num = manager.delete([2, 3.3])
        self.assertEqual(deleted_num, 0)

    def test_successful_delete(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        deleted_num = manager.delete([42, 7.6])

        self.assertEqual(deleted_num, 1)
        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)
        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(233, 66.6)])
        self.assertEqual(node.children, [7, 0])

    @unittest.skip('no longer supported')
    def test_multiple_delete(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([42, 7.6], 212)
        manager.insert([233, 66.6], 7)
        deleted_num = manager.delete([42, 7.6])

        self.assertEqual(deleted_num, 2)
        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)
        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(233, 66.6)])
        self.assertEqual(node.children, [7, 0])

    def test_unsuccessful_delete(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        deleted_num = manager.delete([2, 3.3])
        self.assertEqual(deleted_num, 0)

    def test_shrinking_delete(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        manager.delete([233, 66.6])
        deleted_num = manager.delete([42, 7.6])

        self.assertEqual(deleted_num, 1)
        self.assertEqual(manager.root, 0)
        self.assertEqual(manager.first_deleted_block, 1)
        self.assertEqual(manager.total_blocks, 2)

    def test_reallocating_insert(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.delete([42, 7.6])

        manager.insert([233, 66.6], 7)

        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)

        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(233, 66.6)])
        self.assertEqual(node.children, [7, 0])


class TestPersistence(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass

    def tearDown(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass

    def test_persistence(self):
        manager = IndexManager('spam', '<id')
        manager.insert([42, 7.6], 518)
        manager.insert([233, 66.6], 7)
        manager.delete([42, 7.6])
        manager.dump_header()
        manager._manager.flush_all()
        del manager

        manager = IndexManager('spam', '<id')
        self.assertEqual(manager.root, 1)
        self.assertEqual(manager.first_deleted_block, 0)
        self.assertEqual(manager.total_blocks, 2)
        block = BufferManager().get_file_block('spam', 1)
        Node = manager.Node
        node = Node.frombytes(block.read())
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [(233, 66.6)])
        self.assertEqual(node.children, [7, 0])


class TestTransfer(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass
        self.manager = IndexManager('spam', '<i')

    def tearDown(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass
        del self.manager

    def test_tranfer_left_to_right_leaf(self):
        Node = self.manager.Node
        parent = Node(False,
                      [2, 15, 35],
                      [54, 21, 518, 42])

        left = Node(True,
                    [2, 7, 11],
                    [32, 87, 43, 518])

        right = Node(True,
                     [15, 24, 31],
                     [45, 67, 89, 42])

        self.manager._transfer_left_to_right(left, right, parent, 1)

        self.assertEqual(parent.keys, _convert_to_tuple_list([2, 11, 35]))
        self.assertEqual(parent.children, [54, 21, 518, 42])

        self.assertEqual(left.keys, _convert_to_tuple_list([2, 7]))
        self.assertEqual(left.children, [32, 87, 518])

        self.assertEqual(right.keys, _convert_to_tuple_list([11, 15, 24, 31]))
        self.assertEqual(right.children, [43, 45, 67, 89, 42])

    def test_tranfer_left_to_right_internal(self):
        Node = self.manager.Node
        parent = Node(False,
                      [2, 15, 35],
                      [54, 21, 518, 42])

        left = Node(False,
                    [2, 7, 11],
                    [32, 87, 43, 518])

        right = Node(False,
                     [15, 24, 31],
                     [45, 67, 89, 42])

        self.manager._transfer_left_to_right(left, right, parent, 1)

        self.assertEqual(parent.keys, _convert_to_tuple_list([2, 11, 35]))
        self.assertEqual(parent.children, [54, 21, 518, 42])

        self.assertEqual(left.keys, _convert_to_tuple_list([2, 7]))
        self.assertEqual(left.children, [32, 87, 43])

        self.assertEqual(right.keys, _convert_to_tuple_list([11, 15, 24, 31]))
        self.assertEqual(right.children, [518, 45, 67, 89, 42])

    def test_tranfer_right_to_left_leaf(self):
        Node = self.manager.Node
        parent = Node(False,
                      [2, 15, 35],
                      [54, 21, 518, 42])

        left = Node(True,
                    [2, 7, 11],
                    [32, 87, 43, 518])

        right = Node(True,
                     [15, 24, 31],
                     [45, 67, 89, 42])

        self.manager._transfer_right_to_left(left, right, parent, 1)

        self.assertEqual(parent.keys, _convert_to_tuple_list([2, 24, 35]))
        self.assertEqual(parent.children, [54, 21, 518, 42])

        self.assertEqual(left.keys, _convert_to_tuple_list([2, 7, 11, 15]))
        self.assertEqual(left.children, [32, 87, 43, 45, 518])

        self.assertEqual(right.keys, _convert_to_tuple_list([24, 31]))
        self.assertEqual(right.children, [67, 89, 42])

    def test_tranfer_right_to_left_internal(self):
        Node = self.manager.Node
        parent = Node(False,
                      [2, 15, 35],
                      [54, 21, 518, 42])

        left = Node(False,
                    [2, 7, 11],
                    [32, 87, 43, 518])

        right = Node(False,
                     [15, 24, 31],
                     [45, 67, 89, 42])

        self.manager._transfer_right_to_left(left, right, parent, 1)

        self.assertEqual(parent.keys, _convert_to_tuple_list([2, 24, 35]))
        self.assertEqual(parent.children, [54, 21, 518, 42])

        self.assertEqual(left.keys, _convert_to_tuple_list([2, 7, 11, 15]))
        self.assertEqual(left.children, [32, 87, 43, 518, 45])

        self.assertEqual(right.keys, _convert_to_tuple_list([24, 31]))
        self.assertEqual(right.children, [67, 89, 42])


class TestAdjustingDeletion(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass
        self.manager = IndexManager('spam', '<i')
        self.manager.Node.n = 4

    def tearDown(self):
        try:
            os.remove('spam')
        except FileNotFoundError:
            pass
        del self.manager

    def test_delete_transfer(self):
        self.manager.Node.n = 4
        self.manager.insert(2, 32)
        self.manager.insert(24, 67)
        self.manager.insert(7, 87)
        self.manager.insert(15, 45)
        self.manager.insert(11, 43)
        deleted_num = self.manager.delete(24)

        self.assertEqual(deleted_num, 1)
        self.assertEqual(self.manager.root, 3)
        Node = self.manager.Node

        root_block = BufferManager().get_file_block(self.manager.index_file_path, self.manager.root)
        root_node = Node.frombytes(root_block.read())
        self.assertEqual(root_node.keys, [(11,)])

        left_block = BufferManager().get_file_block(self.manager.index_file_path, 1)
        left_node = Node.frombytes(left_block.read())
        self.assertEqual(left_node.keys, _convert_to_tuple_list([2, 7]))
        self.assertEqual(left_node.children, [32, 87, 2])

        right_block = BufferManager().get_file_block(self.manager.index_file_path, 2)
        right_node = Node.frombytes(right_block.read())
        self.assertEqual(right_node.keys, _convert_to_tuple_list([11, 15]))
        self.assertEqual(right_node.children, [43, 45, 0])

    def test_delete_fuse(self):
        self.manager.Node.n = 4
        self.manager.insert(2, 32)
        self.manager.insert(24, 67)
        self.manager.insert(7, 87)
        self.manager.insert(15, 45)
        self.manager.insert(11, 43)
        self.manager.delete(24)
        deleted_num = self.manager.delete(11)

        self.assertEqual(deleted_num, 1)
        self.assertEqual(self.manager.root, 1)
        Node = self.manager.Node

        root_block = BufferManager().get_file_block(self.manager.index_file_path, self.manager.root)
        root_node = Node.frombytes(root_block.read())
        self.assertEqual(root_node.is_leaf, True)
        self.assertEqual(root_node.keys, _convert_to_tuple_list([2, 7, 15]))
        self.assertEqual(root_node.children, [32, 87, 45, 0])


if __name__ == '__main__':
    unittest.main()
