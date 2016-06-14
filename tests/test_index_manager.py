import unittest
import os
from buffer_manager import Block
from index_manager import _convert_to_tuple, _convert_to_tuple_list, _encode_sequence, _decode_sequence, iter_chunk
from index_manager import node_factory


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
        Node = node_factory('<2i5s')
        node = Node(True, [(42, 666, 'spam'), (233, 987, 'foo')], [518, 2, 42])
        octets = bytes(node)
        node2 = Node.frombytes(octets.ljust(4096, b'\0'))
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
            file.write(b'\0' * 4096 * 100)

    def tearDown(self):
        os.remove('bar')

    def test_leaf_split_and_write(self):
        Node = node_factory('<i')
        node = Node(True, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 42])
        Node.n = 4
        block = Block(4096, 'bar', 6)
        new_block = Block(4096, 'bar', 50)
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


if __name__ == '__main__':
    unittest.main()
