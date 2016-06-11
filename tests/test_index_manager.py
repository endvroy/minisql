import unittest
from index_manager import _encode_sequence, _decode_sequence, iter_chunk, node_factory


class TestHelperFunctions(unittest.TestCase):
    def test_encode_sequence(self):
        self.assertEqual(_encode_sequence(['spam', 42, 'foo']), (b'spam', 42, b'foo'))

    def test_decode_sequence(self):
        self.assertEqual(_decode_sequence([b'spam', 42, b'foo']), ('spam', 42, 'foo'))

    def test_iter_chunk(self):
        octets = b'12345678'
        chunks = list(iter_chunk(octets, 2, 3, 2))
        self.assertEqual(chunks, [b'345', b'678'])


class TestBPlusTree(unittest.TestCase):
    def test_bytes(self):
        Node = node_factory('<2i5s')
        node = Node(True, [(42, 666, 'spam'), (233, 987, 'foo')], [518, 2, 42])
        octets = bytes(node)
        node2 = Node.frombytes(octets.ljust(4096, b'\0'))
        self.assertEqual(node2.is_leaf, True)
        self.assertEqual(node2.keys, [(42, 666, 'spam'), (233, 987, 'foo')])
        self.assertEqual(node2.children, [518, 2, 42])

    def test_split_leaf(self):
        Node = node_factory('<i')
        node = Node(True, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5])
        Node.n = 4
        new_node = node.split()
        self.assertEqual(node.is_leaf, True)
        self.assertEqual(node.keys, [0, 1])
        self.assertEqual(node.children, [0, 1])
        self.assertEqual(new_node.is_leaf, True)
        self.assertEqual(new_node.keys, [2, 3, 4])
        self.assertEqual(new_node.children, [2, 3, 4, 5])

    def test_split_internal(self):
        Node = node_factory('<i')
        node = Node(False, [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5])
        Node.n = 4
        new_node = node.split()
        self.assertEqual(node.is_leaf, False)
        self.assertEqual(node.keys, [0, 1, 2])
        self.assertEqual(node.children, [0, 1, 2])
        self.assertEqual(new_node.is_leaf, False)
        self.assertEqual(new_node.keys, [3, 4])
        self.assertEqual(new_node.children, [3, 4, 5])

    def test_insert_into_leaf(self):
        Node = node_factory('<i')
        node = Node(True, [0, 2, 4, 6, 8], [0, 1, 2, 3, 4, 5])
        node.insert(3, 518)
        self.assertEqual(node.keys, [0, 2, 3, 4, 6, 8])
        self.assertEqual(node.children, [0, 1, 518, 2, 3, 4, 5])

    def test_into_internal(self):
        Node = node_factory('<i')
        node = Node(False, [0, 2, 4, 6, 8], [0, 1, 2, 3, 4, 5])
        node.insert(3, 518)
        self.assertEqual(node.keys, [0, 2, 3, 4, 6, 8])
        self.assertEqual(node.children, [0, 1, 518, 2, 3, 4, 5])


if __name__ == '__main__':
    unittest.main()
