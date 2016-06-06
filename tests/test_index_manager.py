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
        node = Node(True, 765, [(42, 666, 'spam'), (233, 987, 'foo')], [518, 2, 42])
        octets = bytes(node)
        node2 = Node.frombytes(octets)
        self.assertEqual(node2.is_leaf, True)
        self.assertEqual(node2.parent, 765)
        self.assertEqual(node2.keys, [(42, 666, 'spam'), (233, 987, 'foo')])
        self.assertEqual(node2.children, [518, 2, 42])


if __name__ == '__main__':
    unittest.main()
