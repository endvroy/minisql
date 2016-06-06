import bisect
from struct import Struct


def _encode(element):
    if isinstance(element, str):
        return element.encode('ascii')
    else:
        return element


def _encode_sequence(sequence):
    return tuple(_encode(x) for x in sequence)


def _decode(element):
    if isinstance(element, bytes):
        return element.decode('ascii').rstrip('\0')
    else:
        return element


def _decode_sequence(sequence):
    return tuple(_decode(x) for x in sequence)


def iter_chunk(octets, offset, chunk_size, total_chunks):
    for i in range(total_chunks):
        yield octets[offset + i * chunk_size: offset + (i + 1) * chunk_size]


def node_factory(fmt):
    class Node:
        key_struct = Struct(fmt)
        meta_struct = Struct('<3i')
        n = (4096 - 16) // (4 + key_struct.size)

        # N * key_size + 4 * (N + 1) + 4 + 4 + 4 <= 4096
        #                -             -   -   -    ----
        #                ^             ^   ^   ^       ^
        #                |             /   |   \       \
        #            sizeof(int) is_leaf parent len_keys block_size

        def __init__(self, is_leaf, parent, keys, children):
            self.is_leaf = is_leaf
            self.parent = parent
            self.keys = list(keys)
            self.children = list(children)

        def __bytes__(self):
            key_bytes = b''.join(self.key_struct.pack(*_encode_sequence(x)) for x in self.keys)
            children_struct = Struct('<{}i'.format(len(self.keys) + 1))
            return self.meta_struct.pack(self.is_leaf, self.parent, len(self.keys)) \
                   + key_bytes + \
                   children_struct.pack(*self.children)

        @classmethod
        def frombytes(cls, octets):
            is_leaf, parent, len_keys = cls.meta_struct.unpack(octets[:3 * 4])
            keys = [_decode_sequence(cls.key_struct.unpack(chunk))
                    for chunk in iter_chunk(octets, 3 * 4, cls.key_struct.size, len_keys)]
            children_struct = Struct('<{}i'.format(len_keys + 1))
            children = list(children_struct.unpack(octets[3 * 4 + len_keys * cls.key_struct.size:]))
            return cls(is_leaf, parent, keys, children)

    return Node
