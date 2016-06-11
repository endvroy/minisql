import bisect
from struct import Struct
from buffer_manager import BufferManager, pin


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

        def __init__(self, is_leaf, keys, children, next_deleted=0):
            self.is_leaf = is_leaf
            self.keys = list(keys)
            self.children = list(children)
            self.next_deleted = next_deleted

        def __bytes__(self):
            key_bytes = b''.join(self.key_struct.pack(*_encode_sequence(x)) for x in self.keys)
            children_struct = Struct('<{}i'.format(len(self.keys) + 1))
            return (self.meta_struct.pack(self.next_deleted, self.is_leaf, len(self.keys))
                    + key_bytes +
                    children_struct.pack(*self.children)).ljust(4096, b'\0')

        @classmethod
        def frombytes(cls, octets):
            next_deleted, is_leaf, len_keys = cls.meta_struct.unpack(octets[:cls.meta_struct.size])
            keys = [_decode_sequence(cls.key_struct.unpack(chunk))
                    for chunk in iter_chunk(octets, cls.meta_struct.size, cls.key_struct.size, len_keys)]
            children_struct = Struct('<{}i'.format(len_keys + 1))
            children_offset = cls.meta_struct.size + len_keys * cls.key_struct.size
            children = list(children_struct.unpack(octets[children_offset:children_offset + children_struct.size]))
            return cls(is_leaf, keys, children, next_deleted)

        def insert(self, key, value):
            insert_position = bisect.bisect(self.keys, key)
            self.keys.insert(insert_position, key)
            self.children.insert(insert_position, value)

        def split(self):
            if self.is_leaf:
                split_point = self.n // 2
            else:
                split_point = self.n // 2 + 1

            new_node = Node(self.is_leaf,
                            self.keys[split_point:],
                            self.children[split_point:])

            self.keys = self.keys[:split_point]
            self.children = self.children[:split_point]
            return new_node

    class Tree:
        def __init__(self, index_file_name):
            self.root = None
            self.index_file_name = index_file_name
            self.manager = BufferManager()
            self.meta_struct = Struct('<2i')
            meta_block = self.manager.get_file_block(self.index_file_name, 0)
            with pin(meta_block):
                self.total_blocks, self.first_deleted_block = self.meta_struct.unpack(
                    meta_block.read()[:self.meta_struct.size])

        def find(self, key):
            if self.root is None:
                return None
            node_offset = self.root
            while True:
                node_block = self.manager.get_file_block(self.index_file_name, node_offset)
                with pin(node_block):
                    node = Node.frombytes(node_block.read())
                    if node.is_leaf:
                        key_position = bisect.bisect(node.keys, key)
                        if key_position < len(node.keys) and node.keys[key_position] == key:
                            return node.children[key_position]
                        else:
                            return None
                    else:
                        child_index = bisect.bisect_right(node.keys, key)
                        node_offset = node.children[child_index]

        def _find_free_block_offset(self):
            if self.first_deleted_block > 0:
                return self.first_deleted_block
            else:
                return self.total_blocks + 1

        def _get_free_block(self):
            if self.first_deleted_block > 0:
                block_offset = self.first_deleted_block
                block = self.manager.get_file_block(self.index_file_name, block_offset)
                s = Struct('<i')
                next_deleted = s.unpack(block.read()[:s.size])[0]
                self.first_deleted_block = next_deleted
                return block, block_offset
            else:
                block_offset = self.total_blocks + 1
                block = self.manager.get_file_block(self.index_file_name, block_offset)
                self.total_blocks += 1
                return block



        def insert(self, key, value):
            if self.root is None:
                block = self._get_free_block()
                with pin(block):
                    self.root = block.offset
                    node = Node(is_leaf=True,
                                keys=[key],
                                children=[value, 0])
                    block.write(bytes(node))
            else:
                node_block_offset = self.root
                path_to_root = []
                while True:  # find the insert position
                    node_block = self.manager.get_file_block(self.index_file_name, node_block_offset)
                    with pin(node_block):
                        node = Node.frombytes(node_block.read())
                        if not node.is_leaf:  # continue searching
                            child_index = bisect.bisect_right(node.keys, key)
                            node_block_offset = node.children[child_index]
                            path_to_root.append(node_block_offset)

                        else:  # start inserting
                            node.insert(key, value)
                            if len(node.keys) <= node.n:
                                node_block.write(bytes(node))
                                break
                            else:  # split
                                new_node = node.split()
                                new_block = self._get_free_block()
                                node.children.append(new_block.offset)
                                node_block.write(bytes(node))
                                with pin(new_block):
                                    new_block.write(bytes(new_node))

                                key, value = new_node.keys[0], new_block.offset
                                while path_to_root:  # recursively insert into parent
                                    node_block_offset = path_to_root.pop()
                                    node_block = self.manager.get_file_block(self.index_file_name,
                                                                             node_block_offset)
                                    with pin(node_block):
                                        node = Node.frombytes(node_block)
                                        node.insert(key, value)
                                        if len(node.keys) <= node.n:
                                            node_block.write(bytes(node))
                                            break
                                        else:
                                            new_node = node.split()
                                            new_block, new_block.offset = self._get_free_block()
                                            with pin(new_block):
                                                new_block.write(bytes(new_node))
                                            key, value = node.keys.pop(), new_block.offset
                                            if not path_to_root:
                                                new_root_block, new_root_offset = self._get_free_block()
                                                with pin(new_root_block):
                                                    new_root_node = Node(False,
                                                                         keys=[node.keys.pop()],
                                                                         children=[node_block_offset, new_block.offset])
                                                    new_root_block.write(bytes(new_root_node))
                                                    self.root = new_root_offset

                            break

        def delete(self, key):
            pass

    return Node
