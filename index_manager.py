import bisect
from struct import Struct
from math import ceil
from collections.abc import Sequence
from buffer_manager import BufferManager, pin


def _convert_to_tuple(element):
    if isinstance(element, Sequence):
        return tuple(element)
    else:
        return element,


def _convert_to_tuple_list(keys):
    return [_convert_to_tuple(x) for x in keys]


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
        #            sizeof(int) is_leaf next len_keys block_size

        def __init__(self, is_leaf, keys, children, next_deleted=0):
            self.is_leaf = is_leaf
            self.keys = _convert_to_tuple_list(keys)
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
            key = _convert_to_tuple(key)
            insert_position = bisect.bisect_left(self.keys, key)
            self.keys.insert(insert_position, key)
            self.children.insert(insert_position, value)

        def delete(self, key):
            key = _convert_to_tuple(key)
            key_position = bisect.bisect_left(self.keys, key)
            if key_position < len(self.keys) and self.keys[key_position] == key:
                child = self.children[key_position]
                del self.keys[key_position]
                del self.children[key_position]
                return key, child
            else:
                raise ValueError('key {} not in this node'.format(key))

        def fuse_with(self, other):
            if self.is_leaf and other.is_leaf:
                self.keys.extend(other.keys)
                del self.children[-1]
                self.children.extend(other.children)

            elif not self.is_leaf and not other.is_leaf:
                self.keys.extend(other.keys)
                self.children.extend(other.children)

            else:
                raise ValueError('can\'t fuse a leaf node with a non-leaf node')

        def _split(self):
            split_point = self.n // 2 + 1

            new_node = Node(self.is_leaf,
                            self.keys[split_point:],
                            self.children[split_point:])

            self.keys = self.keys[:split_point]
            self.children = self.children[:split_point]
            return new_node

        def split_and_write(self, block, new_block):
            with pin(block), pin(new_block):
                split_point = self.n // 2 + 1
                new_node = Node(self.is_leaf,
                                self.keys[split_point:],
                                self.children[split_point:])

                self.keys = self.keys[:split_point]
                self.children = self.children[:split_point]

                if self.is_leaf:
                    self.children.append(new_block.block_offset)  # maintain the leaf link
                    block.write(bytes(self))
                    new_block.write(bytes(new_node))
                    return new_node.keys[0], new_block.block_offset
                else:
                    key = self.keys.pop()  # remove the largest key in the left node
                    # this is faster than remove the smallest key in the right
                    block.write(bytes(self))
                    new_block.write(bytes(new_node))
                    return key, new_block.block_offset

    return Node


class IndexManager:
    def __init__(self, index_file_path, fmt):
        self.Node = node_factory(fmt)
        self.root = None
        self.index_file_path = index_file_path
        self.manager = BufferManager()
        self.meta_struct = Struct('<3i')
        try:
            meta_block = self.manager.get_file_block(self.index_file_path, 0)
            with pin(meta_block):
                self.total_blocks, self.first_deleted_block, self.root = self.meta_struct.unpack(
                    meta_block.read()[:self.meta_struct.size])
        except FileNotFoundError:
            self.total_blocks, self.first_deleted_block, self.root = 1, 0, 0
            with open(index_file_path, 'wb') as f:
                f.write(self.meta_struct.pack(self.total_blocks,
                                              self.first_deleted_block,
                                              self.root).ljust(4096, b'\0'))

    def dump_header(self):
        meta_block = self.manager.get_file_block(self.index_file_path, 0)
        with pin(meta_block):
            meta_block.write(self.meta_struct.pack(self.total_blocks,
                                                   self.first_deleted_block,
                                                   self.root).ljust(4096, b'\0'))

    def _get_free_block(self):
        if self.first_deleted_block > 0:
            block_offset = self.first_deleted_block
            block = self.manager.get_file_block(self.index_file_path, block_offset)
            s = Struct('<i')
            next_deleted = s.unpack(block.read()[:s.size])[0]
            self.first_deleted_block = next_deleted
            return block
        else:
            block_offset = self.total_blocks
            block = self.manager.get_file_block(self.index_file_path, block_offset)
            self.total_blocks += 1
            return block

    def _delete_node(self, node, block):
        with pin(block):
            node.next_deleted = self.first_deleted_block
            block.write(bytes(node))
            self.first_deleted_block = block.block_offset

    def _find_first_node(self, key):
        key = _convert_to_tuple(key)
        node_block_offset = self.root
        path_to_parents = []
        while True:  # find the insert position
            node_block = self.manager.get_file_block(self.index_file_path, node_block_offset)
            with pin(node_block):
                node = self.Node.frombytes(node_block.read())
                if node.is_leaf:
                    return node, node_block, path_to_parents
                else:  # continue searching
                    child_index = bisect.bisect_right(node.keys, key)
                    node_block_offset = node.children[child_index]
                    path_to_parents.append(node_block_offset)

    def find(self, key):
        key = _convert_to_tuple(key)
        if self.root == 0:
            return []
        else:
            results = []
            node, node_block, path_to_parents = self._find_first_node(key)
            key_position = bisect.bisect_left(node.keys, key)
            while True:
                if key_position < len(node.keys):
                    if node.keys[key_position] == key:
                        results.append(node.children[key_position])
                        key_position += 1
                    else:
                        return results
                else:  # jump
                    node_block = self.manager.get_file_block(self.index_file_path, node.children[-1])
                    with pin(node_block):
                        node = self.Node.frombytes(node_block.read())
                        key_position = 0

    def _insert_into_parents(self, key, value, path_to_parents):
        key = _convert_to_tuple(key)
        while True:  # recursively insert into parent
            node_block_offset = path_to_parents.pop()
            node_block = self.manager.get_file_block(self.index_file_path,
                                                     node_block_offset)
            with pin(node_block):
                node = self.Node.frombytes(node_block)
                node.insert(key, value)
                if len(node.keys) <= node.n:
                    node_block.write(bytes(node))
                    break
                else:  # split
                    new_block = self._get_free_block()
                    key, value = node.split_and_write(node_block, new_block)

                    if not path_to_parents:  # the root split; need a new root
                        new_root_block = self._get_free_block()
                        with pin(new_root_block):
                            new_root_node = self.Node(False,
                                                      keys=[key],
                                                      children=[node_block_offset, new_block.block_offset])
                            new_root_block.write(bytes(new_root_node))
                        self.root = new_root_block.block_offset
                        break

    def insert(self, key, value):
        key = _convert_to_tuple(key)
        if self.root == 0:
            block = self._get_free_block()
            with pin(block):
                self.root = block.block_offset
                node = self.Node(is_leaf=True,
                                 keys=[key],
                                 children=[value, 0])
                block.write(bytes(node))
        else:
            node, node_block, path_to_parents = self._find_first_node(key)
            node.insert(key, value)
            if len(node.keys) <= node.n:
                node_block.write(bytes(node))
                return
            else:  # split
                new_block = self._get_free_block()
                key, value = node.split_and_write(node_block, new_block)
                self._insert_into_parents(key, value, path_to_parents)

    def _handle_underflow(self, node, block, path_to_parents):
        if block.block_offset == self.root:
            if not node.keys:  # root has no key at all; this node is no longer needed
                if node.is_leaf:
                    self.root = 0
                else:
                    self.root = node.children[0]
                self._delete_node(node, block)
            else:
                block.write(bytes(node))
            return  # root underflow is not a problem

        parent_offset = path_to_parents.pop()
        parent_block = self.manager.get_file_block(self.index_file_path, parent_offset)
        with pin(parent_block):
            parent = self.Node.frombytes(parent_block.read())
            my_position = bisect.bisect_right(parent.keys, node.keys[0])

        if my_position > 0:  # try find the left sibling
            left_sibling_offset = parent.children[my_position - 1]
            left_sibling_block = self.manager.get_file_block(self.index_file_path,
                                                             left_sibling_offset)
            with pin(left_sibling_block):
                left_sibling = self.Node.frombytes(left_sibling_block)
            if len(left_sibling.keys) > ceil(node.n / 2):  # a transfer is possible
                node.keys.insert(parent.keys[my_position - 1], 0)
                node.children.insert(left_sibling.children.pop())
                parent.keys[my_position - 1] = left_sibling.keys.pop()
                with pin(block), pin(left_sibling_block), pin(parent_block):
                    block.write(bytes(node))
                    left_sibling_block.write(bytes(left_sibling))
                    parent_block.write(bytes(parent))
                return
        else:
            left_sibling = None  # no left sibling

        if my_position < len(parent.keys) - 1:  # try find the right sibling
            right_sibling_offset = parent.children[my_position + 1]
            right_sibling_block = self.manager.get_file_block(self.index_file_path,
                                                              right_sibling_offset)
            with pin(right_sibling_block):
                right_sibling = self.Node.frombytes(right_sibling_block.read())
            if len(right_sibling.keys) > ceil(node.n / 2):  # a transfer is possible
                node.keys.append(parent.keys[my_position])
                node.children.append(right_sibling.children.pop(0))
                parent.keys[my_position + 1] = right_sibling.keys.pop(0)
                with pin(block), pin(right_sibling_block), pin(parent_block):
                    block.write(bytes(node))
                    right_sibling_block.write(bytes(right_sibling))
                    parent_block.write(bytes(parent))
                return
        else:
            right_sibling = None  # no right sibling

        if left_sibling is not None:  # fuse with left sibling
            left_sibling.fuse_with(node)
            with pin(left_sibling_block):
                left_sibling_block.write(bytes(left_sibling))
            self._delete_node(node, block)
            del parent.keys[my_position - 1]
            del parent.children[my_position]
            if len(parent.keys) >= ceil(node.n / 2):
                return
            else:
                self._handle_underflow(parent, parent_block, path_to_parents)
        else:  # fuse with right sibling
            node.fuse_with(right_sibling)
            with pin(block):
                block.write(bytes())
            self._delete_node(right_sibling, right_sibling_block)
            del parent.keys[my_position]
            del parent.children[my_position + 1]
            if len(parent.keys) >= ceil(node.n / 2):
                return
            else:
                self._handle_underflow(parent, parent_block, path_to_parents)

    def delete(self, key):
        deleted_num = 0
        while True:
            if self.root == 0:
                return deleted_num
            else:
                key = _convert_to_tuple(key)
                node, node_block, path_to_parents = self._find_first_node(key)
                key_position = bisect.bisect_left(node.keys, key)
                if key_position < len(node.keys) and node.keys[key_position] == key:  # key match
                    del node.keys[key_position]
                    del node.children[key_position]
                    if len(node.keys) >= ceil(node.n / 2):
                        node_block.write(bytes(node))
                        return
                    else:  # underflow
                        self._handle_underflow(node, node_block, path_to_parents)
                    deleted_num += 1
                else:  # key doesn't match
                    return deleted_num
