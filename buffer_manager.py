class FileBlockView:
    def __init__(self, size, file=None, block_index=0):
        self.dirty = False
        self.pinned = False
        self.size = size
        self._memory = bytearray(size)
        self.file = file
        self.block_index = block_index
        if file is None:
            self.free = True
        else:
            self.free = False

    @property
    def memory(self):
        return self._memory

    @memory.setter
    def memory(self, data):
        self._memory = data
        self.dirty = True

    def pin(self):
        if self.free:
            raise RuntimeError('Trying to pin a free block')
        else:
            self.pinned = True

    def unpin(self):
        if self.free:
            raise RuntimeError('Trying to unpin a free block')
        if self.pinned:
            self.pinned = False
        else:
            raise RuntimeError('Trying to unpin an already unpinned block')

    def flush(self):
        if self.dirty:
            self.file.seek(self.block_index * self.size)
            self.file.write(self.memory)
            self.dirty = False

    def release(self):
        if self.pinned:
            raise RuntimeError('Trying to release a pinned block')
        else:
            self.flush()
            self.free = True


class BufferManager:
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self.blocks = [FileBlockView(self.block_size) for i in range(self.total_blocks)]

    def _find_free_block(self):
        pass

    def get_file_block(self, file_path, block_index):
        free_block = self._find_free_block()
        with open(file_path, 'rb') as file:
            file.seek(block_index * self.block_size)
            file.readinto(free_block)
