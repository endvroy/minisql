from datetime import datetime
import heapq


class FreeBlock:
    state = 'free'

    def __init__(self, size):
        self.size = size
        self._memory = bytearray(size)

    def read(self):
        raise RuntimeError('Trying to read a free block')

    def write(self, data):
        raise RuntimeError('Trying to write into a free block')

    def flush(self):
        raise RuntimeError('Trying to flush a free block')

    def load(self, file, block_index):
        self.file = file
        self.block_index = block_index
        self.file.seek(self.size * block_index)
        self.effective_bytes = self.file.readinto(self._memory)
        self.dirty = False
        self.__class__ = OccupiedBlock

    def release(self):
        raise RuntimeError('Trying to release a free block')

    def pin(self):
        raise RuntimeError('Tring to pin a free block')

    def unpin(self):
        raise RuntimeError('Tring to unpin a free block')


class OccupiedBlockBase:
    def read(self):
        self.last_accessed_time = datetime.now()
        return self._memory[:self.effective_bytes]

    def write(self, data):
        self._memory[:self.effective_bytes] = data
        self.dirty = True
        self.last_accessed_time = datetime.now()

    def flush(self):
        if self.dirty:
            self.file.seek(self.block_index * self.size)
            self.file.write(self._memory[:self.effective_bytes])
            self.file.flush()
            self.dirty = False
        self.last_accessed_time = datetime.now()

    def load(self, file, block_index):
        raise RuntimeError('Trying to load into an occupied block')


class OccupiedBlock(OccupiedBlockBase):
    state = 'occupied'

    def release(self):
        self.flush()
        self.__class__ = FreeBlock

    def pin(self):
        self.__class__ = PinnedBlock

    def unpin(self):
        raise RuntimeError('Trying to unpin an already unpinned block')


class PinnedBlock(OccupiedBlockBase):
    state = 'pinned'

    def release(self):
        raise RuntimeError('Trying to release a pinned block')

    def pin(self):
        raise RuntimeError('Trying to pin an already pinned block')

    def unpin(self):
        self.__class__ = OccupiedBlock


class BufferManager:
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self.blocks = [FreeBlock(self.block_size) for i in range(self.total_blocks)]

    def _find_free_block(self):
        pass

    def get_file_block(self, file_path, block_index):
        free_block = self._find_free_block()
        with open(file_path, 'rb') as file:
            file.seek(block_index * self.block_size)
            file.readinto(free_block)
