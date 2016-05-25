from abc import ABC, abstractmethod
from datetime import datetime
import heapq


class AbstractBlock(ABC):
    """This class is an Abstract Base Class (ABC)

     it does nothing in itself other than specify the interface that
     all concrete classes inheriting from it share

    all these concrete classes form a state machine

    because of my hackish way to implement the state machine,
    beware NOT to apply class decorators or descriptors
    to this class or all classes inheriting from it,
    otherwise bizarre problems may occur"""

    @abstractmethod
    def read(self):
        """read a block of data from memory"""

    @abstractmethod
    def write(self, data):
        """write data into memory"""

    @abstractmethod
    def flush(self):
        """write data from memory to file"""

    @abstractmethod
    def load(self, file, block_index):
        """load a block of data into memory"""

    @abstractmethod
    def release(self):
        """write data to file and detach with it"""

    @abstractmethod
    def pin(self):
        """pin this block so that it cannot be released"""

    @abstractmethod
    def unpin(self):
        """unpin this block so that it can be released"""


class FreeBlock(AbstractBlock):
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

        # beware that the remaining data in the file may not be enough to fill the whole block!
        # self.effective_bytes store how many bytes are really loaded into memory
        self.effective_bytes = self.file.readinto(self._memory)

        self.dirty = False

        self.__class__ = OccupiedBlock  # change the class of the object
        # it is a hackish way to implement a state machine, but it does work

    def release(self):
        raise RuntimeError('Trying to release a free block')

    def pin(self):
        raise RuntimeError('Trying to pin a free block')

    def unpin(self):
        raise RuntimeError('Trying to unpin a free block')


class OccupiedBlockBase(AbstractBlock):
    def read(self):
        # update last accessed time to support LRU swap algorithm
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


# this class is not finished yet
class BufferManager:
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self.blocks = [FreeBlock(self.block_size) for i in range(self.total_blocks)]

    def _find_free_block(self):
        pass

    def get_file_block(self, file_path, block_index):
        free_block = self._find_free_block()
        with open(file_path, 'r+b') as file:
            file.seek(block_index * self.block_size)
            file.readinto(free_block)
