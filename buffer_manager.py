from datetime import datetime
import heapq
import os


class Block:
    def __init__(self, size, file, block_offset):
        self.size = size
        self._memory = bytearray(size)
        self.file = file
        self.block_offset = block_offset
        self.file.seek(self.size * block_offset)

        # beware that the remaining data in the file may not be enough to fill the whole block!
        # self.effective_bytes store how many bytes are really loaded into memory
        self.effective_bytes = self.file.readinto(self._memory)

        self.dirty = False
        self.pinned = True

        self.last_accessed_time = datetime.now()

    def read(self):
        """read a block of data from memory"""
        # update last accessed time to support LRU swap algorithm
        self.last_accessed_time = datetime.now()
        return self._memory[:self.effective_bytes]

    def write(self, data):
        """write data into memory"""
        self._memory[:self.effective_bytes] = data
        self.dirty = True
        self.last_accessed_time = datetime.now()

    def flush(self):
        """write data from memory to file"""
        if self.dirty:
            self.file.seek(self.block_offset * self.size)
            self.file.write(self._memory[:self.effective_bytes])
            self.file.flush()
            self.dirty = False
        self.last_accessed_time = datetime.now()

    def release(self):
        """write data to file and become volatile"""
        self.flush()
        self.pinned = False

    def _pin(self):
        """pin this block so that it cannot be released"""
        if self.pinned:
            raise RuntimeError('Trying to pin an already pinned block')
        else:
            self.pinned = True

    def _unpin(self):
        """unpin this block so that it can be released"""
        if self.pinned:
            self.pinned = False
        else:
            raise RuntimeError('Trying to unpin an already unpinned block')


# this class is not finished yet
class BufferManager:
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self.blocks = {}

    def get_file_block(self, file_path, block_offset):
        abs_path = os.path.abspath(file_path)
        if (abs_path, block_offset) in self.blocks:
            # found a cached block
            return self.blocks[(abs_path, block_offset)]
        elif len(self.blocks) < self.total_blocks:
            # has free space
            with open(abs_path, 'r+b') as file:
                block = Block(self.block_size, file, block_offset)
                self.blocks[(abs_path, block_offset)] = block
                return block
        else:
            # buffer is full; try to swap out the lru block
            lru_key = None
            lru_block = None
            for key, block in self.blocks.items():
                if not block.pinned:
                    if lru_block is None or block.last_accessed_time < lru_block.last_accessed_time:
                        lru_key = key
                        lru_block = block
            if lru_block is None:
                raise RuntimeError('All blocks are pinned, buffer ran out of blocks')
            else:
                lru_block.release()
                del self.blocks[lru_key]
                with open(abs_path, 'r+b') as file:
                    block = Block(self.block_size, file, block_offset)
                    self.blocks[(abs_path, block_offset)] = block
                    return block
