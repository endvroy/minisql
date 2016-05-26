from datetime import datetime
import heapq
import os


class Block:
    def __init__(self, size, file_path, block_offset):
        self.size = size
        self._memory = bytearray(size)
        self.file = open(file_path, 'r+b')
        self.block_offset = block_offset
        self.file.seek(self.size * block_offset)

        # beware that the remaining data in the file may not be enough to fill the whole block!
        # self.effective_bytes store how many bytes are really loaded into memory
        self.effective_bytes = self.file.readinto(self._memory)

        self.dirty = False
        self.pin_count = 0

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

    def pin(self):
        """pin this block so that it cannot be released"""
        self.pin_count += 1

    def unpin(self):
        """unpin this block so that it can be released"""
        if self.pin_count > 0:
            self.pin_count -= 1
        else:
            raise RuntimeError('this block is already unpinned')

    def free(self):
        """write data to file and close related file"""
        if self.pin_count == 0:
            self.flush()
            self.file.close()
        else:
            raise RuntimeError('Trying to free a pinned block')


# this class is not finished yet
class BufferManager:
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self._blocks = {}

    def get_file_block(self, file_path, block_offset):
        abs_path = os.path.abspath(file_path)
        if (abs_path, block_offset) in self._blocks:
            # found a cached block
            return self._blocks[(abs_path, block_offset)]
        elif len(self._blocks) < self.total_blocks:
            # has free space
            block = Block(self.block_size, abs_path, block_offset)
            self._blocks[(abs_path, block_offset)] = block
            return block
        else:
            # buffer is full; try to swap out the lru block
            lru_key = None
            lru_block = None
            for key, block in self._blocks.items():
                if block.pin_count == 0:
                    if lru_block is None or block.last_accessed_time < lru_block.last_accessed_time:
                        lru_key = key
                        lru_block = block
            if lru_block is None:
                raise RuntimeError('All blocks are pinned, buffer ran out of blocks')
            else:
                lru_block.free()
                del self._blocks[lru_key]
                block = Block(self.block_size, abs_path, block_offset)
                self._blocks[(abs_path, block_offset)] = block
                return block

    def flush_all(self):
        for block in self._blocks:
            block.flush()

    def free(self):
        for block in self._blocks.values():
            block.pin_count = 0
            block.free()
