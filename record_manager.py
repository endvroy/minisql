from buffer_manager import Block, BufferManager
from struct import Struct


class Record:
    def __init__(self, buffer_manager, filename, block_offset, record_offset, format):
        self.buffer_manager = buffer_manager
        self.filename = filename
        self.block_offset = block_offset
        self.record_offset = record_offset
        self.record_struct = Struct(format)

    def append(self, attributes):
        # 1. Write the new record into corresponding file of its table
        # 2. The write will fail when the remaining size of block is not enough
        block = self.buffer_manager.get_file_block(self.filename, self.block_offset)
        block.pin()
        block_content = block.read()
        block_content += (self.record_struct.pack(*attributes))  # not the best solution
        block.write(block_content)
        block.unpin()

    def modify(self, attributes):
        # Change the content of the record at specified position
        block = self.buffer_manager.get_file_block(self.filename, self.block_offset)
        block.pin()
        block_content = block.read()
        byte_offset = self.record_offset * self.record_struct.size
        block_content[byte_offset: byte_offset + self.record_struct.size - 1] = \
            self.record_struct.pack(*attributes)  # modify the corresponding record and then write back
        block.write(block_content)
        block.unpin()

    def get_content(self):
        #
        block = self.buffer_manager.get_file_block(self.filename, self.block_offset)
        block.pin()
        data = block.read()
        records = [self.record_struct.unpack_from(data, offset)
                   for offset in range(0, len(data), self.record_struct.size)]
        return records[self.record_offset]

    def remove(self):
        # Lazy delete the record at specified position
        pass


class RecordManager:
    # 1. Need to have the format of records for each table.(metadata, stored in catalog)
    # 2. Need to receive the table's name, and the block's offset in the file
    # 3. Need to receive the record's position in the corresponding block

    def __init__(self):
        self.buffer_manager = BufferManager()

    def insert(self, filename, block_offset, record_offset, attributes, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        record.append(attributes)
        return record_offset

    def update(self,filename, block_offset, record_offset, attributes, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        record.modify(attributes)

    def select(self, filename, block_offset, record_offset, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        return record.get_content()

    def delete(self):
        pass
