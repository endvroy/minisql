from buffer_manager import Block, BufferManager
from struct import Struct


class Record:
    def __init__(self, buffer_manager, filename, block_offset, record_offset, format):
        self.buffer_manager = buffer_manager
        self.filename = filename
        self.block_offset = block_offset
        self.record_offset = record_offset
        self.record_struct = Struct(format)

    def write(self, attributes, mode):
        # Write will fail when append to a full block, but this should be handled
        # by catalog manager when it provides the block offset and record offset.
        block = self.buffer_manager.get_file_block(self.filename, self.block_offset)
        block.pin()
        data = block.read()
        # Get the list of all tuples in the block
        upper_bound = len(data)
        if upper_bound % self.record_struct.size != 0:
            upper_bound -= self.record_struct.size
        records = [self.record_struct.unpack_from(data, offset)
                   for offset in range(0, upper_bound, self.record_struct.size)]
        # Determine the operation to be executed
        if mode is 'append':
            records.append(attributes)
        elif mode is 'modify':
            records[self.record_offset] = attributes
        elif mode is 'delete':
            del records[self.record_offset]
        else:
            raise RuntimeError('Wrong mode for writing the record')
        new_data = bytearray()
        for r in records:
            new_data += self.record_struct.pack(*r)
        block.write(new_data)
        block.unpin()

    def read(self):
        block = self.buffer_manager.get_file_block(self.filename, self.block_offset)
        block.pin()
        data = block.read()
        block.unpin()
        records = [self.record_struct.unpack_from(data, offset)
                   for offset in range(0, len(data), self.record_struct.size)]
        if len(records) <= self.record_offset:
            raise RuntimeError('The record offset out of range')
        return records[self.record_offset]

    def remove(self):
        # Whether to use lazy deletion?
        pass


class RecordManager:
    # 1. Need to receive the format of the record for corresponding table.
    # 2. Need to receive the table's name(or the filename), and the block's offset in the file.
    # 3. Need to receive the record's position in the corresponding block.
    # 4. All above are considered as metadata and should be manipulated by catalog manager,
    #  In the API module, high-level programs get the filename, block offset, record offset from
    # catalog manager, and then call the corresponding methods provided by record manager.

    def __init__(self):
        self.buffer_manager = BufferManager()

    def insert(self, filename, block_offset, record_offset, attributes, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        record.write(attributes, mode='append')
        return record_offset

    def update(self, filename, block_offset, record_offset, attributes, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        record.write(attributes, mode='modify')

    def select(self, filename, block_offset, record_offset, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        return record.read()

    def delete(self, filename, block_offset, record_offset, format):
        record = Record(self.buffer_manager, filename, block_offset, record_offset, format)
        record.write((), mode='delete')
