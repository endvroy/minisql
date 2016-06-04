from buffer_manager import BufferManager, pin
from struct import Struct
import math


class Record:
    # The format of header should be the same for all records files.
    header_format = '<ii'  # free_list_blk, free_list_record, records_amount (<0 means not exist)
    header_struct = Struct(header_format)

    def __init__(self, filename, format):
        self.buffer_manager = BufferManager()
        self.filename = filename
        real_format = format + 'i'
        # Each record in file has 2 extra info: next's block_off and next's record_off
        self.record_struct = Struct(real_format)
        self.rec_per_blk = math.floor(BufferManager.block_size / self.record_struct.size)
        self.first_free_rec, self.rec_amount = self._parse_header()

    def insert(self, attributes):
        """
        1. Insert will fail when append to a full block, but this should be handled
        2. by catalog manager when it provides the block offset and record offset.
        """
        record_info = attributes + (-1,)  # a negative, means this is a real record
        self.first_free_rec, self.rec_amount = self._parse_header()
        if self.first_free_rec >= 0:
            # There are space in free list
            first_free_blk = math.floor(self.first_free_rec / self.rec_per_blk)
            block = self.buffer_manager.get_file_block(self.filename, first_free_blk)
            with pin(block):
                data = block.read()
                records = self._parse_block_data(data, first_free_blk)
                next_free_rec = records[self.first_free_rec][-1]
                local_offset = self.first_free_rec - first_free_blk * self.rec_per_blk
                records[local_offset] = record_info
                new_data = self._generate_new_data(records, first_free_blk)
                block.write(new_data)
            position = self.first_free_rec
            self.first_free_rec = next_free_rec
        else:  # No space in free list, append the new record to the end of file
            self.rec_amount += 1
            block_offset = math.floor(self.rec_amount / self.rec_per_blk)
            block = self.buffer_manager.get_file_block(self.filename, block_offset)
            with pin(block):
                data = block.read()
                records = self._parse_block_data(data, block_offset)
                records.append(record_info)
                new_data = self._generate_new_data(records, block_offset)
                block.write(new_data)
            position = self.rec_amount
        self._update_header()
        return position

    def remove(self, record_offset):
        """Remove the record at specified position and update the free list"""
        self.first_free_rec, self.rec_amount = self._parse_header()
        block_offset = math.floor(record_offset / self.rec_per_blk)
        local_offset = record_offset - block_offset * self.rec_per_blk
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            try:
                records[local_offset][-1]
            except IndexError:
                raise IndexError('The offset points to a empty space')
            records[local_offset][-1] = self.first_free_rec  # A positive number, putting this position into free list
            self.first_free_rec = record_offset  # update the head of free list
            new_data = self._generate_new_data(records, block_offset)
            block.write(new_data)
        self._update_header()

    def modify(self, attributes, record_offset):
        """
          1. Assume that the provided record_offset must point to a real record
          2. During update, file header keeps invariant.
         """
        block_offset = math.floor(record_offset / self.rec_per_blk)
        local_offset = record_offset - block_offset * self.rec_per_blk
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        record_info = attributes + (-1,)  # Updated record must be real
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            records[local_offset] = record_info
            new_data = self._generate_new_data(records, block_offset)
            block.write(new_data)

    def read(self, record_offset):
        """ Return the record at the corresponding position """
        block_offset = math.floor(record_offset / self.rec_per_blk)
        local_offset = record_offset - block_offset * self.rec_per_blk
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
        return tuple(records[local_offset][:-1])

    def _generate_new_data(self, records, blk_offset):
        if blk_offset is 0:
            data = bytearray(self.header_struct.size)
        else:
            data = bytearray()
        for r in records:
            data += self.record_struct.pack(*r)
        return data

    def _parse_block_data(self, data, blk_offset):
        upper_bound = len(data)
        if (upper_bound - self.header_struct.size) % self.record_struct.size != 0:
            upper_bound -= self.record_struct.size
        if blk_offset is 0:  # is the first block, need to consider the header
            lower_bound = self.header_struct.size
        else:  # not the first block, all data are records
            lower_bound = 0
        records = [list(self.record_struct.unpack_from(data, offset))
                   for offset in range(lower_bound, upper_bound, self.record_struct.size)]
        return records

    def _parse_header(self):
        """Parse the file header, refresh corresponding info
            and return the info with a tuple"""
        block = self.buffer_manager.get_file_block(self.filename, 0)  # Get the first block
        with pin(block):
            data = block.read()
            header_info = self.header_struct.unpack_from(data, 0)
        return header_info

    def _update_header(self):
        """Update the file header after modifying the records"""
        block = self.buffer_manager.get_file_block(self.filename, 0)
        with pin(block):
            data = block.read()
            header_info = (self.first_free_rec, self.rec_amount)
            data[:self.header_struct.size] = self.header_struct.pack(*header_info)
            block.write(data)


class RecordManager:
    # 1. Need to receive the format of the record for corresponding table.
    # 2. Need to receive the table's name(or the filename of which saves the records).
    # 3. Need to receive the record's position in the corresponding block.
    # 4. All above are considered as metadata and should be manipulated by catalog manager,
    #  In the API module, high-level programs get the filename, record offset from
    #  catalog manager and index manager, and then call the corresponding methods
    #  provided by record manager.

    @staticmethod
    def insert(filename, format, attributes):
        record = Record(filename, format)
        position = record.insert(attributes)
        return position

    @staticmethod
    def delete(filename, format, record_offset):
        record = Record(filename, format)
        record.remove(record_offset)

    @staticmethod
    def update(filename, format, attributes, record_offset):
        record = Record(filename, format)
        record.modify(attributes, record_offset)

    @staticmethod
    def select(filename, format, record_offset):
        record = Record(filename, format)
        return record.read(record_offset)
