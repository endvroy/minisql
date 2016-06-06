from buffer_manager import BufferManager, pin
from struct import Struct
import math
import os


class Record:
    # The format of header should be the same for all records files.
    header_format = '<ii'  # will be confirmed by RecordManager
    header_struct = Struct(header_format)

    def __init__(self, file_path, fmt):
        self.buffer_manager = BufferManager()
        self.filename = file_path
        real_format = fmt + 'ci'
        # Each record in file has 2 extra info: next's record_off and valid bit
        self.record_struct = Struct(real_format)
        self.rec_per_blk = math.floor(BufferManager.block_size / self.record_struct.size)
        self.first_free_rec, self.rec_amount = self._parse_header()

    def insert(self, attributes):
        """
        1. Insert will fail when append to a full block, but this should be handled
        2. by catalog manager when it provides the block offset and record offset.
        """
        record_info = self._convert_str_to_bytes(attributes) + (b'1', -1)

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
                raise IndexError('The offset points to an empty space')
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot remove an empty record')
            records[local_offset][-1] = self.first_free_rec  # A positive number, putting this position into free list
            records[local_offset][-2] = b'0'
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
        record_info = self._convert_str_to_bytes(attributes) + (b'1', -1)  # Updated record must be real
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot update an empty record')
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
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot read an empty record')
        return self._convert_bytes_to_str(tuple(records[local_offset][:-2]))

    @staticmethod
    def _convert_str_to_bytes(attributes):
        """
        Convert the string attributes in the record to bytes,
        so that it can be received by struct.pack later.
        """
        attr_list = list(attributes)
        for index, item in enumerate(attr_list):
            if type(item) is str:
                attr_list[index] = item.encode('ASCII')
        return tuple(attr_list)

    @staticmethod
    def _convert_bytes_to_str(attributes):
        """
        When a record  is unpacked from binary files, those
        attributes with type string are still bytes.
        This function converts those bytes attributes into string,
        and then the standard record can be returned to user.
        """
        attr_list = list(attributes)
        for index, item in enumerate(attr_list):
            if type(item) is bytes:
                attr_list[index] = item.decode('ASCII').rstrip('\00')
        return tuple(attr_list)

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

    header_format = '<ii'  # free_list_record(negative means not exist) and  records_amount.
    header_struct = Struct(header_format)
    file_dir = './schema/tables/'

    @classmethod
    def init_table(cls, table_name):
        Record.header_format = cls.header_format  # confirm the corresponding info in Record
        Record.header_struct = cls.header_struct
        file_path = cls.file_dir + table_name + '.table'
        print(os.path.curdir)
        if os.path.exists(file_path):
            raise RuntimeError('The file for table \'{}\' has already exists'.format(table_name))
        else:
            with open(file_path, 'w+b') as file:
                file.write(cls.header_struct.pack(*(-1, 0)))

    @classmethod
    def insert(cls, table_name, fmt, attributes):
        """
            insert the given record into a suitable space,
            and return the offset of the inserted record
        """
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        position = record.insert(attributes)
        return position

    @classmethod
    def delete(cls, table_name, fmt, record_offset):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        record.remove(record_offset)

    @classmethod
    def update(cls, table_name, fmt, attributes, record_offset):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        record.modify(attributes, record_offset)

    @classmethod
    def select(cls, table_name, fmt, record_offset):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        return record.read(record_offset)
