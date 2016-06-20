from index_manager import IndexManager
from record_manager import RecordManager
from catalog_manager import Metadata, init, load_metadata, Column
from collections import defaultdict
import os
import shutil


# v1.0 assume each index in built only on a single key now.

class MinisqlFacade:
    @staticmethod
    def create_table(table_name, primary_key, columns):
        # column : (col_name, (type, length_for_char), UNIQUE)
        os.makedirs('schema/tables/' + table_name, exist_ok=True)
        metadata = load_metadata()  # PK can be set on only one attribute
        columns_lst = []
        print(columns)
        for column in columns:
            # generate the fmt for this column
            if column[1][0] == 'char':
                fmt = str(column[1][1]) + 's'
            elif column[1][0] == 'int':
                fmt = str(column[1][1]) + 'i'
            else:
                fmt = str(column[1][1]) + 'd'
            columns_lst.append(Column(column[0], fmt, primary_key=(column[0] == primary_key), unique=column[2]))
        metadata.add_table(table_name, *columns_lst)
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        RecordManager.init_table(table_name)
        metadata.dump()

    @staticmethod
    def insert_record(table_name, attributes):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        print(attributes)
        position = RecordManager.insert(table_name, metadata.tables[table_name].fmt, tuple(attributes))
        print('indexes are ', metadata.tables[table_name].indexes)
        for index_name, index in metadata.tables[table_name].indexes.items():
            file_path = RecordManager.file_dir + index_name + '.index'
            fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
            manager = IndexManager(file_path, fmt)
            key_pos = list(metadata.tables[table_name].columns.keys()).index(index.columns[0])
            key_list = list()
            key_list.append(attributes[key_pos])
            print('key is ', attributes[key_pos])
            manager.insert(key_list, position)  # index can be set on single attribute

    # @staticmethod
    # def create_index(table_name, index_name, column_name):
    #     RecordManager.set_file_dir('schema/tables/' + table_name + '/')
    #     position = -1
    #     metadata = load_metadata()
    #     metadata.add_index(table_name, index_name, column_name)
    #     records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=0, record_offset=0,
    #                                    conditions={})
    #     for record in records:
    #         file_path = RecordManager.file_dir + index.name + '.index'
    #         manager = IndexManager(file_path, metadata.tables[table_name].fmt)
    #         key_pos = list(metadata.tables[table_name].columns.keys()).index(column_name)
    #         key = record[key_pos]
    #         position += 1
    #         manager.insert(key, position)
    #     # position needs to be determined
    #     metadata.dump()

    @staticmethod
    def select_record_all(table_name):
        metadata = load_metadata()
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        return records

    @staticmethod
    def delete_record_all(table_name):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})

    # @staticmethod
    # def select_record_conditionally(table_name, conditions):  # support only equivalent search
    #     RecordManager.set_file_dir('schema/tables/' + table_name + '/')
    #     metadata = load_metadata()
    #     condition_pos = 0
    #     while condition_pos < len(conditions):
    #         attribute = conditions[condition_pos][0]
    #         for (index_name, index) in metadata.tables[table_name].indexes:
    #             if attribute in index.columns:
    #                 file_path = RecordManager.file_dir + index.name + '.index'  #
    #                 manager = IndexManager(file_path, metadata.tables[table_name].fmt)
    #                 offset = manager.find(conditions[condition_pos][2])
    #                 for rec_offset in offset:
    #                     records += RecordManager.select(table_name,
    #                                                     metadata.tables[table_name].fmt,
    #                                                     with_index=1,
    #                                                     record_offset=rec_offset,
    #                                                     conditions={})
    #                 return records
    #     records = RecordManager.select(table_name,
    #                                    metadata.tables[table_name].fmt,
    #                                    with_index=0,
    #                                    record_offset=0,
    #                                    conditions=conditions)
    #     return records

    # @staticmethod
    # def delete_record_conditionally(table_name, conditions):
    #     RecordManager.set_file_dir('schema/tables/' + table_name + '/')
    #     metadata = load_metadata()
    #     records = select_record_conditionally(table_name, conditions)
    #     for record in records:
    #         for (index_name, index) in metadata.tables[table_name].indexes:
    #             has_index = 1
    #             file_path = RecordManager.file_dir + index.name + '.index'
    #             manager = IndexManager(file_path, metadata.tables[table_name].fmt)
    #             key_pos = list(metadata.tables[table_name].columns.keys()).index(
    #                 index.columns[0])  # index can be added on single column
    #             key = record[key_pos]
    #             value = manager.find(key)
    #             manager.delete(key)
    #         if has_index:
    #             has_index = 0
    #             RecordManager.delete(table_name,
    #                                  metadata.tables[table_name].fmt,
    #                                  with_index=1,
    #                                  record_offset=value,
    #                                  conditions={})
    #         else:
    #             RecordManager.delete(table_name,
    #                                  metadata.tables[table_name].fmt,
    #                                  with_index=0,
    #                                  record_offset=None,
    #                                  conditions=conditions)

    @staticmethod
    def drop_table(table_name):
        metadata = load_metadata()
        metadata.drop_table(table_name)
        shutil.rmtree('schema/tables/' + table_name + '/', True)
        metadata.dump()

        # @staticmethod
        # def drop_index(index_name):
        #     metadata = load_metadata()
        #     for (table_name, table) in metadata.tables:
        #         if index_name in table.indexes:
        #             metadata.drop_index(table_name, index_name)
        #             shutil.rmtree('schema/tables/' + table_name + '/' + index_name + '.index', True)
        #     metadata.dump()
