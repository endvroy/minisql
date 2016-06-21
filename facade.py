from index_manager import IndexManager
from record_manager import RecordManager
from catalog_manager import Metadata, init, load_metadata, Column
from collections import defaultdict
import os
import shutil


# 每次对表进行操作的时候都需要检查表是否存在？

# v1.0 assume each index in built only on a single key now.

# v1.2 use the index_dump() and delete_
# BUGs
# cannot insert the same record after deleting the old one
# exit() needs to be added
class MinisqlFacade:
    @staticmethod
    def create_table(table_name, primary_key, columns):
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
        position = RecordManager.insert(table_name, metadata.tables[table_name].fmt, tuple(attributes))
        for index_name, index in metadata.tables[table_name].indexes.items():
            file_path = RecordManager.file_dir + index_name + '.index'
            fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
            manager = IndexManager(file_path, fmt)
            key_pos = list(metadata.tables[table_name].columns.keys()).index(index.columns[0])
            key_list = list()
            key_list.append(attributes[key_pos])
            manager.insert(key_list, position)  # index can be set on single attribute
            manager.dump_header()

    @staticmethod
    def create_index(table_name, index_name, column_name):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        position = -1
        metadata = load_metadata()
        metadata.add_index(table_name, index_name, column_name)
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        file_path = RecordManager.file_dir + index_name + '.index'
        table_target = metadata.tables[table_name]
        fmt = ''.join(table_target.columns[column].fmt for column in table_target.indexes[index_name].columns)
        manager = IndexManager(file_path, fmt)
        for record in records:
            key_pos = list(metadata.tables[table_name].columns.keys()).index(column_name)
            key_list = list()
            key_list.append(record[key_pos])
            position += 1
            manager.insert(key_list, position)
        metadata.dump()
        manager.dump_header()

    @staticmethod
    def select_record_all(table_name):
        metadata = load_metadata()
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        print(metadata.tables[table_name].fmt)
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        return records

    @staticmethod
    def delete_record_all(table_name):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        for index in metadata.tables[table_name].indexes:
            os.remove('schema/tables/' + table_name + '/' + index + '.index')

            # while condition_pos < len(conditions):
            #     attribute = conditions[condition_pos][0]
            #     for index_name, index in metadata.tables[table_name].indexes.items():
            #         if attribute in index.columns:
            #             file_path = RecordManager.file_dir + index_name + '.index'  #
            #             fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
            #             manager = IndexManager(file_path, fmt)
            #             offset = manager.find(conditions[condition_pos][2])
            #             for rec_offset in offset:
            #                 records += RecordManager.select(table_name,
            #                                                 metadata.tables[table_name].fmt,
            #                                                 with_index=1,
            #                                                 record_offset=rec_offset,
            #                                                 conditions={})
            #             return records
            # records = RecordManager.select(table_name,
            #                                metadata.tables[table_name].fmt,
            #                                with_index=0,
            #                                record_offset=0,
            #                                conditions=conditions)
            # return records

    # @staticmethod
    # def delete_record_conditionally(table_name, conditions):
    #     RecordManager.set_file_dir('schema/tables/' + table_name + '/')
    #     metadata = load_metadata()
    #     records = select_record_conditionally(table_name, conditions)
    #     for record in records:
    #         for index_name, index in metadata.tables[table_name].indexes.items():
    #             has_index = 1
    #             file_path = RecordManager.file_dir + index_name + '.index'
    #             fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
    #             manager = IndexManager(file_path, fmt)
    #             key_pos = list(metadata.tables[table_name].columns.keys()).index(
    #                 index.columns[0])  # index can be added on single column
    #             key = record[key_pos]
    #             value = manager.find(key)
    #             manager.delete(key)
    #             manager.dump_header()
    #         if has_index:
    #             has_index = 0
    #             RecordManager.delete(table_name,
    #                                  metadata.tables[table_name].fmt,
    #                                  with_index=True,
    #                                  record_offset=value,
    #                                  conditions={})
    #         else:
    #             RecordManager.delete(table_name,
    #                                  metadata.tables[table_name].fmt,
    #                                  with_index=False,
    #                                  conditions=conditions)

    @staticmethod
    def _convert_conditions(table_name, condition):
        metadata = load_metadata()
        key_pos = list(metadata.tables[table_name].columns.keys()).index(condition[0])
        condition_inter = dict()
        condition_convert = dict()
        condition_inter[condition[1]] = condition[2]
        condition_convert[key_pos] = condition_inter
        return condition_convert


    @staticmethod
    def _convert_conditions_dual(table_name, *conditions):
        metadata = load_metadata()
        if conditions[0][0] == conditions[1][0]:
            key_pos = list(metadata.tables[table_name].columns.keys()).index(conditions[0][0])
            condition_inter = dict()
            condition_convert = dict()
            condition_inter[conditions[0][1]] = conditions[0][2]
            condition_inter[conditions[1][1]] = conditions[1][2]
            condition_convert[key_pos] = condition_inter        
            return condition_convert
        else:
            condition_convert = dict()
            for condition in conditions:
                key_pos = list(metadata.tables[table_name].columns.keys()).index(condition[0])
                condition_inter = dict()
                condition_inter[condition[1]] = condition[2]
                condition_convert[key_pos] = condition_inter
            return condition_convert
    
    @staticmethod
    def select_record_conditionally_without_index(table_name, conditions):  # support only equivalent search
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        if len(conditions) == 1:
            condition = MinisqlFacade._convert_conditions(table_name, conditions[0])
            records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition)
        elif len(conditions) == 3:
            condition_1 = MinisqlFacade._convert_conditions(table_name, conditions[0])
            condition_2 = MinisqlFacade._convert_conditions(table_name, conditions[2])
            record_1 = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition_1)
            record_2 = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition_2)
            if conditions[1] == 'and':
                records = list(set(record_1).intersection(set(record_2)))
            elif conditions[1] == 'or':
                records = list(set(record_1).union(set(record_2)))
            else:
                pass
            # link the records outside
        else:
            pass
        return records

    @staticmethod
    def delete_record_conditionally_without_index(table_name, conditions):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        if len(conditions) == 1:
            condition = MinisqlFacade._convert_conditions(table_name, conditions[0])
            RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition)
        elif len(conditions) == 3:
            if conditions[1] == 'and':
                condition = MinisqlFacade._convert_conditions_dual(table_name, conditions[0], conditions[2])
                RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition)
            elif conditions[1] == 'or':
                condition_1 = MinisqlFacade._convert_conditions(table_name, conditions[0])
                condition_2 = MinisqlFacade._convert_conditions(table_name, conditions[2])
                RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition_1)
                RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions=condition_2)
            else:
                pass
            # link the records outside
        else:
            pass

    @staticmethod
    def drop_table(table_name):
        metadata = load_metadata()
        metadata.drop_table(table_name)
        shutil.rmtree('schema/tables/' + table_name + '/', True)
        metadata.dump()

    @staticmethod
    def drop_index(index_name):
        metadata = load_metadata()
        for table_name, table in metadata.tables.items():
            if index_name in table.indexes:
                metadata.drop_index(table_name, index_name)
                os.remove('schema/tables/' + table_name + '/' + index_name + '.index')
        metadata.dump()
