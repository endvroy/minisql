import os
import shutil
from collections import OrderedDict
import pickle
from functools import lru_cache


class Column:
    def __init__(self, name, fmt, *, primary_key=False, unique=False):
        self.name = name
        self.fmt = fmt
        self.primary_key = primary_key
        self.unique = unique

    def __iter__(self):
        return (x for x in [self.name, self.fmt, self.primary_key, self.unique])


class Index:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = OrderedDict()
        self.indexes = {}

    def add_column(self, column):
        self.columns[column.name] = column

    def add_index(self, index):
        self.indexes[index.name] = index

    def drop_index(self, index_name):
        if index_name not in self.indexes:
            raise ValueError('no index named {} on table {}'.format(index_name, self.name))

        del self.indexes[index_name]


class Metadata:
    def __init__(self):
        self.tables = {}

    def dump(self):
        with open('schema/metadata.pickle', 'wb') as file:
            pickle.dump(self, file, protocol=pickle.HIGHEST_PROTOCOL)

    def add_table(self, table_name, *columns):
        if table_name in self.tables:
            raise ValueError('already have a table named {}'.format(table_name))

        table = Table(table_name)
        primary_keys = []
        for column in columns:
            table.add_column(column)
            if column.primary_key:
                primary_keys.append(column)

        if not primary_keys:
            raise ValueError('primary key not specified')

        table.add_index(Index('PRIMARY', primary_keys))
        self.tables[table_name] = table

        # todo: create table file on disk?

        self.dump()

    def drop_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))

        shutil.rmtree('schema/tables/{}'.format(table_name))

        self.dump()

    def add_index(self, table_name, index_name, *columns):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))
        if not columns:
            raise ValueError('adding index on empty columns')

        self.tables[table_name].add_index(Index(index_name, columns))

        table_dir = 'schema/tables/{}'.format(table_name)
        with open(os.path.join(table_dir, '{}.index'.format(index_name)), 'wb') as file:
            pass

        self.dump()

    def drop_index(self, table_name, index_name):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))
        if index_name not in self.tables[table_name].indexes:
            raise ValueError('no index named {} on table {}'.format(index_name, table_name))

        del self.tables[table_name].indexes[index_name]

        os.remove('schema/tables/{table_name}/{index_name}.index'.format(table_name=table_name,
                                                                         index_name=index_name))
        self.dump()


def init():
    os.makedirs('schema/tables', exist_ok=True)


@lru_cache()
def load_metadata():
    try:
        with open('schema/metadata.pickle', 'r') as file:
            metadata = pickle.load(file)
            return metadata
    except FileNotFoundError:
        metadata = Metadata()
        with open('schema/metadata.pickle', 'w') as file:
            pickle.dump(metadata, file)
        return metadata
