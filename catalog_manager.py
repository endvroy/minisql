import json
import os
import shutil


class Column:
    def __init__(self, name, fmt, *, primary_key=False, unique=False):
        self.name = name
        self.fmt = fmt
        self.primary_key = primary_key
        self.unique = unique

    def __iter__(self):
        return (x for x in [self.name, self.fmt, self.primary_key, self.unique])


def init():
    os.makedirs('schema/tables', exist_ok=True)


def load_metadata():
    try:
        with open('schema/metadata.json', 'r') as file:
            metadata = json.load(file)
            return metadata
    except FileNotFoundError:
        metadata = {'tables': {}}
        with open('schema/metadata.json', 'w') as file:
            json.dump(metadata, file)
        return metadata


def add_table(metadata, table_name, *columns):
    if table_name in metadata['tables']:
        raise ValueError('already have a table named {}'.format(table_name))

    table_info = {
        'columns': {},
        'indexes': {}}

    primary_keys = []

    for column in columns:  # add columns
        name, fmt, pk, unique = column
        column_info = {
            'format': fmt,
            'primary_key': pk,
            'unique': unique}
        table_info['columns'][name] = column_info
        if pk:
            primary_keys.append(name)

    if not primary_keys:
        raise ValueError('primary key not specified')
    table_info['indexes']['PRIMARY'] = primary_keys
    metadata['tables'][table_name] = table_info

    # create files
    table_dir = 'schema/tables/{}'.format(table_name)
    os.makedirs('table_dir', exist_ok=True)
    with open(os.path.join(table_dir, '{}.table'.format(table_name)), 'wb') as file:
        pass
    with open(os.path.join(table_dir, 'PRIMARY.index'), 'wb') as file:
        pass
    # todo: write json back to file


def drop_table(metadata, table_name):
    if table_name not in metadata['tables']:
        raise ValueError('no table named {}'.format(table_name))

    shutil.rmtree('schema/tables/{}'.format(table_name))


def add_index(metadata, table_name, index_name, *columns):
    if table_name not in metadata['tables']:
        raise ValueError('no table named {}'.format(table_name))
    if not columns:
        raise ValueError('adding index on empty columns')

    metadata['tables'][table_name]['indexes'][index_name] = {'columns': columns}
    table_dir = 'schema/tables/{}'.format(table_name)
    with open(os.path.join(table_dir, '{}.index'.format(index_name)), 'wb') as file:
        pass


def drop_index(metadata, table_name, index_name):
    if table_name not in metadata['tables']:
        raise ValueError('no table named {}'.format(table_name))
    if index_name not in metadata['tables'][table_name]['indexes']:
        raise ValueError('no index named {} on table {}'.format(index_name, table_name))

    del metadata['tables'][table_name]['indexes'][index_name]
    os.remove('schema/tables/{table_name}/{index_name}.index'.format(table_name=table_name,
                                                                     index_name=index_name))
