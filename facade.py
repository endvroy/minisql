from index_manager import IndexManager, Index
from record_manager import RecordManager
from catalog_manager import MetaData, init
import os
import shutil
#每次对表进行操作的时候都需要检查表是否存在？

def create_table(table_name, *columns):
	os.makedirs('schema/tables/'+table_name, exist_ok=True)
	metadata = load_metadata()			#the *columns needs to be reversed
	metadata.add_table(table_name, *columns)


def insert_record(table_name, attributes):
	metadata = load_metadata()
	index = Index()
	RecordManager.init_table(table_name)
	position = RecordManager.insert(table_name, metadata.tables[table_name].fmt, attributes)
	for index.name in metadata.tables[table_name].indexes :
		file_path = RecordManager.file_dir + table_name + index.name + '.index' 
		manager = IndexManager(file_path,metadata.tables[table_name].fmt)
		key_pos = list(metadata.tables[table_name].columns.keys()).index(list(index.columns.keys())[0]) #test
		key = attributes[key_pos]
		manager.insert(key,position)


def create_index(table_name, index_name, column_name):
	position = -1
	metadata = load_metadata()
	metadata.add_index(table_name, index_name, column_name)
	records = RecordManager.select(table_name, metadata.tables[table_name].fmt, *, 0, *, {})
	for record in records :
		file_path = RecordManager.file_dir + table_name + index.name + '.index' 
		manager = IndexManager(file_path,metadata.tables[table_name].fmt)
		key_pos = list(metadata.tables[table_name].columns.keys()).index(column_name)
		key = record[key_pos]
		manager.insert(key,position+=1)		
		#position needs to be determined


def select_record_all(table_name):
	records = RecordManager.select(table_name, metadata.tables[table_name].fmt, *, 0, *, {})
	return records


def delete_record_all(table_name):
	index = Index()
	metadata = load_metadata()
	records = select_record_all(table_name)
	for record in records :
		for index.name in metadata.tables[table_name].indexes :
			file_path = RecordManager.file_dir + table_name + index.name + '.index' 
			manager = IndexManager(file_path,metadata.tables[table_name].fmt)
			key_pos = list(metadata.tables[table_name].columns.keys()).index(list(index.columns.keys())[0])   #index can be added on single column
			key = record[key_pos]
			manager.delete(key)		
	RecordManager.delete(table_name, metadata.tables[table_name].fmt, *, 0, *, {})


def select_record_conditionally(table_name,conditions):  #support only equivalent search
	index = Index()
	metadata = load_metadata()
	condition_pos = 0 
	while condition_pos < len(conditions) :
		attribute = conditions[condition_pos][0]
		for index.name in metadata.tables[table_name].indexes :
			if attribute in index.columns :
				file_path = RecordManager.file_dir + table_name + index.name + '.index'     #
				manager = IndexManager(file_path,metadata.tables[table_name].fmt)
				offset = manager.find(conditions[condition_pos][2])			
				for record_offset in offset :
					records += RecordManager.select(table_name, metadata.tables[table_name].fmt, *, 1, record_offset, {})	
				return records
	records = RecordManager.select(table_name, metadata.tables[table_name].fmt, *, 0, *, conditions)
	return records


def delete_record_conditionally(table_name,conditions):
	index = Index()
	metadata = load_metadata()
	records = select_record_conditionally(table_name,conditions)
	for record in records :
		for index.name in metadata.tables[table_name].indexes :
			has_index = 1
			file_path = RecordManager.file_dir + table_name + index.name + '.index' 
			manager = IndexManager(file_path,metadata.tables[table_name].fmt)
			key_pos = list(metadata.tables[table_name].columns.keys()).index(list(index.columns.keys())[0])   #index can be added on single column
			key = record[key_pos]
			value = manager.find(key)
			manager.delete(key)		
		if has_index :
			has_index = 0
			RecordManager.delete(table_name, metadata.tables[table_name].fmt, *, 1, value, *)
		else :
			RecordManager.delete(table_name, metadata.tables[table_name].fmt, *, 0, 8, conditions)


def drop_table(table_name)
	metadata = load_metadata()
	metadata.drop_table(table_name)
	shutil.rmtree('schema/tables/'+table_name,True)


def drop_index(table_name,index_name)
	metadata = load_metadata()
	metadata.drop_index(table_name,index_name)
	shutil.rmtree('schema/tables/'+table_name+'/'+index_name,True)

