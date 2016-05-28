from record_manager import RecordManager

if __name__ == '__main__':
    record_manager = RecordManager()
    record_manager.insert('a.rec', 0, 0, (1, 2.0, 3.0), '<idd')
    record_manager.insert('a.rec', 0, 1, (23,4.5,6.7), '<idd')
    value = record_manager.select('a.rec', 0, 1, '<idd')
    print(value)