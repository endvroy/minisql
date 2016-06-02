from record_manager import RecordManager
from struct import  Struct
from buffer_manager import pin, Block, BufferManager


def prepare_file():
    with open('a.rec', 'w+b') as f:
        header_strcut = Struct('<ii')
        f.write(header_strcut.pack(*(-1, 0)))

def display_file(filename):
    with open(filename, 'r+b') as f:
        content = f.read()
        print(content, len(content), sep='\n')

if __name__ == '__main__':
    prepare_file()
    buffer_manager = BufferManager()
    RecordManager.insert('a.rec', '<idi', (1, 3.0, 4))
    RecordManager.insert('a.rec', '<idi', (-1, 3.5, -1))
    RecordManager.delete('a.rec', '<idi', 1)
    RecordManager.insert('a.rec', '<idi', (-1, 3.0, -1))
    print(RecordManager.select('a.rec', '<idi', 0))
    buffer_manager.flush_all()
    display_file('a.rec')

