import Memtable.Memtable as memtable
import os
import SSTable.SSTable as sst
current_data_store=memtable.Memtable(maximum_size_limit=1000,log_file_path='/home/pakhandi101/Documents/FunDB/Wal_log.wal')
import sys
records=current_data_store.restore_from_log('Wal_log.wal')
print(len(current_data_store.data))
# for i in range(1,300001):
#     key=f"key{i}"
#     value=f"{i}"
#     current_data_store.put(key,value)

# # current_data_store.put("Kittu","Motu")
# # current_data_store.put("College","MNNIT")
# print(sys.getsizeof(current_data_store.data))
# print(len(current_data_store.data))