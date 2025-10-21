import Memtable.Memtable as memtable
import os
import SSTable.SSTable as sst
current_data_store=memtable.Memtable(maximum_size_limit=1000,log_file_path='/home/pakhandi101/Documents/FunDB/Wal_log.wal')
import sys



# for key in range(500000,600001):
#     k=f"key{key}"
#     v=f"val{key}"
#     current_data_store.put(k,v)

# records=current_data_store.restore_from_log('/home/pakhandi101/Documents/FunDB/Wal_log2.wal')
# print(len(records))
# records=[]
# with open('SSTable1.dat',"rb") as f:
#     l=f.readline()
#     records.append(l)

# print(len(records))

# records=[]
# with open('SSTable1.dat','rb') as f:
#     record=f.read()
#     record.decode('UTF-8')
#     records.append(record)
#     print(record)

