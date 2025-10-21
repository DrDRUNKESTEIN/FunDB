import Memtable.Memtable as memtable
import os
import SSTable.SSTable as sst
current_data_store=memtable.Memtable(maximum_size_limit=1000,log_file_path='/home/pakhandi101/Documents/FunDB/Wal_log.wal')

records=current_data_store.restore_from_log('Wal_log.wal')

